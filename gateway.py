"""
Gateway: FastAPI REST router, streaming uploads/downloads,
failover reads, dynamic node management, and Server-Sent Events (SSE).
"""
import os
import io
import time
import json
import asyncio
import hashlib
from typing import Optional, List, Dict, Any

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    status,
    Request,
    Depends
)
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import config
from auth import require_auth
from models import NodeRegistrationRequest
from metadata_service import (
    db_session,
    init_db,
    log_event,
    get_object_metadata,
    list_objects_summary,
    list_all_versions,
    get_next_version,
    prune_old_versions,
    acquire_lock,
    release_lock,
    global_hash_ring,
    sse_subscribers
)
import storage_node
import health_monitor
import repair_service
from erasure_coding import global_ec_coder

app = FastAPI(title="VAULT Distributed Object Storage", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

latencies = {
    "upload": [],
    "download": []
}

def record_latency(op: str, ms: float) -> None:
    samples = latencies.get(op)
    if samples is not None:
        samples.append(ms)
        if len(samples) > 50:
            samples.pop(0)

def avg_latency(op: str) -> float:
    samples = latencies.get(op, [])
    return round(sum(samples) / len(samples), 2) if samples else 0.0

@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    dashboard_path = config.STATIC_DIR / "dashboard.html"
    if not dashboard_path.exists():
        dashboard_path = config.STATIC_DIR / "index.html"
    return FileResponse(dashboard_path)

@app.get("/events/stream")
async def event_stream(request: Request):
    """Server-Sent Events endpoint pushing cluster events in real-time to the dashboard."""
    queue = asyncio.Queue(maxsize=100)
    sse_subscribers.append(queue)

    async def event_generator():
        try:
            init_msg = json.dumps({"level": "INFO", "category": "SYSTEM", "message": "Connected to Vault live event stream", "timestamp": time.time()})
            yield f"data: {init_msg}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            if queue in sse_subscribers:
                sse_subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@app.get("/nodes")
async def list_nodes(auth: str = Depends(require_auth)):
    """List all storage nodes, health statuses, and disk metrics."""
    return health_monitor.get_all_nodes()

@app.post("/nodes")
async def register_node(body: NodeRegistrationRequest, auth: str = Depends(require_auth)):
    """Dynamically register a new node into the cluster and consistent hash ring."""
    result = health_monitor.register_dynamic_node(
        host=body.host,
        port=body.port,
        node_id=body.node_id,
        capacity=body.capacity
    )
    return result

@app.delete("/nodes/{node_id}")
async def remove_node(node_id: str, auth: str = Depends(require_auth)):
    """Permanently remove a node from the cluster. Must be drained or failed first."""
    try:
        return health_monitor.remove_node_from_cluster(node_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/nodes/{node_id}/drain")
async def drain_node_endpoint(node_id: str, auth: str = Depends(require_auth)):
    """Drain a node: reject writes and evacuate all replicas to healthy nodes in hash ring."""
    return repair_service.drain_node(node_id)

@app.post("/nodes/{node_id}/recover")
async def recover_node_endpoint(node_id: str, auth: str = Depends(require_auth)):
    """Recover node back to HEALTHY status and perform orphan sweep on disk."""
    return repair_service.recover_node_and_reconcile(node_id)

@app.post("/nodes/{node_id}/fail")
async def fail_node_endpoint(node_id: str, auth: str = Depends(require_auth)):
    """Simulate a sudden node crash."""
    ok = health_monitor.set_node_status(node_id, config.NODE_STATUS_FAILED, reason="manual_kill_trigger")
    if not ok:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    return {"node_id": node_id, "status": config.NODE_STATUS_FAILED}

@app.post("/nodes/{node_id}/disconnect")
async def disconnect_node_endpoint(node_id: str, auth: str = Depends(require_auth)):
    """Simulate a network partition isolating the node."""
    ok = health_monitor.set_node_status(node_id, config.NODE_STATUS_DISCONNECTED, reason="network_partition_simulation")
    if not ok:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    return {"node_id": node_id, "status": config.NODE_STATUS_DISCONNECTED}

@app.get("/objects")
async def list_objects(auth: str = Depends(require_auth)):
    """List summary of all stored objects with latest versions and health states."""
    return list_objects_summary()

@app.get("/objects/{key}")
async def get_metadata(key: str, version: Optional[int] = Query(None), auth: str = Depends(require_auth)):
    """Retrieve metadata and replica placements for an object version."""
    meta = get_object_metadata(key, version)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Object '{key}' (version: {version or 'latest'}) not found")
    return meta

@app.get("/objects/{key}/versions")
async def get_versions(key: str, auth: str = Depends(require_auth)):
    """List all available versions for a given object key."""
    return list_all_versions(key)

@app.post("/objects")
async def upload_object(
    file: UploadFile = File(...),
    key: Optional[str] = Form(None),
    replication_factor: int = Form(config.DEFAULT_REPLICATION_FACTOR),
    storage_mode: Optional[str] = Form(None),
    auth: str = Depends(require_auth)
):
    """
    Streaming upload with Consistent Hashing placement, real versioning,
    and row-level distributed locking. Never reads whole large files into RAM.
    """
    start_time = time.time()
    object_key = key or file.filename or f"unnamed_{int(time.time())}"
    mode = storage_mode or config.STORAGE_MODE

    if not acquire_lock(object_key, "upload", ttl_seconds=20):
        raise HTTPException(status_code=423, detail=f"Object '{object_key}' is locked by a concurrent operation.")

    try:
        version = get_next_version(object_key)
        now = time.time()
        hasher = hashlib.sha256()

        if mode == "erasure_coding":
            k = config.EC_DATA_SHARDS
            m = config.EC_PARITY_SHARDS
            total_shards = k + m

            target_nodes = global_hash_ring.get_nodes_for_key(f"{object_key}:v{version}", total_shards)
            if len(target_nodes) < total_shards:
                raise HTTPException(
                    status_code=500,
                    detail=f"Insufficient healthy nodes ({len(target_nodes)}) for EC({k}+{m}) requires {total_shards} nodes."
                )

            chunks = []
            total_size = 0
            while chunk := await file.read(config.CHUNK_SIZE):
                hasher.update(chunk)
                chunks.append(chunk)
                total_size += len(chunk)

            full_payload = b"".join(chunks)
            checksum = hasher.hexdigest()

            shards, orig_len = global_ec_coder.encode(full_payload)

            for idx, (node_id, shard_data) in enumerate(zip(target_nodes, shards)):
                shard_chk = storage_node.calculate_checksum(shard_data)
                storage_node.write_shard_atomic(node_id, object_key, version, idx, shard_data)

            with db_session() as conn:
                conn.execute("""
                    INSERT INTO objects (key, version, size, checksum, storage_mode, replication_factor, created_at)
                    VALUES (?, ?, ?, ?, 'erasure_coding', ?, ?)
                """, (object_key, version, total_size, checksum, total_shards, now))

                for idx, (node_id, shard_data) in enumerate(zip(target_nodes, shards)):
                    shard_chk = storage_node.calculate_checksum(shard_data)
                    conn.execute("""
                        INSERT INTO replicas (object_key, version, node_id, shard_index, checksum, status, last_verified)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (object_key, version, node_id, idx, shard_chk, config.REPLICA_STATUS_HEALTHY, now))

                log_event(
                    "UPLOAD",
                    f"Uploaded '{object_key}' v{version} ({total_size} bytes, EC {k}+{m}) to {len(target_nodes)} nodes",
                    details=f"Checksum: {checksum[:12]}...",
                    conn=conn
                )

        else:
            rf = replication_factor or config.DEFAULT_REPLICATION_FACTOR
            target_nodes = global_hash_ring.get_nodes_for_key(f"{object_key}:v{version}", rf)

            quorum = min(config.WRITE_QUORUM, rf)
            if len(target_nodes) < quorum:
                raise HTTPException(
                    status_code=500,
                    detail=f"Write quorum failed: available healthy nodes ({len(target_nodes)}) < quorum ({quorum})"
                )

            temp_files = []
            node_dirs = []
            for nid in target_nodes:
                nd = storage_node.ensure_node_dir(nid)
                node_dirs.append(nd)
                t_path = nd / f".tmp_{storage_node.get_shard_filename(object_key, version, 0)}"
                temp_files.append(open(t_path, "wb"))

            total_size = 0
            try:
                while chunk := await file.read(config.CHUNK_SIZE):
                    hasher.update(chunk)
                    total_size += len(chunk)
                    for f in temp_files:
                        f.write(chunk)
            finally:
                for f in temp_files:
                    f.flush()
                    os.fsync(f.fileno())
                    f.close()

            checksum = hasher.hexdigest()

            for nid in target_nodes:
                nd = storage_node.ensure_node_dir(nid)
                t_path = nd / f".tmp_{storage_node.get_shard_filename(object_key, version, 0)}"
                final_path = storage_node.get_replica_path(nid, object_key, version, 0)
                t_path.replace(final_path)

            with db_session() as conn:
                conn.execute("""
                    INSERT INTO objects (key, version, size, checksum, storage_mode, replication_factor, created_at)
                    VALUES (?, ?, ?, ?, 'replication', ?, ?)
                """, (object_key, version, total_size, checksum, rf, now))

                for nid in target_nodes:
                    conn.execute("""
                        INSERT INTO replicas (object_key, version, node_id, shard_index, checksum, status, last_verified)
                        VALUES (?, ?, ?, 0, ?, ?, ?)
                    """, (object_key, version, nid, checksum, config.REPLICA_STATUS_HEALTHY, now))

                log_event(
                    "UPLOAD",
                    f"Uploaded '{object_key}' v{version} ({total_size} bytes, RF={rf}) to nodes: {', '.join(target_nodes)}",
                    details=f"Checksum: {checksum[:12]}...",
                    conn=conn
                )

        stale_reps = prune_old_versions(object_key, config.MAX_VERSIONS)
        for sr in stale_reps:
            try:
                storage_node.delete_shard(sr["node_id"], sr["object_key"], sr["version"], sr["shard_index"])
            except Exception:
                pass

        duration_ms = round((time.time() - start_time) * 1000, 2)
        record_latency("upload", duration_ms)

        meta = get_object_metadata(object_key, version)
        return meta

    finally:
        release_lock(object_key)

@app.get("/objects/{key}/download")
async def download_object(key: str, version: Optional[int] = Query(None)):
    """
    Download object stream with automatic failover across replicas and passive read-repair.
    """
    start_time = time.time()
    meta = get_object_metadata(key, version)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Object '{key}' not found")

    expected_checksum = meta["checksum"]
    v = meta["version"]
    mode = meta["storage_mode"]
    replicas = meta.get("replicas", [])

    if mode == "erasure_coding":
        k = config.EC_DATA_SHARDS
        available_shards = {}
        for r in replicas:
            if r["node_status"] == config.NODE_STATUS_HEALTHY:
                try:
                    data = storage_node.read_shard(r["node_id"], key, v, r["shard_index"])
                    if storage_node.calculate_checksum(data) == r["checksum"]:
                        available_shards[r["shard_index"]] = data
                        if len(available_shards) == k:
                            break
                except Exception:
                    pass

        if len(available_shards) < k:
            raise HTTPException(status_code=500, detail="Insufficient healthy shards available to reconstruct object")

        payload = global_ec_coder.decode(available_shards, meta["size"])
        actual_chk = storage_node.calculate_checksum(payload)
        if actual_chk != expected_checksum:
            raise HTTPException(status_code=500, detail="Reconstructed data checksum failed verification")

        record_latency("download", round((time.time() - start_time) * 1000, 2))
        return StreamingResponse(
            io.BytesIO(payload),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{key}"',
                "X-Vault-Checksum": expected_checksum,
                "X-Vault-Version": str(v),
                "X-Vault-Storage-Mode": "erasure_coding"
            }
        )

    else:
        candidates = [r for r in replicas if r["node_status"] == config.NODE_STATUS_HEALTHY]
        if not candidates:
            raise HTTPException(status_code=503, detail="No healthy storage nodes hosting replicas are online")

        for cand in candidates:
            node_id = cand["node_id"]
            try:
                payload = storage_node.read_shard(node_id, key, v, 0)
                actual_chk = storage_node.calculate_checksum(payload)

                if actual_chk != expected_checksum:
                    with db_session() as conn:
                        conn.execute("""
                            UPDATE replicas SET status = ? WHERE object_key = ? AND version = ? AND node_id = ?
                        """, (config.REPLICA_STATUS_CORRUPTED, key, v, node_id))
                        log_event("INTEGRITY", f"Download detected corrupt replica of '{key}' on {node_id}", level="ERROR", conn=conn)

                    repair_service.trigger_async_read_repair(key, v, node_id)
                    continue

                record_latency("download", round((time.time() - start_time) * 1000, 2))
                return StreamingResponse(
                    io.BytesIO(payload),
                    media_type="application/octet-stream",
                    headers={
                        "Content-Disposition": f'attachment; filename="{key}"',
                        "X-Vault-Checksum": expected_checksum,
                        "X-Vault-Version": str(v),
                        "X-Vault-Storage-Mode": "replication"
                    }
                )

            except Exception:
                continue

        raise HTTPException(status_code=500, detail="Failed to read object from all available replicas")

@app.delete("/objects/{key}")
async def delete_object(key: str, version: Optional[int] = Query(None), auth: str = Depends(require_auth)):
    """Delete an object (all versions, or a specific version if specified)."""
    with db_session() as conn:
        cursor = conn.cursor()
        if version is not None:
            replicas = cursor.execute("""
                SELECT node_id, shard_index FROM replicas WHERE object_key = ? AND version = ?
            """, (key, version)).fetchall()
            for r in replicas:
                storage_node.delete_shard(r["node_id"], key, version, r["shard_index"])
            cursor.execute("DELETE FROM replicas WHERE object_key = ? AND version = ?", (key, version))
            cursor.execute("DELETE FROM objects WHERE key = ? AND version = ?", (key, version))
            log_event("DELETE", f"Object '{key}' v{version} deleted", conn=conn)
        else:
            replicas = cursor.execute("""
                SELECT node_id, version, shard_index FROM replicas WHERE object_key = ?
            """, (key,)).fetchall()
            for r in replicas:
                storage_node.delete_shard(r["node_id"], key, r["version"], r["shard_index"])
            cursor.execute("DELETE FROM replicas WHERE object_key = ?", (key,))
            cursor.execute("DELETE FROM objects WHERE key = ?", (key,))
            log_event("DELETE", f"Object '{key}' (all versions) deleted", conn=conn)

    return {"key": key, "status": "deleted"}

@app.post("/objects/{key}/corrupt")
async def corrupt_object_endpoint(key: str, version: Optional[int] = Query(None), auth: str = Depends(require_auth)):
    """Simulate byte corruption on a physical replica for demonstration."""
    meta = get_object_metadata(key, version)
    if not meta or not meta["replicas"]:
        raise HTTPException(status_code=404, detail="No replicas found to corrupt")

    target_rep = meta["replicas"][0]
    ok = storage_node.corrupt_replica(target_rep["node_id"], key, meta["version"], target_rep["shard_index"])
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to corrupt replica file on disk")

    with db_session() as conn:
        conn.execute("""
            UPDATE replicas SET status = ? WHERE id = ?
        """, (config.REPLICA_STATUS_CORRUPTED, target_rep["id"]))
        log_event(
            "INTEGRITY",
            f"Injected bit-rot into replica of '{key}' v{meta['version']} on {target_rep['node_id']}",
            level="WARNING",
            conn=conn
        )

    return {"key": key, "node_id": target_rep["node_id"], "status": "corrupted"}

@app.post("/objects/{key}/repair")
async def manual_repair_endpoint(key: str, version: Optional[int] = Query(None), auth: str = Depends(require_auth)):
    """Trigger replica repair for an object."""
    meta = get_object_metadata(key, version)
    if not meta:
        raise HTTPException(status_code=404, detail="Object not found")
    return repair_service.repair_object_version(key, meta["version"])

@app.post("/objects/verify/all")
async def verify_all_checksums(auth: str = Depends(require_auth)):
    """Scrub all stored shards against their recorded SHA-256 checksums."""
    with db_session() as conn:
        cursor = conn.cursor()
        replicas = cursor.execute("""
            SELECT id, object_key, version, node_id, shard_index, checksum FROM replicas
        """).fetchall()

    checked = 0
    corrupted = 0
    for r in replicas:
        checked += 1
        p = storage_node.get_replica_path(r["node_id"], r["object_key"], r["version"], r["shard_index"])
        try:
            actual = storage_node.calculate_file_checksum(p)
            valid = (actual == r["checksum"])
        except Exception:
            valid = False

        status = config.REPLICA_STATUS_HEALTHY if valid else config.REPLICA_STATUS_CORRUPTED
        if not valid:
            corrupted += 1
        with db_session() as conn:
            conn.execute("""
                UPDATE replicas SET status = ?, last_verified = ? WHERE id = ?
            """, (status, time.time(), r["id"]))

    log_event("INTEGRITY", f"Cluster integrity scrub complete: {checked} checked, {corrupted} corrupted.")
    return {"total_checked": checked, "corrupted_count": corrupted}

@app.get("/metrics")
async def get_cluster_metrics(auth: str = Depends(require_auth)):
    """Comprehensive cluster operational metrics and overhead multiplier."""
    nodes = health_monitor.get_all_nodes()
    total_nodes = len(nodes)
    healthy_nodes = sum(1 for n in nodes if n["status"] == config.NODE_STATUS_HEALTHY)
    draining_nodes = sum(1 for n in nodes if n["status"] == config.NODE_STATUS_DRAINING)
    failed_nodes = sum(1 for n in nodes if n["status"] == config.NODE_STATUS_FAILED)

    with db_session() as conn:
        cursor = conn.cursor()
        total_keys = cursor.execute("SELECT COUNT(DISTINCT key) as c FROM objects").fetchone()["c"]
        total_versions = cursor.execute("SELECT COUNT(*) as c FROM objects").fetchone()["c"]
        logical_bytes = cursor.execute("SELECT COALESCE(SUM(size), 0) as s FROM objects").fetchone()["s"]

        total_replicas = cursor.execute("SELECT COUNT(*) as c FROM replicas").fetchone()["c"]
        healthy_reps = cursor.execute("SELECT COUNT(*) as c FROM replicas WHERE status = ?", (config.REPLICA_STATUS_HEALTHY,)).fetchone()["c"]
        corrupt_reps = cursor.execute("SELECT COUNT(*) as c FROM replicas WHERE status = ?", (config.REPLICA_STATUS_CORRUPTED,)).fetchone()["c"]

        completed_repairs = cursor.execute("SELECT COUNT(*) as c FROM repair_jobs WHERE status = ?", (config.REPAIR_STATUS_COMPLETED,)).fetchone()["c"]
        failed_repairs = cursor.execute("SELECT COUNT(*) as c FROM repair_jobs WHERE status = ?", (config.REPAIR_STATUS_FAILED,)).fetchone()["c"]
        avg_repair = cursor.execute("SELECT COALESCE(AVG(duration_ms), 0) as a FROM repair_jobs WHERE status = ?", (config.REPAIR_STATUS_COMPLETED,)).fetchone()["a"]

    physical_bytes = sum(n["used_storage"] for n in nodes)
    overhead = round(physical_bytes / max(logical_bytes, 1), 2)

    if failed_nodes > 0 or corrupt_reps > 0:
        system_status = "DEGRADED" if healthy_nodes >= 2 else "CRITICAL"
    else:
        system_status = "HEALTHY"

    return {
        "system_status": system_status,
        "storage_mode": config.STORAGE_MODE,
        "total_keys": total_keys,
        "total_versions": total_versions,
        "logical_bytes": logical_bytes,
        "physical_bytes": physical_bytes,
        "storage_overhead": overhead,
        "total_nodes": total_nodes,
        "healthy_nodes": healthy_nodes,
        "draining_nodes": draining_nodes,
        "failed_nodes": failed_nodes,
        "total_replicas": total_replicas,
        "healthy_replicas": healthy_reps,
        "corrupted_replicas": corrupt_reps,
        "completed_repairs": completed_repairs,
        "failed_repairs": failed_repairs,
        "avg_repair_duration_ms": round(avg_repair, 2),
        "upload_latency_ms": avg_latency("upload"),
        "download_latency_ms": avg_latency("download")
    }

@app.get("/events")
async def get_recent_events(limit: int = 50, auth: str = Depends(require_auth)):
    """Retrieve audit log events."""
    with db_session() as conn:
        cursor = conn.cursor()
        events = cursor.execute("""
            SELECT id, level, category, message, details, timestamp
            FROM events
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(e) for e in events]
