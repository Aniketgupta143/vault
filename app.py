"""Vault Distributed Object Storage - FastAPI Gateway."""
import time
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import io

from config import (
    INITIAL_NODES,
    DEFAULT_REPLICATION_FACTOR,
    NODE_STATUS_HEALTHY,
    NODE_STATUS_FAILED,
    NODE_STATUS_DISCONNECTED,
    REPLICA_STATUS_HEALTHY,
    REPLICA_STATUS_CORRUPTED,
    HEALTH_CHECK_INTERVAL_SECONDS,
    AUTO_REPAIR_INTERVAL_SECONDS,
    AUTO_REPAIR_ENABLED,
)
from database import init_db, db_session, log_event
import storage
import health
import replication
import repair
import integrity
import rebalance

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("vault.gateway")

# Rolling performance metrics
perf_metrics = {
    "upload_latencies": [],
    "download_latencies": []
}

def record_latency(metric_type: str, duration_ms: float):
    samples = perf_metrics.get(metric_type)
    if samples is not None:
        samples.append(duration_ms)
        if len(samples) > 50:
            samples.pop(0)

def get_avg_latency(metric_type: str) -> float:
    samples = perf_metrics.get(metric_type, [])
    if not samples:
        return 0.0
    return round(sum(samples) / len(samples), 2)

# Background tasks lifecycle
background_tasks = []

async def health_and_repair_worker():
    """Background worker that continuously monitors node health and triggers self-healing."""
    logger.info("Vault background monitor & auto-healer started.")
    while True:
        try:
            # 1. Pulse active healthy nodes
            health.pulse_active_nodes()

            # 2. Check for missed heartbeats
            timed_out = health.check_heartbeats()
            if timed_out:
                logger.warning(f"Nodes timed out: {timed_out}")

            # 3. Run auto-repair cycle if enabled
            if AUTO_REPAIR_ENABLED:
                repaired = repair.run_auto_repair_cycle()
                if repaired:
                    logger.info(f"Auto-repair cycle executed: {len(repaired)} objects processed.")
        except Exception as e:
            logger.error(f"Error in background monitor: {e}")

        await asyncio.sleep(AUTO_REPAIR_INTERVAL_SECONDS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Ensure directories and database exist
    storage.ensure_storage_nodes()
    init_db()
    health.pulse_active_nodes()
    log_event("SYSTEM", "Vault Gateway started successfully")

    # Start background healing worker
    task = asyncio.create_task(health_and_repair_worker())
    background_tasks.append(task)

    yield

    # Shutdown: Cancel background tasks
    for t in background_tasks:
        t.cancel()
    log_event("SYSTEM", "Vault Gateway shutting down")

app = FastAPI(
    title="Vault Distributed Object Storage",
    description="Fault-tolerant, self-healing distributed object storage prototype",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------------
# Static Files & Frontend Dashboard
# -------------------------------------------------------------
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse("static/index.html")

# -------------------------------------------------------------
# Object Storage API Endpoints
# -------------------------------------------------------------
@app.post("/objects", summary="Upload and replicate a new object")
async def upload_object(
    file: UploadFile = File(...),
    replication_factor: int = Form(DEFAULT_REPLICATION_FACTOR)
):
    """
    Upload a file, compute SHA-256, store across healthy nodes respecting quorum,
    and persist metadata.
    """
    t0 = time.time()
    try:
        content = await file.read()
        metadata = await replication.store_object(
            filename=file.filename or "unnamed_file",
            data=content,
            replication_factor=replication_factor
        )
        duration_ms = (time.time() - t0) * 1000
        record_latency("upload_latencies", duration_ms)
        return metadata
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/objects", summary="List all objects with health status")
async def list_objects():
    """Retrieve metadata and replica health for all stored objects."""
    return replication.list_all_objects()

@app.get("/objects/{object_id}", summary="Get object metadata")
async def get_object_metadata(object_id: str):
    """Retrieve metadata for a specific object."""
    meta = replication.get_object_metadata(object_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Object not found")
    return meta

@app.get("/objects/{object_id}/download", summary="Download object with failover")
async def download_object(object_id: str, verify: bool = Query(True)):
    """
    Retrieve object with automatic failover across replicas and on-the-fly checksum verification.
    """
    t0 = time.time()
    try:
        data, metadata = await replication.retrieve_object(object_id, verify=verify)
        duration_ms = (time.time() - t0) * 1000
        record_latency("download_latencies", duration_ms)

        headers = {
            "Content-Disposition": f'attachment; filename="{metadata["filename"]}"',
            "X-Vault-Checksum": metadata["checksum"],
            "X-Vault-Version": str(metadata["version"])
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Object metadata not found")
    except Exception as e:
        logger.error(f"Download failed for {object_id}: {e}")
        raise HTTPException(status_code=503, detail=str(e))

@app.delete("/objects/{object_id}", summary="Delete an object and all its replicas")
async def delete_object_endpoint(object_id: str):
    """Delete object metadata and all physical replicas across nodes."""
    success = replication.delete_object(object_id)
    if not success:
        raise HTTPException(status_code=404, detail="Object not found")
    return {"status": "success", "message": f"Object {object_id} deleted"}

# -------------------------------------------------------------
# Node Management & Failure Injection
# -------------------------------------------------------------
@app.get("/nodes", summary="List all storage nodes and their statuses")
async def get_nodes():
    """List status, storage capacity, and active replica counts for all nodes."""
    return health.get_all_nodes()

@app.post("/nodes/{node_id}/fail", summary="Simulate node failure")
async def simulate_fail(node_id: str):
    """Simulate a node crash/failure and immediately trigger health evaluation."""
    success = health.set_node_status(node_id, NODE_STATUS_FAILED, reason="Dashboard simulated failure")
    if not success:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    
    # Check affected objects
    under_rep = repair.find_under_replicated_objects()
    return {
        "status": "success",
        "node_id": node_id,
        "new_status": NODE_STATUS_FAILED,
        "affected_objects_count": len(under_rep)
    }

@app.post("/nodes/{node_id}/recover", summary="Simulate node recovery")
async def simulate_recover(node_id: str):
    """Simulate node coming back online."""
    success = health.set_node_status(node_id, NODE_STATUS_HEALTHY, reason="Dashboard node recovery")
    if not success:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    return {"status": "success", "node_id": node_id, "new_status": NODE_STATUS_HEALTHY}

@app.post("/nodes/{node_id}/disconnect", summary="Simulate network partition")
async def simulate_disconnect(node_id: str):
    """Simulate network partition isolating this node."""
    success = health.set_node_status(node_id, NODE_STATUS_DISCONNECTED, reason="Dashboard network partition")
    if not success:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    return {"status": "success", "node_id": node_id, "new_status": NODE_STATUS_DISCONNECTED}

@app.post("/nodes/{node_id}/heartbeat", summary="Record node heartbeat")
async def node_heartbeat(node_id: str):
    """Register heartbeat timestamp for a node."""
    success = health.record_heartbeat(node_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    return {"status": "success", "node_id": node_id}

# -------------------------------------------------------------
# Integrity Verification & Repair Endpoints
# -------------------------------------------------------------
@app.post("/objects/{object_id}/verify", summary="Verify object replica integrity")
async def verify_object(object_id: str):
    """Run SHA-256 verification across all replicas of an object."""
    try:
        return integrity.verify_object_integrity(object_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Object not found")

@app.post("/verify/all", summary="Scrub all objects for corruption")
async def verify_all():
    """Scrub all replicas in the cluster against their recorded SHA-256 hashes."""
    return integrity.verify_all_objects()

@app.post("/objects/{object_id}/corrupt", summary="Simulate byte corruption on replica")
async def corrupt_replica_endpoint(object_id: str, node_id: Optional[str] = Query(None)):
    """Intentionally alter physical bytes of an object replica to demonstrate checksum detection."""
    try:
        return integrity.simulate_corruption(object_id, node_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/objects/{object_id}/repair", summary="Manually trigger replica repair")
async def trigger_repair_endpoint(object_id: str):
    """Manually heal missing or corrupted replicas for an object."""
    try:
        return repair.repair_object(object_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rebalance", summary="Trigger cluster storage rebalancing")
async def trigger_rebalance_endpoint():
    """Migrate replicas from high-utilization nodes to under-utilized nodes."""
    return rebalance.rebalance_cluster()

# -------------------------------------------------------------
# System Health, Metrics & Event Logs
# -------------------------------------------------------------
@app.get("/health", summary="Cluster health summary")
async def get_cluster_health():
    """Summary of cluster status, node health, and under-replicated objects."""
    nodes = health.get_all_nodes()
    healthy_nodes = [n for n in nodes if n["status"] == NODE_STATUS_HEALTHY]
    failed_nodes = [n for n in nodes if n["status"] == NODE_STATUS_FAILED]
    disconnected_nodes = [n for n in nodes if n["status"] == NODE_STATUS_DISCONNECTED]
    
    under_replicated = repair.find_under_replicated_objects()

    with db_session() as conn:
        corrupt_reps = conn.execute("SELECT COUNT(*) as c FROM replicas WHERE status = 'CORRUPTED'").fetchone()["c"]

    if len(healthy_nodes) < 2 or len(under_replicated) > 0 and len(healthy_nodes) == 0:
        system_status = "CRITICAL"
    elif len(failed_nodes) > 0 or len(disconnected_nodes) > 0 or len(under_replicated) > 0 or corrupt_reps > 0:
        system_status = "DEGRADED"
    else:
        system_status = "HEALTHY"

    return {
        "status": system_status,
        "nodes": {
            "total": len(nodes),
            "healthy": len(healthy_nodes),
            "failed": len(failed_nodes),
            "disconnected": len(disconnected_nodes)
        },
        "under_replicated_objects": len(under_replicated),
        "corrupted_replicas": corrupt_reps
    }

@app.get("/metrics", summary="Detailed system metrics")
async def get_metrics():
    """Retrieve operational and performance metrics for the dashboard."""
    nodes = health.get_all_nodes()
    healthy_nodes = sum(1 for n in nodes if n["status"] == NODE_STATUS_HEALTHY)
    failed_nodes = sum(1 for n in nodes if n["status"] == NODE_STATUS_FAILED)
    disconnected_nodes = sum(1 for n in nodes if n["status"] == NODE_STATUS_DISCONNECTED)

    with db_session() as conn:
        cursor = conn.cursor()
        total_objects = cursor.execute("SELECT COUNT(*) as c FROM objects").fetchone()["c"]
        logical_bytes = cursor.execute("SELECT COALESCE(SUM(size), 0) as s FROM objects").fetchone()["s"]
        
        total_replicas = cursor.execute("SELECT COUNT(*) as c FROM replicas").fetchone()["c"]
        healthy_replicas = cursor.execute("SELECT COUNT(*) as c FROM replicas WHERE status = 'HEALTHY'").fetchone()["c"]
        corrupted_replicas = cursor.execute("SELECT COUNT(*) as c FROM replicas WHERE status = 'CORRUPTED'").fetchone()["c"]

        completed_repairs = cursor.execute("SELECT COUNT(*) as c FROM repair_jobs WHERE status = 'COMPLETED'").fetchone()["c"]
        failed_repairs = cursor.execute("SELECT COUNT(*) as c FROM repair_jobs WHERE status = 'FAILED'").fetchone()["c"]
        avg_repair = cursor.execute("SELECT COALESCE(AVG(duration_ms), 0) as a FROM repair_jobs WHERE status = 'COMPLETED'").fetchone()["a"]

    # Calculate actual physical storage used across all nodes
    physical_bytes = sum(storage.get_node_used_bytes(n["node_id"]) for n in nodes)
    storage_overhead = round(physical_bytes / logical_bytes, 2) if logical_bytes > 0 else 1.0

    under_replicated = len(repair.find_under_replicated_objects())

    if healthy_nodes < 2:
        system_status = "CRITICAL"
    elif failed_nodes > 0 or disconnected_nodes > 0 or under_replicated > 0 or corrupted_replicas > 0:
        system_status = "DEGRADED"
    else:
        system_status = "HEALTHY"

    return {
        "system_status": system_status,
        "total_objects": total_objects,
        "logical_bytes": logical_bytes,
        "physical_bytes": physical_bytes,
        "storage_overhead": storage_overhead,
        "healthy_nodes": healthy_nodes,
        "failed_nodes": failed_nodes,
        "disconnected_nodes": disconnected_nodes,
        "total_replicas": total_replicas,
        "healthy_replicas": healthy_replicas,
        "corrupted_replicas": corrupted_replicas,
        "under_replicated_objects": under_replicated,
        "completed_repairs": completed_repairs,
        "failed_repairs": failed_repairs,
        "avg_repair_duration_ms": round(avg_repair, 2),
        "upload_latency_ms": get_avg_latency("upload_latencies"),
        "download_latency_ms": get_avg_latency("download_latencies")
    }

@app.get("/events", summary="Get recent audit/event logs")
async def get_recent_events(limit: int = Query(50, le=200)):
    """Retrieve audit logs of system actions, failures, and repairs."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT id, level, category, message, details, timestamp
            FROM events
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
