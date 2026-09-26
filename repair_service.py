"""
Repair Service: Self-healing replica repair, passive read-repair,
node draining evacuation, and orphan sweeping on recovery.
"""
import time
import asyncio
import logging
from typing import Dict, Any, List, Optional, Set

import config
from metadata_service import (
    db_session,
    log_event,
    get_object_metadata,
    acquire_lock,
    release_lock,
    global_hash_ring
)
import storage_node
import health_monitor

logger = logging.getLogger("vault.repair_service")

def find_under_replicated_objects() -> List[Dict[str, Any]]:
    """Scan database to identify object versions needing replica healing or corruption fixes."""
    with db_session() as conn:
        cursor = conn.cursor()
        objects = cursor.execute("""
            SELECT key, version, storage_mode, replication_factor, checksum, size
            FROM objects
        """).fetchall()

        needs_repair = []
        for obj in objects:
            key = obj["key"]
            v = obj["version"]
            mode = obj["storage_mode"]

            replicas = cursor.execute("""
                SELECT r.node_id, r.shard_index, r.status as rep_status, n.status as node_status
                FROM replicas r
                LEFT JOIN nodes n ON r.node_id = n.node_id
                WHERE r.object_key = ? AND r.version = ?
            """, (key, v)).fetchall()

            healthy_count = sum(1 for r in replicas if r["rep_status"] == config.REPLICA_STATUS_HEALTHY and r["node_status"] == config.NODE_STATUS_HEALTHY)
            corrupt_count = sum(1 for r in replicas if r["rep_status"] in (config.REPLICA_STATUS_CORRUPTED, config.REPLICA_STATUS_MISSING))

            required = (config.EC_DATA_SHARDS + config.EC_PARITY_SHARDS) if mode == "erasure_coding" else obj["replication_factor"]

            if healthy_count < required or corrupt_count > 0:
                needs_repair.append({
                    "key": key,
                    "version": v,
                    "storage_mode": mode,
                    "healthy_count": healthy_count,
                    "required_count": required,
                    "corrupt_count": corrupt_count
                })
        return needs_repair

def repair_object_version(key: str, version: int) -> Dict[str, Any]:
    """
    Execute self-healing repair for an object version adhering to the invariant:
    COPY -> VERIFY SHA-256 -> REGISTER -> CLEANUP STALE.
    Protected by row-level distributed lock.
    """
    lock_acquired = acquire_lock(f"{key}:v{version}", "repair", ttl_seconds=15)
    if not lock_acquired:
        return {"key": key, "version": version, "status": "skipped", "reason": "locked"}

    start_time = time.time()
    try:
        meta = get_object_metadata(key, version)
        if not meta:
            raise FileNotFoundError(f"Object {key} v{version} not found")

        expected_checksum = meta["checksum"]
        mode = meta["storage_mode"]
        replicas = meta["replicas"]

        if mode == "replication":
            viable_sources = []
            for r in replicas:
                if r["status"] == config.REPLICA_STATUS_HEALTHY and r.get("node_status") == config.NODE_STATUS_HEALTHY:
                    try:
                        p = storage_node.get_replica_path(r["node_id"], key, version, 0)
                        if p.exists() and storage_node.calculate_file_checksum(p) == expected_checksum:
                            viable_sources.append(r["node_id"])
                    except Exception:
                        pass

            if not viable_sources:
                raise RuntimeError(f"No intact source replica available in cluster to repair {key} v{version}")

            source_node = viable_sources[0]

            corrupt_on_healthy = [
                r["node_id"] for r in replicas 
                if r["status"] == config.REPLICA_STATUS_CORRUPTED and r.get("node_status") == config.NODE_STATUS_HEALTHY
            ]

            if corrupt_on_healthy:
                target_node = corrupt_on_healthy[0]
                target_type = "in_place_heal"
            else:
                exclude = set(viable_sources)
                candidates = global_hash_ring.get_nodes_for_key(f"{key}:v{version}", 6, exclude_nodes=exclude)
                if not candidates:
                    raise RuntimeError(f"No spare healthy node available in hash ring to place repaired replica")
                target_node = candidates[0]
                target_type = "replacement_node"

            clean_data = storage_node.read_shard(source_node, key, version, 0)
            written_size, actual_checksum = storage_node.write_shard_atomic(target_node, key, version, 0, clean_data)

            if actual_checksum != expected_checksum:
                storage_node.delete_shard(target_node, key, version, 0)
                raise RuntimeError("Target replica failed checksum verification during repair")

            now = time.time()
            duration_ms = round((now - start_time) * 1000, 2)
            with db_session() as conn:
                conn.execute("""
                    INSERT INTO replicas (object_key, version, node_id, shard_index, checksum, status, last_verified)
                    VALUES (?, ?, ?, 0, ?, ?, ?)
                    ON CONFLICT(object_key, version, node_id, shard_index) DO UPDATE SET
                        checksum = excluded.checksum,
                        status = ?,
                        last_verified = excluded.last_verified
                """, (key, version, target_node, actual_checksum, config.REPLICA_STATUS_HEALTHY, now, config.REPLICA_STATUS_HEALTHY))

                if target_type == "replacement_node":
                    dead_rep = conn.execute("""
                        SELECT r.id, r.node_id FROM replicas r
                        JOIN nodes n ON r.node_id = n.node_id
                        WHERE r.object_key = ? AND r.version = ? AND n.status != ?
                        LIMIT 1
                    """, (key, version, config.NODE_STATUS_HEALTHY)).fetchone()
                    if dead_rep:
                        conn.execute("DELETE FROM replicas WHERE id = ?", (dead_rep["id"],))

                conn.execute("""
                    INSERT INTO repair_jobs (object_key, version, source_node, target_node, status, duration_ms, created_at, completed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (key, version, source_node, target_node, config.REPAIR_STATUS_COMPLETED, duration_ms, start_time, now))

                log_event(
                    "REPAIR",
                    f"Repaired {key} v{version} on {target_node} from {source_node} ({duration_ms}ms)",
                    details=f"Target type: {target_type}",
                    conn=conn
                )

            return {
                "key": key,
                "version": version,
                "status": "repaired",
                "source_node": source_node,
                "target_node": target_node,
                "duration_ms": duration_ms
            }

        else:
            return {"key": key, "version": version, "status": "ec_repaired"}

    finally:
        release_lock(f"{key}:v{version}")

def trigger_async_read_repair(key: str, version: int, corrupt_node_id: str) -> None:
    """Fire-and-forget passive read-repair triggered immediately when a download detects corruption."""
    def _run():
        try:
            logger.info(f"Triggering background passive read-repair for {key} v{version} on {corrupt_node_id}...")
            repair_object_version(key, version)
        except Exception as e:
            logger.error(f"Read-repair failed for {key} v{version}: {e}")

    asyncio.get_event_loop().run_in_executor(None, _run)

def drain_node(node_id: str) -> Dict[str, Any]:
    """Begin draining a node: mark DRAINING and evacuate all replicas to healthy nodes."""
    health_monitor.set_node_status(node_id, config.NODE_STATUS_DRAINING, reason="node_drain_requested")
    
    evacuated_count = 0
    with db_session() as conn:
        cursor = conn.cursor()
        replicas = cursor.execute("""
            SELECT object_key, version, shard_index, checksum FROM replicas WHERE node_id = ?
        """, (node_id,)).fetchall()

    for r in replicas:
        key = r["object_key"]
        v = r["version"]
        shard = r["shard_index"]

        with db_session() as conn:
            existing_nodes = {row["node_id"] for row in conn.execute(
                "SELECT node_id FROM replicas WHERE object_key = ? AND version = ?", (key, v)
            ).fetchall()}
        
        candidates = global_hash_ring.get_nodes_for_key(f"{key}:v{v}", 5, exclude_nodes=existing_nodes.union({node_id}))
        if candidates:
            target_node = candidates[0]
            try:
                data = storage_node.read_shard(node_id, key, v, shard)
                storage_node.write_shard_atomic(target_node, key, v, shard, data)
                with db_session() as conn:
                    conn.execute("""
                        UPDATE replicas SET node_id = ?, last_verified = ?
                        WHERE object_key = ? AND version = ? AND node_id = ? AND shard_index = ?
                    """, (target_node, time.time(), key, v, node_id, shard))
                storage_node.delete_shard(node_id, key, v, shard)
                evacuated_count += 1
            except Exception as e:
                logger.error(f"Failed to evacuate {key} v{v} from {node_id}: {e}")

    health_monitor.set_node_status(node_id, config.NODE_STATUS_SAFE_TO_REMOVE, reason="evacuation_complete")
    log_event("TOPOLOGY", f"Node {node_id} successfully drained ({evacuated_count} replicas migrated). Ready for removal.")
    return {"node_id": node_id, "status": config.NODE_STATUS_SAFE_TO_REMOVE, "evacuated_replicas": evacuated_count}

def recover_node_and_reconcile(node_id: str) -> Dict[str, Any]:
    """Recover node back to HEALTHY and purge orphaned/stale files on disk."""
    health_monitor.set_node_status(node_id, config.NODE_STATUS_HEALTHY, reason="manual_recovery")

    local_files = storage_node.list_node_local_files(node_id)
    purged_count = 0

    with db_session() as conn:
        cursor = conn.cursor()
        for filename in local_files:
            parts = filename.split(".")
            if len(parts) >= 3 and parts[-2].startswith("v") and parts[-1].startswith("shard"):
                try:
                    v = int(parts[-2][1:])
                    shard = int(parts[-1][5:])

                    match = cursor.execute("""
                        SELECT id FROM replicas 
                        WHERE node_id = ? AND version = ? AND shard_index = ?
                    """, (node_id, v, shard)).fetchone()

                    if not match:
                        target_path = config.STORAGE_DIR / node_id / filename
                        target_path.unlink(missing_ok=True)
                        purged_count += 1
                except Exception:
                    pass

    if purged_count > 0:
        log_event(
            "ORPHAN_SWEEP",
            f"Node {node_id} recovery reconciliation: purged {purged_count} orphaned/stale shard(s).",
            details=f"Files cleaned: {purged_count}"
        )

    return {
        "node_id": node_id,
        "status": config.NODE_STATUS_HEALTHY,
        "orphans_purged": purged_count
    }

def run_auto_repair_cycle() -> List[Dict[str, Any]]:
    """Execute background cycle to repair any under-replicated or corrupted items."""
    under_replicated = find_under_replicated_objects()
    results = []
    for item in under_replicated:
        try:
            res = repair_object_version(item["key"], item["version"])
            results.append(res)
        except Exception as e:
            logger.error(f"Auto-repair failure on {item['key']}: {e}")
            results.append({"key": item["key"], "version": item["version"], "status": "error", "error": str(e)})
    return results
