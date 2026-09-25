"""Automatic replica healing, repair jobs, and under-replication reconciliation."""
import time
import logging
from typing import Dict, Any, List, Optional

from database import db_session, log_event
import storage
from config import (
    NODE_STATUS_HEALTHY,
    NODE_STATUS_FAILED,
    REPLICA_STATUS_HEALTHY,
    REPLICA_STATUS_CORRUPTED,
    REPLICA_STATUS_MISSING,
    REPAIR_STATUS_PENDING,
    REPAIR_STATUS_IN_PROGRESS,
    REPAIR_STATUS_COMPLETED,
    REPAIR_STATUS_FAILED,
)

logger = logging.getLogger("vault.repair")

def find_under_replicated_objects() -> List[Dict[str, Any]]:
    """
    Identify objects where healthy replica count < replication_factor,
    or objects containing corrupted/missing replicas.
    """
    with db_session() as conn:
        objects = conn.execute("""
            SELECT object_id, filename, checksum, replication_factor, size, version
            FROM objects
        """).fetchall()

        under_replicated = []
        for obj in objects:
            object_id = obj["object_id"]
            # Count replicas that are HEALTHY and on HEALTHY nodes
            healthy_count = conn.execute("""
                SELECT COUNT(*) as c
                FROM replicas r
                JOIN nodes n ON r.node_id = n.node_id
                WHERE r.object_id = ? 
                  AND r.status = 'HEALTHY' 
                  AND n.status = 'HEALTHY'
            """, (object_id,)).fetchone()["c"]

            # Count corrupted or missing replicas
            bad_count = conn.execute("""
                SELECT COUNT(*) as c
                FROM replicas
                WHERE object_id = ? AND status IN ('CORRUPTED', 'MISSING')
            """, (object_id,)).fetchone()["c"]

            if healthy_count < obj["replication_factor"] or bad_count > 0:
                under_replicated.append({
                    "object_id": object_id,
                    "filename": obj["filename"],
                    "replication_factor": obj["replication_factor"],
                    "healthy_replicas": healthy_count,
                    "bad_replicas": bad_count,
                    "needs_repair": True
                })

        return under_replicated

def repair_object(object_id: str) -> Dict[str, Any]:
    """
    Execute self-healing repair for an object following the safety invariant:
    COPY -> VERIFY (SHA-256) -> REGISTER -> CLEANUP STALE
    """
    start_time = time.time()

    with db_session() as conn:
        obj = conn.execute("""
            SELECT object_id, filename, size, checksum, version, replication_factor
            FROM objects WHERE object_id = ?
        """, (object_id,)).fetchone()

        if not obj:
            raise FileNotFoundError(f"Object {object_id} not found")

        expected_checksum = obj["checksum"]

        # Fetch current replicas with node status
        replicas = conn.execute("""
            SELECT r.id, r.node_id, r.status as replica_status, n.status as node_status
            FROM replicas r
            JOIN nodes n ON r.node_id = n.node_id
            WHERE r.object_id = ?
        """, (object_id,)).fetchall()

        # Find healthy source candidates: replica HEALTHY and node HEALTHY
        viable_sources = []
        for r in replicas:
            if r["replica_status"] == REPLICA_STATUS_HEALTHY and r["node_status"] == NODE_STATUS_HEALTHY:
                # Double-check physical file exists and is intact
                try:
                    p = storage.get_replica_path(r["node_id"], object_id)
                    if p.exists() and storage.calculate_file_checksum(p) == expected_checksum:
                        viable_sources.append(r["node_id"])
                except Exception:
                    continue

        if not viable_sources:
            msg = f"Cannot repair {object_id}: No healthy source replica exists in cluster!"
            log_event("REPAIR", msg, level="ERROR", conn=conn)
            raise RuntimeError(msg)

        source_node = viable_sources[0]

        # Check if already fully replicated and healthy
        if len(viable_sources) >= obj["replication_factor"]:
            return {
                "object_id": object_id,
                "status": "already_healthy",
                "message": f"Object {object_id} already has {len(viable_sources)} healthy replicas."
            }

        # Find target node to host the new replica:
        # Must be HEALTHY and not already hosting a viable replica
        all_healthy_nodes = conn.execute("""
            SELECT node_id, used_storage 
            FROM nodes 
            WHERE status = ? 
            ORDER BY used_storage ASC
        """, (NODE_STATUS_HEALTHY,)).fetchall()

        healthy_node_ids = [n["node_id"] for n in all_healthy_nodes]
        target_node = None
        target_type = None  # "in_place_fix" or "replacement_node"

        # Case 1: Is there a corrupted/missing replica on a healthy node? Repair in-place
        corrupted_on_healthy = [
            r["node_id"] for r in replicas 
            if r["replica_status"] in (REPLICA_STATUS_CORRUPTED, REPLICA_STATUS_MISSING)
            and r["node_status"] == NODE_STATUS_HEALTHY
        ]
        if corrupted_on_healthy:
            target_node = corrupted_on_healthy[0]
            target_type = "in_place_fix"
        else:
            # Case 2: Replacement node (e.g. node4 replacing failed node)
            candidate_nodes = [nid for nid in healthy_node_ids if nid not in viable_sources]
            if candidate_nodes:
                target_node = candidate_nodes[0]
                target_type = "replacement_node"

        if not target_node:
            msg = f"Cannot repair {object_id}: No healthy target node available"
            log_event("REPAIR", msg, level="WARNING", conn=conn)
            raise RuntimeError(msg)

        # Log repair job start in DB
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO repair_jobs (object_id, source_node, target_node, status, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (object_id, source_node, target_node, REPAIR_STATUS_IN_PROGRESS, start_time))
        job_id = cursor.lastrowid
        log_event("REPAIR", f"{object_id} repair started (Source: {source_node} -> Target: {target_node})", conn=conn)

    # Step 1: Read verified source data
    source_data = storage.read_replica(source_node, object_id)
    if storage.calculate_checksum(source_data) != expected_checksum:
        with db_session() as conn:
            conn.execute("UPDATE repair_jobs SET status = ?, error = ? WHERE id = ?", (REPAIR_STATUS_FAILED, "Source data checksum mismatch", job_id))
        raise RuntimeError("Source replica data failed checksum verification during repair")

    # Step 2: Copy to target node using atomic write
    try:
        written_size, actual_checksum = storage.write_replica(target_node, object_id, source_data)
        
        # Step 3: Verify target checksum before registering
        if actual_checksum != expected_checksum:
            storage.delete_replica(target_node, object_id)
            with db_session() as conn:
                conn.execute("UPDATE repair_jobs SET status = ?, error = ? WHERE id = ?", (REPAIR_STATUS_FAILED, "Target checksum mismatch", job_id))
            raise RuntimeError(f"Target replica failed verification: {actual_checksum} != {expected_checksum}")

        # Step 4: Register target replica into metadata
        duration_ms = round((time.time() - start_time) * 1000, 2)
        with db_session() as conn:
            # If in-place fix, update existing record; otherwise insert/replace
            conn.execute("""
                INSERT INTO replicas (object_id, node_id, version, checksum, status, last_verified)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(object_id, node_id) DO UPDATE SET
                    checksum = excluded.checksum,
                    status = 'HEALTHY',
                    last_verified = excluded.last_verified
            """, (object_id, target_node, obj["version"], actual_checksum, REPLICA_STATUS_HEALTHY, time.time()))

            # If this was a replacement for a FAILED node, remove the stale replica entry
            # so the replica count accurately reflects the new active placement
            if target_type == "replacement_node":
                stale_replica = conn.execute("""
                    SELECT r.id, r.node_id FROM replicas r
                    JOIN nodes n ON r.node_id = n.node_id
                    WHERE r.object_id = ? AND n.status != 'HEALTHY'
                    LIMIT 1
                """, (object_id,)).fetchone()
                if stale_replica:
                    conn.execute("DELETE FROM replicas WHERE id = ?", (stale_replica["id"],))

            # Complete repair job
            conn.execute("""
                UPDATE repair_jobs 
                SET status = ?, duration_ms = ?, completed_at = ?
                WHERE id = ?
            """, (REPAIR_STATUS_COMPLETED, duration_ms, time.time(), job_id))

            log_event(
                "REPAIR",
                f"{object_id} successfully repaired to {target_node} from {source_node} ({duration_ms}ms)",
                details=f"Target type: {target_type}",
                conn=conn
            )

        return {
            "object_id": object_id,
            "status": "repaired",
            "source_node": source_node,
            "target_node": target_node,
            "target_type": target_type,
            "duration_ms": duration_ms,
            "checksum": actual_checksum
        }

    except Exception as e:
        with db_session() as conn:
            conn.execute("""
                UPDATE repair_jobs 
                SET status = ?, error = ?, completed_at = ?
                WHERE id = ?
            """, (REPAIR_STATUS_FAILED, str(e), time.time(), job_id))
            log_event("REPAIR", f"Repair failed for {object_id}: {e}", level="ERROR", conn=conn)
        raise

def run_auto_repair_cycle() -> List[Dict[str, Any]]:
    """Scan and automatically repair any under-replicated or corrupted objects."""
    under_replicated = find_under_replicated_objects()
    results = []
    for item in under_replicated:
        try:
            res = repair_object(item["object_id"])
            results.append(res)
        except Exception as e:
            logger.error(f"Auto-repair error for {item['object_id']}: {e}")
            results.append({
                "object_id": item["object_id"],
                "status": "error",
                "error": str(e)
            })
    return results
