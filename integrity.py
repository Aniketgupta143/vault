"""Data integrity verification, checksum auditing, and corruption simulation."""
import time
import logging
from typing import Dict, Any, List, Optional

from database import db_session, log_event
import storage
from config import (
    REPLICA_STATUS_HEALTHY,
    REPLICA_STATUS_CORRUPTED,
    REPLICA_STATUS_MISSING,
)

logger = logging.getLogger("vault.integrity")

def verify_replica_integrity(object_id: str, node_id: str, expected_checksum: str) -> Dict[str, Any]:
    """Verify integrity of a single replica on disk."""
    target_path = storage.get_replica_path(node_id, object_id)
    now = time.time()

    if not target_path.exists():
        status = REPLICA_STATUS_MISSING
        actual_checksum = None
        is_valid = False
    else:
        try:
            actual_checksum = storage.calculate_file_checksum(target_path)
            is_valid = (actual_checksum == expected_checksum)
            status = REPLICA_STATUS_HEALTHY if is_valid else REPLICA_STATUS_CORRUPTED
        except Exception as e:
            status = REPLICA_STATUS_CORRUPTED
            actual_checksum = f"error: {str(e)}"
            is_valid = False

    # Update replica status in database
    with db_session() as conn:
        conn.execute("""
            UPDATE replicas 
            SET status = ?, checksum = COALESCE(?, checksum), last_verified = ?
            WHERE object_id = ? AND node_id = ?
        """, (status, actual_checksum if is_valid else expected_checksum, now, object_id, node_id))

        if not is_valid:
            log_event(
                "INTEGRITY",
                f"Checksum mismatch for object {object_id} on {node_id}! Expected {expected_checksum[:8]}..., actual {str(actual_checksum)[:8]}...",
                level="ERROR",
                conn=conn
            )
        else:
            log_event(
                "INTEGRITY",
                f"Checksum verified for object {object_id} on {node_id} (SHA-256 match)",
                level="INFO",
                conn=conn
            )

    return {
        "object_id": object_id,
        "node_id": node_id,
        "status": status,
        "is_valid": is_valid,
        "expected_checksum": expected_checksum,
        "actual_checksum": actual_checksum,
        "last_verified": now
    }

def verify_object_integrity(object_id: str) -> Dict[str, Any]:
    """Verify all replicas for an object."""
    with db_session() as conn:
        obj = conn.execute("SELECT object_id, checksum, replication_factor FROM objects WHERE object_id = ?", (object_id,)).fetchone()
        if not obj:
            raise FileNotFoundError(f"Object {object_id} not found")

        replicas = conn.execute("SELECT node_id FROM replicas WHERE object_id = ?", (object_id,)).fetchall()
        node_ids = [r["node_id"] for r in replicas]

    details = []
    healthy_count = 0
    corrupted_count = 0
    missing_count = 0

    for node_id in node_ids:
        res = verify_replica_integrity(object_id, node_id, obj["checksum"])
        details.append(res)
        if res["status"] == REPLICA_STATUS_HEALTHY:
            healthy_count += 1
        elif res["status"] == REPLICA_STATUS_CORRUPTED:
            corrupted_count += 1
        elif res["status"] == REPLICA_STATUS_MISSING:
            missing_count += 1

    return {
        "object_id": object_id,
        "expected_checksum": obj["checksum"],
        "replication_factor": obj["replication_factor"],
        "total_checked": len(node_ids),
        "healthy_count": healthy_count,
        "corrupted_count": corrupted_count,
        "missing_count": missing_count,
        "replicas": details
    }

def verify_all_objects() -> Dict[str, Any]:
    """Scrub all objects in the cluster and verify checksums."""
    with db_session() as conn:
        objects = conn.execute("SELECT object_id FROM objects").fetchall()

    results = []
    corrupted_total = 0
    healthy_total = 0

    for row in objects:
        res = verify_object_integrity(row["object_id"])
        results.append(res)
        corrupted_total += res["corrupted_count"]
        healthy_total += res["healthy_count"]

    return {
        "total_objects": len(objects),
        "total_healthy_replicas": healthy_total,
        "total_corrupted_replicas": corrupted_total,
        "objects": results
    }

def simulate_corruption(object_id: str, node_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Intentionally corrupt a replica file on disk for demonstration.
    """
    with db_session() as conn:
        obj = conn.execute("SELECT object_id, checksum FROM objects WHERE object_id = ?", (object_id,)).fetchone()
        if not obj:
            raise FileNotFoundError(f"Object {object_id} not found")

        if node_id:
            target_node = node_id
        else:
            rep = conn.execute("""
                SELECT node_id FROM replicas 
                WHERE object_id = ? AND status = 'HEALTHY' 
                LIMIT 1
            """, (object_id,)).fetchone()
            if not rep:
                raise RuntimeError("No healthy replica found to corrupt")
            target_node = rep["node_id"]

    # Physically corrupt the file on disk
    corrupted = storage.corrupt_replica(target_node, object_id)
    if not corrupted:
        raise RuntimeError(f"Failed to corrupt replica on node {target_node}")

    # Mark as corrupted in DB
    with db_session() as conn:
        conn.execute("""
            UPDATE replicas SET status = ? WHERE object_id = ? AND node_id = ?
        """, (REPLICA_STATUS_CORRUPTED, object_id, target_node))
        log_event(
            "CORRUPTION",
            f"Simulated byte corruption injected into {object_id} on {target_node}",
            level="WARNING",
            conn=conn
        )

    return {
        "status": "success",
        "object_id": object_id,
        "corrupted_node": target_node,
        "message": f"Replica of {object_id} on {target_node} was intentionally corrupted"
    }
