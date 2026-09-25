"""Basic storage rebalancing across nodes."""
import time
import logging
from typing import Dict, Any, List

from database import db_session, log_event
import storage
from health import get_all_nodes
from config import NODE_STATUS_HEALTHY, REPLICA_STATUS_HEALTHY

logger = logging.getLogger("vault.rebalance")

def calculate_imbalance() -> Dict[str, Any]:
    """Calculate storage usage across healthy nodes."""
    nodes = get_all_nodes()
    healthy_nodes = [n for n in nodes if n["status"] == NODE_STATUS_HEALTHY]

    if len(healthy_nodes) < 2:
        return {"rebalance_needed": False, "reason": "Fewer than 2 healthy nodes available"}

    usages = {n["node_id"]: n["used_storage"] for n in healthy_nodes}
    min_node = min(usages, key=usages.get)
    max_node = max(usages, key=usages.get)
    diff = usages[max_node] - usages[min_node]

    return {
        "rebalance_needed": diff > 0,
        "usages": usages,
        "max_node": max_node,
        "max_bytes": usages[max_node],
        "min_node": min_node,
        "min_bytes": usages[min_node],
        "diff_bytes": diff
    }

def rebalance_cluster() -> Dict[str, Any]:
    """
    Rebalance storage by moving replicas from highest-utilized node to lowest-utilized node.
    Strictly adheres to: COPY -> VERIFY -> UPDATE METADATA -> REMOVE OLD.
    """
    imbalance = calculate_imbalance()
    if not imbalance.get("rebalance_needed"):
        return {
            "status": "balanced",
            "message": "Cluster storage is already balanced or insufficient nodes to migrate.",
            "moved_count": 0,
            "details": []
        }

    source_node = imbalance["max_node"]
    target_node = imbalance["min_node"]
    moved = []

    with db_session() as conn:
        # Find replicas present on source_node that are NOT on target_node
        candidate_replicas = conn.execute("""
            SELECT r.object_id, r.checksum, o.size, o.filename
            FROM replicas r
            JOIN objects o ON r.object_id = o.object_id
            WHERE r.node_id = ? AND r.status = 'HEALTHY'
              AND r.object_id NOT IN (
                  SELECT object_id FROM replicas WHERE node_id = ?
              )
            LIMIT 2
        """, (source_node, target_node)).fetchall()

        for cand in candidate_replicas:
            obj_id = cand["object_id"]
            expected_checksum = cand["checksum"]

            try:
                # 1. Read source
                data = storage.read_replica(source_node, obj_id)
                if storage.calculate_checksum(data) != expected_checksum:
                    continue

                # 2. Write target
                written_size, actual_checksum = storage.write_replica(target_node, obj_id, data)

                # 3. Verify target
                if actual_checksum != expected_checksum:
                    storage.delete_replica(target_node, obj_id)
                    continue

                # 4. Update metadata
                conn.execute("""
                    UPDATE replicas SET node_id = ?, last_verified = ?
                    WHERE object_id = ? AND node_id = ?
                """, (target_node, time.time(), obj_id, source_node))

                # 5. Only after metadata update, safely remove old physical replica
                storage.delete_replica(source_node, obj_id)

                log_event(
                    "REBALANCE",
                    f"Replica {obj_id} ('{cand['filename']}') moved from {source_node} to {target_node}",
                    details=f"Transferred {cand['size']} bytes, verified SHA-256",
                    conn=conn
                )

                moved.append({
                    "object_id": obj_id,
                    "filename": cand["filename"],
                    "from_node": source_node,
                    "to_node": target_node,
                    "bytes": cand["size"]
                })

            except Exception as e:
                logger.error(f"Error during rebalancing move for {obj_id}: {e}")

    return {
        "status": "success",
        "moved_count": len(moved),
        "details": moved,
        "message": f"Successfully rebalanced {len(moved)} replicas from {source_node} to {target_node}."
    }
