"""Node health monitoring, failure detection, and simulation."""
import time
import logging
from typing import List, Dict, Any, Optional

from database import db_session, log_event
from storage import get_node_used_bytes
from config import (
    NODE_STATUS_HEALTHY,
    NODE_STATUS_FAILED,
    NODE_STATUS_RECOVERING,
    NODE_STATUS_DISCONNECTED,
    HEARTBEAT_TIMEOUT_SECONDS,
)

logger = logging.getLogger("vault.health")

def get_all_nodes() -> List[Dict[str, Any]]:
    """Retrieve all nodes with updated storage and replica counts."""
    with db_session() as conn:
        cursor = conn.cursor()
        nodes = cursor.execute("""
            SELECT node_id, status, capacity, used_storage, last_heartbeat, address
            FROM nodes
            ORDER BY node_id ASC
        """).fetchall()

        result = []
        for n in nodes:
            node_id = n["node_id"]
            # Recalculate actual disk usage dynamically
            actual_used = get_node_used_bytes(node_id)
            cursor.execute("UPDATE nodes SET used_storage = ? WHERE node_id = ?", (actual_used, node_id))
            
            # Count healthy replicas stored on this node
            rep_count = cursor.execute("""
                SELECT COUNT(*) as c FROM replicas WHERE node_id = ? AND status = 'HEALTHY'
            """, (node_id,)).fetchone()["c"]

            result.append({
                "node_id": node_id,
                "status": n["status"],
                "capacity": n["capacity"],
                "used_storage": actual_used,
                "last_heartbeat": n["last_heartbeat"],
                "address": n["address"],
                "active_replicas_count": rep_count
            })
        return result

def get_healthy_nodes() -> List[str]:
    """Return list of node_ids that are currently HEALTHY."""
    with db_session() as conn:
        cursor = conn.cursor()
        rows = cursor.execute("""
            SELECT node_id FROM nodes WHERE status = ?
        """, (NODE_STATUS_HEALTHY,)).fetchall()
        return [r["node_id"] for r in rows]

def set_node_status(node_id: str, new_status: str, reason: str = "manual") -> bool:
    """Update a node's operational status and record an event."""
    with db_session() as conn:
        cursor = conn.cursor()
        current = cursor.execute("SELECT status FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
        if not current:
            return False

        old_status = current["status"]
        if old_status == new_status:
            return True

        cursor.execute("UPDATE nodes SET status = ?, last_heartbeat = ? WHERE node_id = ?", (new_status, time.time(), node_id))
        
        category = "FAILURE" if new_status in (NODE_STATUS_FAILED, NODE_STATUS_DISCONNECTED) else "RECOVERY"
        msg = f"Node {node_id} status changed from {old_status} to {new_status} (trigger: {reason})"
        log_event(category, msg, details=f"Reason: {reason}", level="WARNING" if category == "FAILURE" else "INFO", conn=conn)

        return True

def record_heartbeat(node_id: str) -> bool:
    """Record heartbeat for a node."""
    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE nodes SET last_heartbeat = ? WHERE node_id = ?
        """, (time.time(), node_id))
        return cursor.rowcount > 0

def pulse_active_nodes():
    """Simulate healthy storage nodes actively emitting heartbeats."""
    now = time.time()
    with db_session() as conn:
        conn.execute("""
            UPDATE nodes SET last_heartbeat = ? WHERE status = ?
        """, (now, NODE_STATUS_HEALTHY))

def check_heartbeats() -> List[str]:
    """Check for timed-out nodes and mark them FAILED."""
    now = time.time()
    failed_nodes = []
    with db_session() as conn:
        cursor = conn.cursor()
        nodes = cursor.execute("SELECT node_id, last_heartbeat, status FROM nodes").fetchall()
        for n in nodes:
            if n["status"] == NODE_STATUS_HEALTHY and (now - n["last_heartbeat"] > HEARTBEAT_TIMEOUT_SECONDS):
                node_id = n["node_id"]
                cursor.execute("UPDATE nodes SET status = ? WHERE node_id = ?", (NODE_STATUS_FAILED, node_id))
                log_event(
                    "FAILURE",
                    f"Node {node_id} missed heartbeat for {round(now - n['last_heartbeat'], 1)}s. Marked FAILED.",
                    level="WARNING",
                    conn=conn
                )
                failed_nodes.append(node_id)
    return failed_nodes
