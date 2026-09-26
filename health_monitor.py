"""
Health Monitor: Node failure detection, heartbeat tracking,
dynamic node membership, and status lifecycle management.
"""
import time
import logging
from typing import List, Dict, Any, Optional

import config
from metadata_service import db_session, log_event, refresh_hash_ring
import storage_node

logger = logging.getLogger("vault.health_monitor")

def get_all_nodes() -> List[Dict[str, Any]]:
    """Retrieve all cluster nodes with real-time disk consumption and active replicas count."""
    with db_session() as conn:
        cursor = conn.cursor()
        nodes = cursor.execute("""
            SELECT node_id, host, port, status, capacity, used_storage, last_heartbeat
            FROM nodes
            ORDER BY node_id ASC
        """).fetchall()

        result = []
        for n in nodes:
            node_id = n["node_id"]
            actual_used = storage_node.get_node_used_bytes(node_id)
            cursor.execute("UPDATE nodes SET used_storage = ? WHERE node_id = ?", (actual_used, node_id))

            rep_count = cursor.execute("""
                SELECT COUNT(*) as c FROM replicas WHERE node_id = ? AND status = ?
            """, (node_id, config.REPLICA_STATUS_HEALTHY)).fetchone()["c"]

            result.append({
                "node_id": node_id,
                "host": n["host"],
                "port": n["port"],
                "status": n["status"],
                "capacity": n["capacity"],
                "used_storage": actual_used,
                "last_heartbeat": n["last_heartbeat"],
                "active_replicas_count": rep_count
            })
        return result

def get_healthy_node_ids() -> List[str]:
    """Return list of node IDs currently marked HEALTHY."""
    with db_session() as conn:
        cursor = conn.cursor()
        rows = cursor.execute("""
            SELECT node_id FROM nodes WHERE status = ?
        """, (config.NODE_STATUS_HEALTHY,)).fetchall()
        return [r["node_id"] for r in rows]

def set_node_status(node_id: str, new_status: str, reason: str = "manual") -> bool:
    """Update node lifecycle status and refresh the consistent hash ring."""
    with db_session() as conn:
        cursor = conn.cursor()
        current = cursor.execute("SELECT status FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
        if not current:
            return False

        old_status = current["status"]
        if old_status == new_status:
            return True

        cursor.execute("UPDATE nodes SET status = ?, last_heartbeat = ? WHERE node_id = ?", (new_status, time.time(), node_id))

        category = "FAILURE" if new_status in (config.NODE_STATUS_FAILED, config.NODE_STATUS_DISCONNECTED) else "TOPOLOGY"
        level = "WARNING" if category == "FAILURE" else "INFO"
        msg = f"Node {node_id} transitioned: {old_status} -> {new_status} (reason: {reason})"
        log_event(category, msg, details=f"Trigger: {reason}", level=level, conn=conn)

    refresh_hash_ring()
    return True

def record_heartbeat(node_id: str) -> bool:
    """Acknowledge heartbeat received from a node."""
    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE nodes SET last_heartbeat = ? WHERE node_id = ?", (time.time(), node_id))
        return cursor.rowcount > 0

def pulse_active_nodes() -> None:
    """Simulate healthy storage nodes periodically sending heartbeats."""
    now = time.time()
    with db_session() as conn:
        conn.execute("""
            UPDATE nodes SET last_heartbeat = ? WHERE status = ?
        """, (now, config.NODE_STATUS_HEALTHY))

def check_heartbeats() -> List[str]:
    """Detect nodes that missed heartbeats beyond timeout and mark them FAILED."""
    now = time.time()
    failed_nodes = []
    with db_session() as conn:
        cursor = conn.cursor()
        nodes = cursor.execute("""
            SELECT node_id, last_heartbeat, status FROM nodes WHERE status = ?
        """, (config.NODE_STATUS_HEALTHY,)).fetchall()

        for n in nodes:
            if now - n["last_heartbeat"] > config.HEARTBEAT_TIMEOUT_SECONDS:
                failed_nodes.append(n["node_id"])

        for nid in failed_nodes:
            cursor.execute("UPDATE nodes SET status = ? WHERE node_id = ?", (config.NODE_STATUS_FAILED, nid))
            log_event(
                "FAILURE",
                f"Heartbeat timeout for node {nid}! Marked as FAILED.",
                level="ERROR",
                conn=conn
            )

    if failed_nodes:
        refresh_hash_ring()
    return failed_nodes

def register_dynamic_node(
    host: str = "127.0.0.1",
    port: int = 8000,
    node_id: Optional[str] = None,
    capacity: Optional[int] = None
) -> Dict[str, Any]:
    """Dynamically register a new storage node into the cluster and hash ring."""
    with db_session() as conn:
        cursor = conn.cursor()
        if not node_id:
            count = cursor.execute("SELECT COUNT(*) as c FROM nodes").fetchone()["c"]
            node_id = f"node{count + 1}"

        node_cap = capacity or config.DEFAULT_NODE_CAPACITY_BYTES
        now = time.time()

        cursor.execute("""
            INSERT INTO nodes (node_id, host, port, status, capacity, used_storage, last_heartbeat)
            VALUES (?, ?, ?, ?, ?, 0, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                host = excluded.host,
                port = excluded.port,
                status = ?,
                last_heartbeat = excluded.last_heartbeat
        """, (node_id, host, port, config.NODE_STATUS_HEALTHY, node_cap, now, config.NODE_STATUS_HEALTHY))

        storage_node.ensure_node_dir(node_id)
        log_event("TOPOLOGY", f"New node {node_id} ({host}:{port}) registered and joined hash ring.", conn=conn)

    refresh_hash_ring()
    return {"node_id": node_id, "host": host, "port": port, "status": config.NODE_STATUS_HEALTHY}

def remove_node_from_cluster(node_id: str) -> Dict[str, Any]:
    """
    Remove node from cluster.
    Enforces that draining must be complete (status SAFE_TO_REMOVE or FAILED) before removal.
    """
    with db_session() as conn:
        cursor = conn.cursor()
        node = cursor.execute("SELECT status FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
        if not node:
            raise FileNotFoundError(f"Node {node_id} not found")

        status = node["status"]
        if status in (config.NODE_STATUS_HEALTHY, config.NODE_STATUS_DRAINING):
            raise RuntimeError(f"Cannot remove node {node_id} while in status '{status}'. Drain it first.")

        cursor.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
        log_event("TOPOLOGY", f"Node {node_id} permanently removed from cluster.", conn=conn)

    refresh_hash_ring()
    return {"node_id": node_id, "status": "removed"}
