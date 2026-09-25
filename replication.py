"""Replication manager supporting Quorum writes, failover reads, and multi-node placement."""
import uuid
import time
import asyncio
from typing import Tuple, List, Dict, Any, Optional
import logging

from config import (
    DEFAULT_REPLICATION_FACTOR,
    WRITE_QUORUM,
    NODE_STATUS_HEALTHY,
    REPLICA_STATUS_HEALTHY,
    REPLICA_STATUS_CORRUPTED,
)
from database import db_session, log_event
import storage

logger = logging.getLogger("vault.replication")

def get_object_metadata(object_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve full object metadata and its replicas from the database."""
    with db_session() as conn:
        cursor = conn.cursor()
        obj = cursor.execute("""
            SELECT object_id, filename, size, checksum, version, replication_factor, created_at, updated_at
            FROM objects WHERE object_id = ?
        """, (object_id,)).fetchone()

        if not obj:
            return None

        replicas = cursor.execute("""
            SELECT r.id, r.object_id, r.node_id, r.version, r.checksum, r.status, r.last_verified, n.status as node_status
            FROM replicas r
            JOIN nodes n ON r.node_id = n.node_id
            WHERE r.object_id = ?
        """, (object_id,)).fetchall()

        replicas_list = [dict(r) for r in replicas]
        
        # Determine aggregate health status
        healthy_reps = [r for r in replicas_list if r["status"] == REPLICA_STATUS_HEALTHY and r["node_status"] == NODE_STATUS_HEALTHY]
        corrupted_reps = [r for r in replicas_list if r["status"] == REPLICA_STATUS_CORRUPTED]
        
        if len(corrupted_reps) > 0:
            health_status = "CORRUPTED"
        elif len(healthy_reps) < obj["replication_factor"]:
            health_status = "UNDER_REPLICATED" if len(healthy_reps) > 0 else "CRITICAL"
        else:
            health_status = "HEALTHY"

        result = dict(obj)
        result["replicas"] = replicas_list
        result["health_status"] = health_status
        return result

def list_all_objects() -> List[Dict[str, Any]]:
    """List all objects with their current replica and health statuses."""
    with db_session() as conn:
        cursor = conn.cursor()
        objects = cursor.execute("""
            SELECT object_id, filename, size, checksum, version, replication_factor, created_at, updated_at
            FROM objects
            ORDER BY created_at DESC
        """).fetchall()

        results = []
        for obj in objects:
            meta = get_object_metadata(obj["object_id"])
            if meta:
                results.append(meta)
        return results

async def store_object(filename: str, data: bytes, replication_factor: int = DEFAULT_REPLICATION_FACTOR) -> Dict[str, Any]:
    """
    Store an object across nodes adhering to replication factor and write quorum.
    Returns object metadata upon reaching quorum.
    """
    object_id = f"obj_{uuid.uuid4().hex[:10]}"
    size = len(data)
    checksum = storage.calculate_checksum(data)
    now = time.time()

    # Step 1: Select healthy candidate nodes, sorting by lowest storage usage
    with db_session() as conn:
        cursor = conn.cursor()
        nodes = cursor.execute("""
            SELECT node_id, used_storage, capacity 
            FROM nodes 
            WHERE status = ? 
            ORDER BY used_storage ASC
        """, (NODE_STATUS_HEALTHY,)).fetchall()
        healthy_nodes = [n["node_id"] for n in nodes]

    quorum = min(WRITE_QUORUM, replication_factor)

    if len(healthy_nodes) < quorum:
        raise RuntimeError(f"Insufficient healthy nodes ({len(healthy_nodes)}) to satisfy write quorum ({quorum})")

    target_nodes = healthy_nodes[:replication_factor]
    successful_nodes: List[str] = []

    # Step 2: Write replicas to target nodes in parallel
    async def write_to_node(node_id: str):
        try:
            written_size, actual_checksum = await asyncio.to_thread(storage.write_replica, node_id, object_id, data)
            if actual_checksum == checksum and written_size == size:
                return (node_id, True, None)
            return (node_id, False, "Checksum or size mismatch")
        except Exception as e:
            return (node_id, False, str(e))

    tasks = [write_to_node(node_id) for node_id in target_nodes]
    write_results = await asyncio.gather(*tasks)

    for node_id, success, err in write_results:
        if success:
            successful_nodes.append(node_id)
        else:
            logger.warning(f"Failed write replica to {node_id}: {err}")

    # Step 3: Quorum verification
    if len(successful_nodes) < quorum:
        # Roll back any partial writes
        for node_id in successful_nodes:
            try:
                storage.delete_replica(node_id, object_id)
            except Exception:
                pass
        raise RuntimeError(f"Write quorum failed: only {len(successful_nodes)}/{quorum} replicas written successfully")

    # Step 4: Record metadata atomically in SQLite
    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO objects (object_id, filename, size, checksum, version, replication_factor, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        """, (object_id, filename, size, checksum, replication_factor, now, now))

        for node_id in successful_nodes:
            cursor.execute("""
                INSERT INTO replicas (object_id, node_id, version, checksum, status, last_verified)
                VALUES (?, ?, 1, ?, ?, ?)
            """, (object_id, node_id, checksum, REPLICA_STATUS_HEALTHY, now))

        log_event("UPLOAD", f"Object {object_id} ('{filename}', {size} bytes) uploaded", details=f"Checksum: {checksum}", conn=conn)
        log_event("REPLICATION", f"{object_id} written to nodes: {', '.join(successful_nodes)} (Quorum {len(successful_nodes)}/{quorum} met)", conn=conn)

    metadata = get_object_metadata(object_id)
    return metadata

async def retrieve_object(object_id: str, verify: bool = True) -> Tuple[bytes, Dict[str, Any]]:
    """
    Retrieve object with automatic failover across healthy replicas.
    Optionally verifies checksum on read.
    """
    metadata = get_object_metadata(object_id)
    if not metadata:
        raise FileNotFoundError(f"Object {object_id} not found in metadata")

    expected_checksum = metadata["checksum"]
    replicas = metadata.get("replicas", [])

    # Filter candidate replicas located on HEALTHY nodes
    candidates = [r for r in replicas if r["node_status"] == NODE_STATUS_HEALTHY and r["status"] == REPLICA_STATUS_HEALTHY]

    if not candidates:
        raise RuntimeError(f"Object {object_id} has no available healthy replicas on active nodes")

    last_error = None
    for candidate in candidates:
        node_id = candidate["node_id"]
        try:
            data = await asyncio.to_thread(storage.read_replica, node_id, object_id)
            
            if verify:
                actual_checksum = storage.calculate_checksum(data)
                if actual_checksum != expected_checksum:
                    # Mark replica as corrupted in DB
                    with db_session() as conn:
                        conn.execute("""
                            UPDATE replicas SET status = ? WHERE object_id = ? AND node_id = ?
                        """, (REPLICA_STATUS_CORRUPTED, object_id, node_id))
                        log_event("INTEGRITY", f"Replica of {object_id} on {node_id} is CORRUPTED (mismatch {actual_checksum[:8]} != {expected_checksum[:8]})", level="ERROR", conn=conn)
                    continue

            # Return first successfully read healthy replica
            return data, metadata

        except Exception as e:
            last_error = e
            logger.warning(f"Failed to read {object_id} from {node_id}: {e}")
            continue

    raise RuntimeError(f"Failed to retrieve {object_id} from any candidate replica: {last_error}")

def delete_object(object_id: str) -> bool:
    """Delete an object and all physical replicas."""
    metadata = get_object_metadata(object_id)
    if not metadata:
        return False

    # Delete physical replicas on all nodes
    for replica in metadata.get("replicas", []):
        try:
            storage.delete_replica(replica["node_id"], object_id)
        except Exception as e:
            logger.warning(f"Error removing physical replica on {replica['node_id']}: {e}")

    # Remove database entries (cascade deletes replicas)
    with db_session() as conn:
        conn.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))
        log_event("DELETE", f"Object {object_id} ('{metadata['filename']}') deleted", conn=conn)

    return True
