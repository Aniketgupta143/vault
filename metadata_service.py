"""
Metadata Service: Database management, real object versioning, and row-level distributed locks.
Supports SQLite with WAL mode and optional Raft metadata synchronization.
"""
import sqlite3
import time
import logging
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

import config
from hash_ring import ConsistentHashRing

logger = logging.getLogger("vault.metadata")

# Global consistent hash ring instance
global_hash_ring = ConsistentHashRing()

# In-memory queue for real-time Server-Sent Events (SSE)
sse_subscribers: List[Any] = []

def get_db_connection() -> sqlite3.Connection:
    """Create a new SQLite connection configured for concurrent access."""
    conn = sqlite3.connect(str(config.DB_PATH), timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

@contextmanager
def db_session():
    """Context manager for SQLite database sessions with automatic commit/rollback."""
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db() -> None:
    """Initialize database tables, indexes, and bootstrap nodes."""
    config.STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    with db_session() as conn:
        cursor = conn.cursor()

        # Nodes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                host TEXT NOT NULL DEFAULT '127.0.0.1',
                port INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'HEALTHY',
                capacity INTEGER NOT NULL,
                used_storage INTEGER NOT NULL DEFAULT 0,
                last_heartbeat REAL NOT NULL
            );
        """)

        # Objects table with versioning
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                storage_mode TEXT NOT NULL DEFAULT 'replication',
                replication_factor INTEGER NOT NULL DEFAULT 3,
                created_at REAL NOT NULL,
                UNIQUE (key, version)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_objects_key ON objects(key);")

        # Replicas and Erasure Coding shards
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS replicas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_key TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                node_id TEXT NOT NULL,
                shard_index INTEGER NOT NULL DEFAULT 0,
                checksum TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'HEALTHY',
                last_verified REAL NOT NULL,
                FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
                UNIQUE (object_key, version, node_id, shard_index)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_replicas_lookup ON replicas(object_key, version);")

        # Distributed Row-Level Locks with TTL
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS locks (
                key TEXT PRIMARY KEY,
                operation TEXT NOT NULL,
                owner TEXT NOT NULL,
                expires_at REAL NOT NULL
            );
        """)

        # Repair Jobs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS repair_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_key TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                source_node TEXT NOT NULL,
                target_node TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                duration_ms REAL DEFAULT 0,
                error TEXT,
                created_at REAL NOT NULL,
                completed_at REAL
            );
        """)

        # Audit Events Log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT NOT NULL DEFAULT 'INFO',
                category TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT,
                timestamp REAL NOT NULL
            );
        """)

        # Seed initial nodes if empty
        existing_nodes = cursor.execute("SELECT COUNT(*) as c FROM nodes").fetchone()["c"]
        if existing_nodes == 0:
            now = time.time()
            for n in config.DEFAULT_NODES:
                cursor.execute("""
                    INSERT INTO nodes (node_id, host, port, status, capacity, used_storage, last_heartbeat)
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                """, (n["node_id"], n["host"], n["port"], config.NODE_STATUS_HEALTHY, config.DEFAULT_NODE_CAPACITY_BYTES, now))

    # Populate hash ring with active nodes
    refresh_hash_ring()
    log_event("SYSTEM", "Metadata service and database initialized successfully")

def refresh_hash_ring() -> None:
    """Sync the in-memory consistent hash ring with healthy nodes in the database."""
    with db_session() as conn:
        cursor = conn.cursor()
        nodes = cursor.execute("""
            SELECT node_id, status FROM nodes WHERE status != ?
        """, (config.NODE_STATUS_FAILED,)).fetchall()

        current_active = {n["node_id"] for n in nodes if n["status"] == config.NODE_STATUS_HEALTHY}
        
        # Add missing nodes to ring
        for nid in current_active:
            if nid not in global_hash_ring.nodes:
                global_hash_ring.add_node(nid)

        # Remove dead nodes from ring
        for nid in list(global_hash_ring.nodes):
            if nid not in current_active:
                global_hash_ring.remove_node(nid)

def log_event(category: str, message: str, details: Optional[str] = None, level: str = "INFO", conn: Optional[sqlite3.Connection] = None) -> None:
    """Record an audit log event and push to live SSE subscribers."""
    now = time.time()
    event_dict = {
        "level": level,
        "category": category,
        "message": message,
        "details": details,
        "timestamp": now
    }

    if conn is not None:
        conn.execute("""
            INSERT INTO events (level, category, message, details, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (level, category, message, details, now))
    else:
        with db_session() as c:
            c.execute("""
                INSERT INTO events (level, category, message, details, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (level, category, message, details, now))

    # Broadcast to SSE subscribers
    for queue in list(sse_subscribers):
        try:
            queue.put_nowait(event_dict)
        except Exception:
            pass

# ----------------- Distributed Row-Level Locks -----------------

def acquire_lock(key: str, operation: str, ttl_seconds: int = config.LOCK_TTL_SECONDS, owner: str = "gateway") -> bool:
    """
    Acquire a row-level lease on an object key with auto-expiring TTL.
    Returns True if acquired, False if locked by another active operation.
    """
    now = time.time()
    expires_at = now + ttl_seconds

    with db_session() as conn:
        cursor = conn.cursor()
        # Clean expired lock if any
        cursor.execute("DELETE FROM locks WHERE key = ? AND expires_at < ?", (key, now))

        # Attempt to insert lock
        try:
            cursor.execute("""
                INSERT INTO locks (key, operation, owner, expires_at)
                VALUES (?, ?, ?, ?)
            """, (key, operation, owner, expires_at))
            return True
        except sqlite3.IntegrityError:
            # Key is currently locked
            return False

def release_lock(key: str) -> bool:
    """Release an acquired row-level lock."""
    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM locks WHERE key = ?", (key,))
        return cursor.rowcount > 0

# ----------------- Versioning & Metadata Queries -----------------

def get_next_version(key: str) -> int:
    """Determine the next sequential version number for an object key."""
    with db_session() as conn:
        cursor = conn.cursor()
        row = cursor.execute("SELECT MAX(version) as max_v FROM objects WHERE key = ?", (key,)).fetchone()
        if row and row["max_v"] is not None:
            return row["max_v"] + 1
        return 1

def get_object_metadata(key: str, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Retrieve object metadata and replicas for a specific version (or latest if omitted).
    Calculates dynamic health status based on replica health and node status.
    """
    with db_session() as conn:
        cursor = conn.cursor()
        if version is None:
            obj = cursor.execute("""
                SELECT id, key, version, size, checksum, storage_mode, replication_factor, created_at
                FROM objects WHERE key = ?
                ORDER BY version DESC LIMIT 1
            """, (key,)).fetchone()
        else:
            obj = cursor.execute("""
                SELECT id, key, version, size, checksum, storage_mode, replication_factor, created_at
                FROM objects WHERE key = ? AND version = ?
            """, (key, version)).fetchone()

        if not obj:
            return None

        actual_version = obj["version"]
        replicas = cursor.execute("""
            SELECT r.id, r.object_key, r.version, r.node_id, r.shard_index, r.checksum, r.status, r.last_verified, n.status as node_status
            FROM replicas r
            LEFT JOIN nodes n ON r.node_id = n.node_id
            WHERE r.object_key = ? AND r.version = ?
        """, (key, actual_version)).fetchall()

        reps_list = [dict(r) for r in replicas]
        
        # Calculate health status
        storage_mode = obj["storage_mode"]
        if storage_mode == "erasure_coding":
            # In EC mode, requires at least k healthy shards
            k = config.EC_DATA_SHARDS
            healthy_shards = [r for r in reps_list if r["status"] == config.REPLICA_STATUS_HEALTHY and r["node_status"] == config.NODE_STATUS_HEALTHY]
            corrupt_shards = [r for r in reps_list if r["status"] == config.REPLICA_STATUS_CORRUPTED]
            if len(healthy_shards) >= (k + config.EC_PARITY_SHARDS):
                health_status = "HEALTHY"
            elif len(healthy_shards) >= k:
                health_status = "DEGRADED"  # Still recoverable with k shards
            else:
                health_status = "CRITICAL"
        else:
            # In replication mode
            rf = obj["replication_factor"]
            healthy_reps = [r for r in reps_list if r["status"] == config.REPLICA_STATUS_HEALTHY and r["node_status"] == config.NODE_STATUS_HEALTHY]
            corrupted_reps = [r for r in reps_list if r["status"] == config.REPLICA_STATUS_CORRUPTED]
            if len(corrupted_reps) > 0:
                health_status = "CORRUPTED"
            elif len(healthy_reps) >= rf:
                health_status = "HEALTHY"
            elif len(healthy_reps) > 0:
                health_status = "UNDER_REPLICATED"
            else:
                health_status = "CRITICAL"

        result = dict(obj)
        result["replicas"] = reps_list
        result["health_status"] = health_status
        return result

def list_all_versions(key: str) -> List[Dict[str, Any]]:
    """Return metadata for all available versions of a given key."""
    with db_session() as conn:
        cursor = conn.cursor()
        versions = cursor.execute("""
            SELECT version FROM objects WHERE key = ? ORDER BY version DESC
        """, (key,)).fetchall()
        
        results = []
        for v in versions:
            meta = get_object_metadata(key, v["version"])
            if meta:
                results.append(meta)
        return results

def list_objects_summary() -> List[Dict[str, Any]]:
    """Return summary list of all distinct keys with their latest version info."""
    with db_session() as conn:
        cursor = conn.cursor()
        keys = cursor.execute("""
            SELECT DISTINCT key FROM objects ORDER BY key ASC
        """).fetchall()

        results = []
        for k in keys:
            key_name = k["key"]
            latest = get_object_metadata(key_name)
            if latest:
                total_versions = cursor.execute(
                    "SELECT COUNT(*) as c FROM objects WHERE key = ?", (key_name,)
                ).fetchone()["c"]
                results.append({
                    "key": key_name,
                    "latest_version": latest["version"],
                    "total_versions": total_versions,
                    "latest_size": latest["size"],
                    "latest_checksum": latest["checksum"],
                    "storage_mode": latest["storage_mode"],
                    "health_status": latest["health_status"],
                    "created_at": latest["created_at"],
                    "replicas": latest["replicas"]
                })
        return results

def prune_old_versions(key: str, max_versions: int = config.MAX_VERSIONS) -> List[Dict[str, Any]]:
    """
    Remove versions beyond max_versions limit.
    Returns list of replica records that must be physically deleted from disk.
    """
    stale_replicas = []
    with db_session() as conn:
        cursor = conn.cursor()
        versions = cursor.execute("""
            SELECT version FROM objects WHERE key = ? ORDER BY version DESC
        """, (key,)).fetchall()

        if len(versions) > max_versions:
            versions_to_prune = [v["version"] for v in versions[max_versions:]]
            for v in versions_to_prune:
                # Find all replicas for this pruned version
                reps = cursor.execute("""
                    SELECT node_id, object_key, version, shard_index FROM replicas 
                    WHERE object_key = ? AND version = ?
                """, (key, v)).fetchall()
                stale_replicas.extend([dict(r) for r in reps])

                # Delete from DB
                cursor.execute("DELETE FROM replicas WHERE object_key = ? AND version = ?", (key, v))
                cursor.execute("DELETE FROM objects WHERE key = ? AND version = ?", (key, v))

            log_event("VERSIONING", f"Pruned {len(versions_to_prune)} old version(s) for key '{key}'", conn=conn)

    return stale_replicas
