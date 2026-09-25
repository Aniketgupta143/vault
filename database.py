"""Database layer for Vault using SQLite with WAL mode."""
import sqlite3
import time
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

import config
from config import (
    INITIAL_NODES,
    DEFAULT_NODE_CAPACITY_BYTES,
    NODE_STATUS_HEALTHY,
)

logger = logging.getLogger("vault.database")

def get_connection() -> sqlite3.Connection:
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
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    """Initialize database tables, indexes, and initial nodes."""
    with db_session() as conn:
        cursor = conn.cursor()

        # Nodes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'HEALTHY',
                capacity INTEGER NOT NULL,
                used_storage INTEGER NOT NULL DEFAULT 0,
                last_heartbeat REAL NOT NULL,
                address TEXT
            );
        """)

        # Objects table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS objects (
                object_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                replication_factor INTEGER NOT NULL DEFAULT 3,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
        """)

        # Replicas table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS replicas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                checksum TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'HEALTHY',
                last_verified REAL NOT NULL,
                FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
                FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE,
                UNIQUE (object_id, node_id)
            );
        """)

        # Repair jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS repair_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_id TEXT NOT NULL,
                source_node TEXT NOT NULL,
                target_node TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                duration_ms REAL DEFAULT 0,
                error TEXT,
                created_at REAL NOT NULL,
                completed_at REAL
            );
        """)

        # System Events / Audit Log table
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

        # Indexes for fast lookup
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_replicas_object_id ON replicas(object_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_replicas_node_id ON replicas(node_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_replicas_status ON replicas(status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_repair_jobs_status ON repair_jobs(status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp DESC);")

        # Seed initial nodes if not present
        now = time.time()
        for node_id in INITIAL_NODES:
            cursor.execute("""
                INSERT OR IGNORE INTO nodes (node_id, status, capacity, used_storage, last_heartbeat, address)
                VALUES (?, ?, ?, 0, ?, ?)
            """, (node_id, NODE_STATUS_HEALTHY, DEFAULT_NODE_CAPACITY_BYTES, now, f"local://{node_id}"))

        logger.info("Vault SQLite database initialized successfully.")

def log_event(category: str, message: str, details: Optional[str] = None, level: str = "INFO", conn: Optional[sqlite3.Connection] = None):
    """Log an event into the SQLite events table and stdout."""
    now = time.time()
    formatted = f"[{category}] {message}"
    if level == "ERROR":
        logger.error(formatted)
    elif level == "WARNING":
        logger.warning(formatted)
    else:
        logger.info(formatted)

    def _insert(c: sqlite3.Connection):
        c.execute("""
            INSERT INTO events (level, category, message, details, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (level, category, message, details, now))

    if conn is not None:
        _insert(conn)
    else:
        try:
            with db_session() as session:
                _insert(session)
        except Exception as e:
            logger.error(f"Failed to record event log to DB: {e}")
