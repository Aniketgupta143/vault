"""
Configuration settings for the Vault Distributed Object Storage.
Single source of truth for ports, replication, timeouts, storage modes, and feature flags.
Every other module imports from here.
"""
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
DB_PATH = BASE_DIR / "vault.db"
STATIC_DIR = BASE_DIR / "static"

# Network & Server
HOST = "0.0.0.0"
PORT = 8000
GATEWAY_URL = f"http://127.0.0.1:{PORT}"

# Initial / Bootstrap Nodes (Seed for dynamic node membership table)
# 6 nodes by default so Erasure Coding (k=4, m=2) and RF=3 works immediately
DEFAULT_NODES = [
    {"node_id": "node1", "host": "127.0.0.1", "port": 8001},
    {"node_id": "node2", "host": "127.0.0.1", "port": 8002},
    {"node_id": "node3", "host": "127.0.0.1", "port": 8003},
    {"node_id": "node4", "host": "127.0.0.1", "port": 8004},
    {"node_id": "node5", "host": "127.0.0.1", "port": 8005},
    {"node_id": "node6", "host": "127.0.0.1", "port": 8006},
]
DEFAULT_NODE_CAPACITY_BYTES = 500 * 1024 * 1024  # 500 MB per node

# Replication Settings
DEFAULT_REPLICATION_FACTOR = 3
WRITE_QUORUM = 2

# Streaming & I/O
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB streaming chunks (never buffer whole files)

# Consistent Hash Ring
VIRTUAL_NODES_PER_PHYSICAL = 150  # 150 virtual nodes per physical node for balanced distribution

# Storage Engine Mode: "replication" | "erasure_coding"
STORAGE_MODE = os.getenv("VAULT_STORAGE_MODE", "replication")
EC_DATA_SHARDS = 4     # k data shards
EC_PARITY_SHARDS = 2   # m parity shards (tolerates 2 node losses with only 1.5x overhead)

# Object Versioning
MAX_VERSIONS = 5

# Distributed Row-Level Locks
LOCK_TTL_SECONDS = 10

# Health Monitor & Timeouts (seconds)
HEARTBEAT_TIMEOUT_SECONDS = 12
HEALTH_CHECK_INTERVAL_SECONDS = 3
AUTO_REPAIR_INTERVAL_SECONDS = 3
AUTO_REPAIR_ENABLED = True

# Security & Encryption
AUTH_ENABLED = False  # Set False by default for seamless UI demo; Bearer middleware active when True
DEFAULT_API_KEY = os.getenv("VAULT_API_KEY", "vault-secret-token-2026")
ENCRYPTION_AT_REST = True
MASTER_ENCRYPTION_KEY = os.getenv("VAULT_MASTER_KEY", "vault-aes256-master-key-32b!!")

# Raft Feature Flag
USE_RAFT_METADATA = os.getenv("VAULT_USE_RAFT", "false").lower() in ("true", "1", "yes")

# Node Status Constants
NODE_STATUS_HEALTHY = "HEALTHY"
NODE_STATUS_DRAINING = "DRAINING"
NODE_STATUS_SAFE_TO_REMOVE = "SAFE_TO_REMOVE"
NODE_STATUS_FAILED = "FAILED"
NODE_STATUS_DISCONNECTED = "DISCONNECTED"

# Replica Status Constants
REPLICA_STATUS_HEALTHY = "HEALTHY"
REPLICA_STATUS_CORRUPTED = "CORRUPTED"
REPLICA_STATUS_MISSING = "MISSING"

# Repair Job Status Constants
REPAIR_STATUS_PENDING = "PENDING"
REPAIR_STATUS_IN_PROGRESS = "IN_PROGRESS"
REPAIR_STATUS_COMPLETED = "COMPLETED"
REPAIR_STATUS_FAILED = "FAILED"
