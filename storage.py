"""Physical storage manager for logical Vault storage nodes."""
import hashlib
import os
import shutil
from pathlib import Path
from typing import Tuple, List, Optional
import logging

import config
from config import INITIAL_NODES

logger = logging.getLogger("vault.storage")

def ensure_storage_nodes(node_ids: Optional[List[str]] = None) -> None:
    """Ensure all node storage directories exist."""
    nodes = node_ids or INITIAL_NODES
    config.STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    for node_id in nodes:
        node_dir = config.STORAGE_DIR / node_id
        node_dir.mkdir(parents=True, exist_ok=True)

def get_node_path(node_id: str) -> Path:
    """Return directory path for a specific node."""
    return config.STORAGE_DIR / node_id

def get_replica_path(node_id: str, object_id: str) -> Path:
    """Return file path for an object replica on a specific node."""
    return config.STORAGE_DIR / node_id / object_id

def calculate_checksum(data: bytes) -> str:
    """Calculate SHA-256 checksum of raw bytes."""
    return hashlib.sha256(data).hexdigest()

def calculate_file_checksum(file_path: Path) -> str:
    """Calculate SHA-256 checksum of an existing file on disk."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()

def write_replica(node_id: str, object_id: str, data: bytes) -> Tuple[int, str]:
    """
    Safely write object data to a storage node.
    Uses atomic write via temp file to avoid partial/corrupted writes.
    Returns (bytes_written, sha256_checksum).
    """
    node_dir = get_node_path(node_id)
    node_dir.mkdir(parents=True, exist_ok=True)

    target_path = get_replica_path(node_id, object_id)
    temp_path = node_dir / f".tmp_{object_id}"

    # Atomic write pattern
    with open(temp_path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())

    # Atomically replace
    temp_path.replace(target_path)

    # Verify write
    actual_checksum = calculate_file_checksum(target_path)
    file_size = target_path.stat().st_size

    return file_size, actual_checksum

def read_replica(node_id: str, object_id: str) -> bytes:
    """Read raw replica bytes from a storage node."""
    target_path = get_replica_path(node_id, object_id)
    if not target_path.exists():
        raise FileNotFoundError(f"Replica {object_id} not found on node {node_id}")
    with open(target_path, "rb") as f:
        return f.read()

def delete_replica(node_id: str, object_id: str) -> bool:
    """Delete a replica file from a storage node."""
    target_path = get_replica_path(node_id, object_id)
    if target_path.exists():
        target_path.unlink()
        return True
    return False

def corrupt_replica(node_id: str, object_id: str) -> bool:
    """
    Intentionally corrupt a replica file for hackathon demo.
    Flips bytes to simulate silent disk corruption.
    """
    target_path = get_replica_path(node_id, object_id)
    if not target_path.exists():
        return False

    with open(target_path, "r+b") as f:
        content = bytearray(f.read())
        if len(content) > 0:
            # Corrupt the first byte and append garbage
            content[0] = (content[0] ^ 0xFF)
            content.extend(b"__CORRUPTED_BY_DEMO__")
        else:
            content.extend(b"__CORRUPTED_EMPTY_OBJECT__")
        f.seek(0)
        f.write(content)
        f.truncate()
        f.flush()
        os.fsync(f.fileno())

    return True

def get_node_used_bytes(node_id: str) -> int:
    """Calculate total stored bytes on a specific node."""
    node_dir = get_node_path(node_id)
    if not node_dir.exists():
        return 0
    total = 0
    for entry in node_dir.iterdir():
        if entry.is_file() and not entry.name.startswith("."):
            total += entry.stat().st_size
    return total
