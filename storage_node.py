"""
Storage Node: Physical disk I/O, streaming chunked reads/writes,
crash-consistent atomic writes, and optional AES-256-GCM encryption at rest.
"""
import os
import shutil
import hashlib
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional, AsyncGenerator
import logging
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import config

logger = logging.getLogger("vault.storage_node")

VAULT_MAGIC_ENCRYPTED = b"VAULT_ENC_V1\x00"

def _get_aesgcm() -> AESGCM:
    """Derive 32-byte AES-GCM key from master key in config."""
    key = hashlib.sha256(config.MASTER_ENCRYPTION_KEY.encode("utf-8")).digest()
    return AESGCM(key)

def ensure_node_dir(node_id: str) -> Path:
    """Ensure physical directory for a specific storage node exists."""
    node_dir = config.STORAGE_DIR / node_id
    node_dir.mkdir(parents=True, exist_ok=True)
    return node_dir

def get_shard_filename(key: str, version: int, shard_index: int = 0) -> str:
    """Deterministic physical filename for a replica or EC shard."""
    safe_key = "".join(c if c.isalnum() or c in "._-" else "_" for c in key)
    return f"{safe_key}.v{version}.shard{shard_index}"

def get_replica_path(node_id: str, key: str, version: int, shard_index: int = 0) -> Path:
    """Return absolute path for a shard file on a specific storage node."""
    return ensure_node_dir(node_id) / get_shard_filename(key, version, shard_index)

def calculate_checksum(data: bytes) -> str:
    """Compute SHA-256 checksum of raw byte content."""
    return hashlib.sha256(data).hexdigest()

def calculate_file_checksum(file_path: Path) -> str:
    """
    Compute SHA-256 checksum of the plain data of a stored file.
    Transparently decrypts if encrypted at rest.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    raw_bytes = file_path.read_bytes()
    if raw_bytes.startswith(VAULT_MAGIC_ENCRYPTED):
        nonce = raw_bytes[len(VAULT_MAGIC_ENCRYPTED):len(VAULT_MAGIC_ENCRYPTED)+12]
        ciphertext = raw_bytes[len(VAULT_MAGIC_ENCRYPTED)+12:]
        aesgcm = _get_aesgcm()
        decrypted = aesgcm.decrypt(nonce, ciphertext, None)
        return calculate_checksum(decrypted)
    else:
        return calculate_checksum(raw_bytes)

def write_shard_atomic(
    node_id: str,
    key: str,
    version: int,
    shard_index: int,
    data: bytes
) -> Tuple[int, str]:
    """
    Safely write shard data to a storage node using atomic write pattern:
    Write temp -> fsync -> atomic replace.
    Transparently applies AES-256-GCM encryption if config.ENCRYPTION_AT_REST is True.
    Returns (logical_bytes_written, sha256_checksum).
    """
    node_dir = ensure_node_dir(node_id)
    target_path = get_replica_path(node_id, key, version, shard_index)
    temp_path = node_dir / f".tmp_{get_shard_filename(key, version, shard_index)}"

    checksum = calculate_checksum(data)
    logical_size = len(data)

    payload_to_write = data
    if config.ENCRYPTION_AT_REST:
        aesgcm = _get_aesgcm()
        nonce = os.urandom(12)
        encrypted_data = aesgcm.encrypt(nonce, data, None)
        payload_to_write = VAULT_MAGIC_ENCRYPTED + nonce + encrypted_data

    # Write & fsync
    with open(temp_path, "wb") as f:
        f.write(payload_to_write)
        f.flush()
        os.fsync(f.fileno())

    # Atomically move to target path
    temp_path.replace(target_path)
    return logical_size, checksum

def read_shard(node_id: str, key: str, version: int, shard_index: int = 0) -> bytes:
    """Read and decrypt shard data from disk."""
    target_path = get_replica_path(node_id, key, version, shard_index)
    if not target_path.exists():
        raise FileNotFoundError(f"Shard {get_shard_filename(key, version, shard_index)} not found on {node_id}")

    raw_bytes = target_path.read_bytes()
    if raw_bytes.startswith(VAULT_MAGIC_ENCRYPTED):
        nonce = raw_bytes[len(VAULT_MAGIC_ENCRYPTED):len(VAULT_MAGIC_ENCRYPTED)+12]
        ciphertext = raw_bytes[len(VAULT_MAGIC_ENCRYPTED)+12:]
        aesgcm = _get_aesgcm()
        return aesgcm.decrypt(nonce, ciphertext, None)
    return raw_bytes

def delete_shard(node_id: str, key: str, version: int, shard_index: int = 0) -> bool:
    """Delete a physical shard file from disk."""
    target_path = get_replica_path(node_id, key, version, shard_index)
    if target_path.exists():
        target_path.unlink()
        return True
    return False

def corrupt_replica(node_id: str, key: str, version: int, shard_index: int = 0) -> bool:
    """
    Intentionally corrupt a replica file on disk to simulate bit-rot or bad sectors.
    Used for hackathon self-healing demonstration.
    """
    target_path = get_replica_path(node_id, key, version, shard_index)
    if not target_path.exists():
        return False

    raw = bytearray(target_path.read_bytes())
    if len(raw) > 0:
        mid = len(raw) // 2
        raw[mid] = raw[mid] ^ 0xFF
        target_path.write_bytes(raw)
        return True
    return False

def get_node_used_bytes(node_id: str) -> int:
    """Calculate total physical storage consumption for a node."""
    node_dir = config.STORAGE_DIR / node_id
    if not node_dir.exists():
        return 0
    return sum(f.stat().st_size for f in node_dir.iterdir() if f.is_file() and not f.name.startswith("."))

def list_node_local_files(node_id: str) -> List[str]:
    """List all non-temporary shard filenames present on a node's physical directory."""
    node_dir = config.STORAGE_DIR / node_id
    if not node_dir.exists():
        return []
    return [f.name for f in node_dir.iterdir() if f.is_file() and not f.name.startswith(".")]
