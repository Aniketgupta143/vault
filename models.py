"""
Pydantic data models and schemas for Vault Distributed Object Storage.
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class NodeRegistrationRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int
    node_id: Optional[str] = None
    capacity: Optional[int] = None

class NodeModel(BaseModel):
    node_id: str
    host: str = "127.0.0.1"
    port: int
    status: str
    capacity: int
    used_storage: int = 0
    last_heartbeat: float
    active_replicas_count: int = 0

class ReplicaModel(BaseModel):
    id: Optional[int] = None
    object_key: str
    version: int = 1
    node_id: str
    shard_index: int = 0  # 0 for whole replica, 0..k+m-1 for EC shards
    checksum: str
    status: str = "HEALTHY"
    last_verified: float
    node_status: Optional[str] = None

class ObjectVersionMetadata(BaseModel):
    key: str
    version: int = 1
    size: int
    checksum: str
    storage_mode: str = "replication"
    replication_factor: int = 3
    created_at: float
    replicas: List[ReplicaModel] = Field(default_factory=list)
    health_status: str = "HEALTHY"  # HEALTHY, UNDER_REPLICATED, CORRUPTED, CRITICAL

class ObjectSummary(BaseModel):
    key: str
    latest_version: int
    total_versions: int
    latest_size: int
    latest_checksum: str
    storage_mode: str
    health_status: str
    updated_at: float

class LockAcquisitionRequest(BaseModel):
    operation: str
    owner: str = "gateway"
    ttl_seconds: int = 10

class EventModel(BaseModel):
    id: Optional[int] = None
    level: str
    category: str
    message: str
    details: Optional[str] = None
    timestamp: float

class ClusterMetrics(BaseModel):
    system_status: str  # HEALTHY, DEGRADED, CRITICAL
    storage_mode: str
    total_keys: int
    total_versions: int
    logical_bytes: int
    physical_bytes: int
    storage_overhead: float
    total_nodes: int
    healthy_nodes: int
    draining_nodes: int
    failed_nodes: int
    total_replicas: int
    healthy_replicas: int
    corrupted_replicas: int
    under_replicated_objects: int
    completed_repairs: int
    failed_repairs: int
    avg_repair_duration_ms: float
    upload_latency_ms: float
    download_latency_ms: float
