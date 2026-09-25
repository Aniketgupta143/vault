"""Pydantic data models for Vault Distributed Object Storage."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

class ReplicaModel(BaseModel):
    id: Optional[int] = None
    object_id: str
    node_id: str
    version: int = 1
    checksum: str
    status: str = "HEALTHY"
    last_verified: float

class ObjectMetadata(BaseModel):
    object_id: str
    filename: str
    size: int
    checksum: str
    version: int = 1
    replication_factor: int = 3
    created_at: float
    updated_at: float
    replicas: List[ReplicaModel] = Field(default_factory=list)
    health_status: str = "HEALTHY"  # HEALTHY, UNDER_REPLICATED, CORRUPTED, CRITICAL

class NodeModel(BaseModel):
    node_id: str
    status: str
    capacity: int
    used_storage: int = 0
    last_heartbeat: float
    address: Optional[str] = None
    active_replicas_count: int = 0

class NodeStatusUpdateRequest(BaseModel):
    status: str

class RepairJobModel(BaseModel):
    id: Optional[int] = None
    object_id: str
    source_node: str
    target_node: str
    status: str = "PENDING"
    duration_ms: float = 0.0
    error: Optional[str] = None
    created_at: float
    completed_at: Optional[float] = None

class EventModel(BaseModel):
    id: Optional[int] = None
    level: str
    category: str
    message: str
    details: Optional[str] = None
    timestamp: float

class MetricsSummary(BaseModel):
    system_status: str  # HEALTHY, DEGRADED, CRITICAL
    total_objects: int
    logical_bytes: int
    physical_bytes: int
    storage_overhead: float
    healthy_nodes: int
    failed_nodes: int
    disconnected_nodes: int
    total_replicas: int
    healthy_replicas: int
    corrupted_replicas: int
    under_replicated_objects: int
    completed_repairs: int
    failed_repairs: int
    avg_repair_duration_ms: float
    upload_latency_ms: float
    download_latency_ms: float
