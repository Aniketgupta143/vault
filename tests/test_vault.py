"""
Comprehensive automated test suite for Vault Distributed Object Storage.
Tests Consistent Hashing, Versioning, Passive Read-Repair, Draining,
Orphan Sweeping, Erasure Coding, and Distributed Locking.
"""
import os
import time
import shutil
import pytest
import asyncio
from pathlib import Path
from httpx import AsyncClient, ASGITransport

import config
from gateway import app
from metadata_service import init_db, acquire_lock, release_lock, db_session
from hash_ring import ConsistentHashRing
import storage_node
import health_monitor
import repair_service
from erasure_coding import global_ec_coder

TEST_STORAGE_DIR = config.BASE_DIR / "test_vault_storage"
TEST_DB_PATH = config.BASE_DIR / "test_vault.db"

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup isolated test environment and teardown cleanly."""
    if TEST_STORAGE_DIR.exists():
        shutil.rmtree(TEST_STORAGE_DIR, ignore_errors=True)
    if TEST_DB_PATH.exists():
        try:
            TEST_DB_PATH.unlink()
        except Exception:
            pass

    config.STORAGE_DIR = TEST_STORAGE_DIR
    config.DB_PATH = TEST_DB_PATH
    config.AUTH_ENABLED = False

    init_db()

    yield

    if TEST_STORAGE_DIR.exists():
        shutil.rmtree(TEST_STORAGE_DIR, ignore_errors=True)
    if TEST_DB_PATH.exists():
        try:
            TEST_DB_PATH.unlink()
        except Exception:
            pass

@pytest.mark.asyncio
async def test_01_hash_ring_remapping():
    """Verify consistent hash ring distributes keys and minimizes remapping on topology change."""
    ring = ConsistentHashRing(100)
    for n in ["node1", "node2", "node3", "node4"]:
        ring.add_node(n)

    sample_keys = [f"key_{i}" for i in range(500)]
    analysis = ring.analyze_remapping(sample_keys, "node5")

    assert 0.10 <= analysis["fraction_remapped"] <= 0.35
    assert len(ring.get_nodes_for_key("sample_object", 3)) == 3

@pytest.mark.asyncio
async def test_02_dynamic_node_registration():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        reg_res = await client.post("/nodes", json={"node_id": "node7", "host": "127.0.0.1", "port": 8007})
        assert reg_res.status_code == 200
        assert reg_res.json()["node_id"] == "node7"

        nodes_res = await client.get("/nodes")
        assert nodes_res.status_code == 200
        nodes = nodes_res.json()
        assert any(n["node_id"] == "node7" for n in nodes)

@pytest.mark.asyncio
async def test_03_streaming_upload_and_versioning():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        content_v1 = b"Vault Versioning Test - Content V1"
        files1 = {"file": ("version_test.txt", content_v1, "text/plain")}
        res1 = await client.post("/objects", files=files1, data={"replication_factor": 3})
        assert res1.status_code == 200
        obj1 = res1.json()
        assert obj1["key"] == "version_test.txt"
        assert obj1["version"] == 1
        assert len(obj1["replicas"]) == 3

        content_v2 = b"Vault Versioning Test - Updated Content V2"
        files2 = {"file": ("version_test.txt", content_v2, "text/plain")}
        res2 = await client.post("/objects", files=files2, data={"replication_factor": 3})
        assert res2.status_code == 200
        obj2 = res2.json()
        assert obj2["key"] == "version_test.txt"
        assert obj2["version"] == 2

        dl1 = await client.get("/objects/version_test.txt/download?version=1")
        assert dl1.status_code == 200
        assert dl1.content == content_v1

        dl2 = await client.get("/objects/version_test.txt/download")
        assert dl2.status_code == 200
        assert dl2.content == content_v2

@pytest.mark.asyncio
async def test_04_passive_read_repair():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        content = b"Passive read-repair verification payload"
        files = {"file": ("read_repair_test.txt", content, "text/plain")}
        res = await client.post("/objects", files=files)
        assert res.status_code == 200
        obj = res.json()

        corrupt_res = await client.post(f"/objects/{obj['key']}/corrupt")
        assert corrupt_res.status_code == 200
        corrupt_node = corrupt_res.json()["node_id"]

        dl = await client.get(f"/objects/{obj['key']}/download")
        assert dl.status_code == 200
        assert dl.content == content

        await asyncio.sleep(1.0)
        meta = (await client.get(f"/objects/{obj['key']}")).json()
        target_rep = next(r for r in meta["replicas"] if r["node_id"] == corrupt_node)
        assert target_rep["status"] == "HEALTHY"

@pytest.mark.asyncio
async def test_05_node_draining_and_removal():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        drain_res = await client.post("/nodes/node7/drain")
        assert drain_res.status_code == 200
        assert drain_res.json()["status"] == config.NODE_STATUS_SAFE_TO_REMOVE

        del_res = await client.delete("/nodes/node7")
        assert del_res.status_code == 200
        assert del_res.json()["status"] == "removed"

@pytest.mark.asyncio
async def test_06_orphan_sweep_on_recovery():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        storage_node.ensure_node_dir("node3")
        orphan_path = config.STORAGE_DIR / "node3" / "orphan_ghost.v1.shard0"
        orphan_path.write_bytes(b"I am a stale orphaned shard!")

        assert orphan_path.exists()

        rec_res = await client.post("/nodes/node3/recover")
        assert rec_res.status_code == 200
        assert rec_res.json()["orphans_purged"] >= 1
        assert not orphan_path.exists()

@pytest.mark.asyncio
async def test_07_distributed_locking_ttl():
    acquired1 = acquire_lock("test_key", "operation_A", ttl_seconds=2)
    assert acquired1 is True

    acquired2 = acquire_lock("test_key", "operation_B", ttl_seconds=2)
    assert acquired2 is False

    release_lock("test_key")
    acquired3 = acquire_lock("test_key", "operation_B", ttl_seconds=2)
    assert acquired3 is True
    release_lock("test_key")

@pytest.mark.asyncio
async def test_08_erasure_coding_resilience():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = b"Erasure coding test block! Survives node losses with Reed-Solomon math."
        files = {"file": ("ec_test_doc.bin", payload, "application/octet-stream")}
        res = await client.post("/objects", files=files, data={"storage_mode": "erasure_coding"})
        assert res.status_code == 200
        ec_obj = res.json()
        assert ec_obj["storage_mode"] == "erasure_coding"
        assert len(ec_obj["replicas"]) == 6

        failed_node_a = ec_obj["replicas"][0]["node_id"]
        failed_node_b = ec_obj["replicas"][1]["node_id"]

        await client.post(f"/nodes/{failed_node_a}/fail")
        await client.post(f"/nodes/{failed_node_b}/fail")

        dl = await client.get("/objects/ec_test_doc.bin/download")
        assert dl.status_code == 200
        assert dl.content == payload

        await client.post(f"/nodes/{failed_node_a}/recover")
        await client.post(f"/nodes/{failed_node_b}/recover")

@pytest.mark.asyncio
async def test_09_metrics_and_events():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        metrics = (await client.get("/metrics")).json()
        assert metrics["total_keys"] >= 1
        assert metrics["total_nodes"] >= 4
        assert metrics["storage_overhead"] >= 1.0

        events = (await client.get("/events?limit=10")).json()
        assert len(events) >= 1
