"""Automated test suite for Vault distributed object storage."""
import os
import time
import shutil
import pytest
import asyncio
from pathlib import Path
from httpx import AsyncClient, ASGITransport

from app import app
from database import init_db, db_session
import storage
import config

TEST_STORAGE_DIR = config.BASE_DIR / "test_storage"
TEST_DB_PATH = config.BASE_DIR / "test_vault.db"

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Configure isolated test environment."""
    if TEST_STORAGE_DIR.exists():
        shutil.rmtree(TEST_STORAGE_DIR, ignore_errors=True)
    if TEST_DB_PATH.exists():
        try:
            TEST_DB_PATH.unlink()
        except Exception:
            pass

    config.STORAGE_DIR = TEST_STORAGE_DIR
    config.DB_PATH = TEST_DB_PATH
    storage.ensure_storage_nodes()
    init_db()

    yield

    # Teardown
    if TEST_STORAGE_DIR.exists():
        shutil.rmtree(TEST_STORAGE_DIR, ignore_errors=True)
    if TEST_DB_PATH.exists():
        try:
            TEST_DB_PATH.unlink()
        except Exception:
            pass

@pytest.mark.asyncio
async def test_01_health_and_nodes_init():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/nodes")
        assert res.status_code == 200
        nodes = res.json()
        assert len(nodes) == 4
        for n in nodes:
            assert n["status"] == "HEALTHY"

@pytest.mark.asyncio
async def test_02_upload_and_replication():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        content = b"Vault distributed storage hackathon test content 12345!"
        files = {"file": ("demo.txt", content, "text/plain")}
        data = {"replication_factor": 3}

        res = await client.post("/objects", files=files, data=data)
        assert res.status_code == 200
        obj = res.json()

        assert obj["filename"] == "demo.txt"
        assert obj["size"] == len(content)
        assert len(obj["checksum"]) == 64
        assert len(obj["replicas"]) == 3
        assert obj["health_status"] == "HEALTHY"

        # Verify physical files exist on the 3 chosen nodes
        object_id = obj["object_id"]
        for r in obj["replicas"]:
            p = storage.get_replica_path(r["node_id"], object_id)
            assert p.exists()
            assert p.read_bytes() == content

@pytest.mark.asyncio
async def test_03_download_object():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Get object list
        res = await client.get("/objects")
        assert res.status_code == 200
        objs = res.json()
        assert len(objs) >= 1
        obj = objs[0]

        # Download
        dl_res = await client.get(f"/objects/{obj['object_id']}/download")
        assert dl_res.status_code == 200
        assert dl_res.content == b"Vault distributed storage hackathon test content 12345!"
        assert dl_res.headers["X-Vault-Checksum"] == obj["checksum"]

@pytest.mark.asyncio
async def test_04_node_failover_download():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        objs = (await client.get("/objects")).json()
        obj = objs[0]
        first_node = obj["replicas"][0]["node_id"]

        # Fail the first node
        fail_res = await client.post(f"/nodes/{first_node}/fail")
        assert fail_res.status_code == 200

        # Download should still succeed via failover to replica 2 or 3
        dl_res = await client.get(f"/objects/{obj['object_id']}/download")
        assert dl_res.status_code == 200
        assert dl_res.content == b"Vault distributed storage hackathon test content 12345!"

@pytest.mark.asyncio
async def test_05_auto_repair_after_node_failure():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        objs = (await client.get("/objects")).json()
        obj = objs[0]

        # Trigger repair for the under-replicated object
        repair_res = await client.post(f"/objects/{obj['object_id']}/repair")
        assert repair_res.status_code == 200
        data = repair_res.json()
        assert data["status"] == "repaired"

        # Check metadata shows 3 healthy replicas again on active nodes
        updated_meta = (await client.get(f"/objects/{obj['object_id']}")).json()
        healthy_reps = [r for r in updated_meta["replicas"] if r["status"] == "HEALTHY" and r["node_status"] == "HEALTHY"]
        assert len(healthy_reps) == 3

@pytest.mark.asyncio
async def test_06_corruption_detection_and_repair():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        objs = (await client.get("/objects")).json()
        obj = objs[0]

        # 1. Simulate byte corruption
        corrupt_res = await client.post(f"/objects/{obj['object_id']}/corrupt")
        assert corrupt_res.status_code == 200
        corrupted_node = corrupt_res.json()["corrupted_node"]

        # 2. Verify integrity detects corruption
        verify_res = await client.post(f"/objects/{obj['object_id']}/verify")
        assert verify_res.status_code == 200
        verify_data = verify_res.json()
        assert verify_data["corrupted_count"] >= 1

        # 3. Heal corrupted replica
        repair_res = await client.post(f"/objects/{obj['object_id']}/repair")
        assert repair_res.status_code == 200
        assert repair_res.json()["status"] == "repaired"

        # 4. Verify integrity again - should be fully healthy now
        post_verify = (await client.post(f"/objects/{obj['object_id']}/verify")).json()
        assert post_verify["corrupted_count"] == 0
        assert post_verify["healthy_count"] == 3

@pytest.mark.asyncio
async def test_07_node_recovery():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Recover all failed nodes
        nodes = (await client.get("/nodes")).json()
        for n in nodes:
            if n["status"] != "HEALTHY":
                rec_res = await client.post(f"/nodes/{n['node_id']}/recover")
                assert rec_res.status_code == 200

        # All nodes should be healthy now
        all_nodes = (await client.get("/nodes")).json()
        assert all(n["status"] == "HEALTHY" for n in all_nodes)

@pytest.mark.asyncio
async def test_08_concurrent_uploads():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async def upload_file(idx: int):
            content = f"Concurrent test payload #{idx} {time.time()}".encode()
            files = {"file": (f"concurrent_{idx}.txt", content, "text/plain")}
            return await client.post("/objects", files=files, data={"replication_factor": 3})

        tasks = [upload_file(i) for i in range(5)]
        results = await asyncio.gather(*tasks)

        for res in results:
            assert res.status_code == 200
            assert res.json()["health_status"] == "HEALTHY"

@pytest.mark.asyncio
async def test_09_rebalance_cluster():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        reb_res = await client.post("/rebalance")
        assert reb_res.status_code == 200
        data = reb_res.json()
        assert data["status"] in ("success", "balanced")

@pytest.mark.asyncio
async def test_10_metrics_and_events():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        metrics = (await client.get("/metrics")).json()
        assert metrics["total_objects"] >= 6
        assert metrics["completed_repairs"] >= 2
        assert metrics["storage_overhead"] >= 1.0

        events = (await client.get("/events?limit=10")).json()
        assert len(events) >= 5

@pytest.mark.asyncio
async def test_11_delete_object():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        objs = (await client.get("/objects")).json()
        target_obj = objs[0]
        obj_id = target_obj["object_id"]

        del_res = await client.delete(f"/objects/{obj_id}")
        assert del_res.status_code == 200

        # Assert 404 on get
        get_res = await client.get(f"/objects/{obj_id}")
        assert get_res.status_code == 404

@pytest.mark.asyncio
async def test_12_network_partition_simulation():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Simulate network disconnect on node4
        disc_res = await client.post("/nodes/node4/disconnect")
        assert disc_res.status_code == 200
        assert disc_res.json()["new_status"] == "DISCONNECTED"

        # Check nodes list reports DISCONNECTED
        nodes = (await client.get("/nodes")).json()
        node4 = next(n for n in nodes if n["node_id"] == "node4")
        assert node4["status"] == "DISCONNECTED"

        # Reconnect node4
        rec_res = await client.post("/nodes/node4/recover")
        assert rec_res.status_code == 200
        assert rec_res.json()["new_status"] == "HEALTHY"

@pytest.mark.asyncio
async def test_13_quorum_enforcement():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Fail 3 out of 4 nodes so only 1 healthy node remains
        await client.post("/nodes/node1/fail")
        await client.post("/nodes/node2/fail")
        await client.post("/nodes/node3/fail")

        try:
            # Attempt to upload with RF=3 (quorum=2, but only 1 healthy node available)
            files = {"file": ("quorum_test.txt", b"Should fail quorum", "text/plain")}
            res = await client.post("/objects", files=files, data={"replication_factor": 3})
            assert res.status_code == 500  # Quorum not met
        finally:
            # Recover nodes
            await client.post("/nodes/node1/recover")
            await client.post("/nodes/node2/recover")
            await client.post("/nodes/node3/recover")

@pytest.mark.asyncio
async def test_14_idempotent_repair():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        objs = (await client.get("/objects")).json()
        healthy_obj = next(o for o in objs if o["health_status"] == "HEALTHY")

        # Calling repair on an already healthy object should safely succeed and return 'already_healthy'
        repair_res = await client.post(f"/objects/{healthy_obj['object_id']}/repair")
        assert repair_res.status_code == 200
        assert repair_res.json()["status"] == "already_healthy"
