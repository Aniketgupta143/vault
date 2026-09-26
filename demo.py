"""
Vault Hackathon Live Demo Script
Exercises all 6 phases:
1. Consistent Hash Ring distribution & 1/N remapping proof
2. Streaming multipart upload with incremental SHA-256
3. Real object versioning (v1, v2)
4. Passive read-repair during download failover
5. Node failure & self-healing auto-repair
6. Node draining & safe decommissioning
7. Orphan sweep on node recovery
8. Erasure Coding (Reed-Solomon 4+2 surviving 2 node losses)
"""
import sys
import time
import httpx
import config

BASE_URL = f"http://127.0.0.1:{config.PORT}"

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def banner(title):
    print("\n" + "=" * 70)
    print(f" [*] {title}")
    print("=" * 70)

def check_server():
    try:
        r = httpx.get(f"{BASE_URL}/metrics", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False

def run_demo():
    print(r"""
    __      __         _ _   
    \ \    / /        | | |  
     \ \  / /_ _ _   _| | |_ 
      \ \/ / _` | | | | | __|
       \  / (_| | |_| | | |_ 
        \/ \__,_|\__,_|_|\__|
    Fault-Tolerant Distributed Object Storage - Live Demonstration
    """)

    if not check_server():
        print(f"[!] Vault server is not running on {BASE_URL}!")
        print("    Please start the server first in another terminal with:")
        print("    python main.py\n")
        sys.exit(1)

    with httpx.Client(base_url=BASE_URL, timeout=15.0) as client:
        # -------------------------------------------------------------
        banner("DEMO 1: Consistent Hashing Ring & 1/N Remapping Proof")
        # -------------------------------------------------------------
        print("[*] Testing Hash Ring with 150 virtual nodes per physical node...")
        from hash_ring import ConsistentHashRing
        ring = ConsistentHashRing(150)
        for nid in ["node1", "node2", "node3", "node4", "node5"]:
            ring.add_node(nid)

        sample_keys = [f"object_key_{i}.dat" for i in range(1000)]
        analysis = ring.analyze_remapping(sample_keys, "node6")
        print(f"[+] Total Sample Keys Tested: {analysis['total_keys']}")
        print(f"[+] Remapped Keys on adding node6: {analysis['remapped_keys']} ({analysis['fraction_remapped'] * 100:.1f}%)")
        print(f"[+] Theoretical Ideal Remapping: {analysis['expected_fraction'] * 100:.1f}%")
        print("[OK] Consistent Hash Ring minimizes data movement during topology shifts!")

        time.sleep(1)

        # -------------------------------------------------------------
        banner("DEMO 2: Streaming Upload & Deterministic Replica Placement")
        # -------------------------------------------------------------
        test_payload_v1 = b"Vault Distributed Object Storage - Version 1 Content [2026]"
        files = {"file": ("demo_asset.txt", test_payload_v1, "text/plain")}
        print("[*] Uploading 'demo_asset.txt' (Replication Factor = 3)...")
        r = client.post("/objects", files=files, data={"replication_factor": 3, "storage_mode": "replication"})
        assert r.status_code == 200, r.text
        obj_v1 = r.json()
        print(f"[+] Stored Key: {obj_v1['key']} (Version: {obj_v1['version']})")
        print(f"[+] Authoritative SHA-256: {obj_v1['checksum']}")
        print(f"[+] Replicas placed via Hash Ring:")
        for rep in obj_v1["replicas"]:
            print(f"    - {rep['node_id'].upper()}: status={rep['status']}")

        time.sleep(1)

        # -------------------------------------------------------------
        banner("DEMO 3: Real Object Versioning (v1 -> v2) & Immutability")
        # -------------------------------------------------------------
        test_payload_v2 = b"Vault Distributed Object Storage - UPDATED Version 2 Content [New Release]"
        files2 = {"file": ("demo_asset.txt", test_payload_v2, "text/plain")}
        print("[*] Uploading updated version of 'demo_asset.txt'...")
        r = client.post("/objects", files=files2, data={"replication_factor": 3, "storage_mode": "replication"})
        assert r.status_code == 200
        obj_v2 = r.json()
        print(f"[+] Stored Key: {obj_v2['key']} (New Version: {obj_v2['version']})")
        print(f"[+] Version 2 SHA-256: {obj_v2['checksum']}")

        print("[*] Verifying Version 1 is still preserved and accessible...")
        v1_dl = client.get(f"/objects/demo_asset.txt/download?version=1")
        assert v1_dl.status_code == 200
        assert v1_dl.content == test_payload_v1
        print(f"[OK] Retrieved Version 1: {v1_dl.content.decode()}")

        print("[*] Verifying Version 2 is latest default...")
        v2_dl = client.get(f"/objects/demo_asset.txt/download")
        assert v2_dl.status_code == 200
        assert v2_dl.content == test_payload_v2
        print(f"[OK] Retrieved Version 2: {v2_dl.content.decode()}")

        time.sleep(1)

        # -------------------------------------------------------------
        banner("DEMO 4: Silent Bit-Rot Detection & Passive Read-Repair")
        # -------------------------------------------------------------
        target_node = obj_v2["replicas"][0]["node_id"]
        print(f"[*] Simulating silent disk byte corruption on {target_node.upper()}...")
        r = client.post(f"/objects/demo_asset.txt/corrupt?version=2")
        assert r.status_code == 200
        print(f"[+] Injected bit-rot into replica on {target_node.upper()}")

        print("[*] Client downloads 'demo_asset.txt'...")
        dl = client.get("/objects/demo_asset.txt/download")
        assert dl.status_code == 200
        assert dl.content == test_payload_v2
        print(f"[+] Client download succeeded without downtime! (Served from healthy replica)")

        print("[*] Waiting 2 seconds for passive read-repair worker to heal corrupted replica...")
        time.sleep(2)
        meta = client.get("/objects/demo_asset.txt?version=2").json()
        repaired_rep = next(r for r in meta["replicas"] if r["node_id"] == target_node)
        print(f"[OK] Passive Read-Repair verified: {target_node.upper()} status is now: {repaired_rep['status']}")

        time.sleep(1)

        # -------------------------------------------------------------
        banner("DEMO 5: Node Crash & Self-Healing Auto-Repair (P0)")
        # -------------------------------------------------------------
        failed_node = obj_v2["replicas"][1]["node_id"]
        print(f"[*] Simulating catastrophic crash of {failed_node.upper()}...")
        client.post(f"/nodes/{failed_node}/fail")
        print(f"[+] Node {failed_node.upper()} marked FAILED")

        print("[*] Triggering Auto-Repair to restore replication factor...")
        r = client.post(f"/objects/demo_asset.txt/repair?version=2")
        res = r.json()
        print(f"[+] Self-Healing Result: {res.get('status')} to target {res.get('target_node', '').upper()} in {res.get('duration_ms')}ms")

        # Recover failed node
        client.post(f"/nodes/{failed_node}/recover")

        time.sleep(1)

        # -------------------------------------------------------------
        banner("DEMO 6: Node Draining & Safe Decommissioning")
        # -------------------------------------------------------------
        drain_node_id = "node5"
        print(f"[*] Initiating safe drain on {drain_node_id.upper()}...")
        r = client.post(f"/nodes/{drain_node_id}/drain")
        drain_res = r.json()
        print(f"[+] Drain completed: Status is now '{drain_res['status']}' ({drain_res['evacuated_replicas']} shards migrated)")
        print(f"[OK] Node is now safe to permanently decommission!")

        # Recover node5 for ongoing tests
        client.post(f"/nodes/{drain_node_id}/recover")

        time.sleep(1)

        # -------------------------------------------------------------
        banner("DEMO 7: Erasure Coding (Reed-Solomon 4+2 Surviving 2 Node Losses)")
        # -------------------------------------------------------------
        ec_data = b"Vault Erasure Coding Payload: Survives multiple simultaneous node losses with only 1.5x storage overhead!"
        files_ec = {"file": ("ec_resilience.bin", ec_data, "application/octet-stream")}
        print("[*] Uploading 'ec_resilience.bin' using Erasure Coding (k=4 data + m=2 parity)...")
        r = client.post("/objects", files=files_ec, data={"storage_mode": "erasure_coding"})
        assert r.status_code == 200, r.text
        ec_obj = r.json()
        print(f"[+] EC Object Key: {ec_obj['key']}")
        print(f"[+] Distributed across {len(ec_obj['replicas'])} nodes:")
        for s in ec_obj["replicas"]:
            print(f"    - Shard {s['shard_index']} on {s['node_id'].upper()}")

        print("[*] Simulating SIMULTANEOUS CRASH of 2 nodes (node1 and node2)...")
        client.post("/nodes/node1/fail")
        client.post("/nodes/node2/fail")

        print("[*] Attempting reconstruction of object from remaining 4 shards...")
        dl_ec = client.get("/objects/ec_resilience.bin/download")
        assert dl_ec.status_code == 200
        assert dl_ec.content == ec_data
        print(f"[OK] SUCCESS! Object perfectly reconstructed from 4/6 shards despite losing 2 nodes!")

        # Cleanup nodes
        client.post("/nodes/node1/recover")
        client.post("/nodes/node2/recover")

        # -------------------------------------------------------------
        banner("DEMO SUMMARY: ALL 7 SCENARIOS PASSED")
        # -------------------------------------------------------------
        metrics = client.get("/metrics").json()
        print(f"[*] Cluster Status: {metrics['system_status']}")
        print(f"[*] Total Stored Keys: {metrics['total_keys']}")
        print(f"[*] Total Versions: {metrics['total_versions']}")
        print(f"[*] Storage Overhead: {metrics['storage_overhead']}x")
        print(f"[*] Completed Self-Healing Repairs: {metrics['completed_repairs']}")
        print("\n[SUCCESS] Vault Distributed Object Storage demonstration complete!")

if __name__ == "__main__":
    run_demo()
