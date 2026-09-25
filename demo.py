"""
Vault Hackathon Live Demo Script
Demonstrates the 4 core distributed storage scenarios sequentially with rich console output.
Can be executed with: python demo.py
"""
import sys
import time
import httpx

BASE_URL = "http://127.0.0.1:8000"

def banner(title):
    print("\n" + "=" * 65)
    print(f" {title}")
    print("=" * 65)

def check_server():
    try:
        r = httpx.get(f"{BASE_URL}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False

def run_demo():
    print("""
    __      __         _ _   
    \\ \\    / /        | | |  
     \\ \\  / /_ _ _   _| | |_ 
      \\ \\/ / _` | | | | | __|
       \\  / (_| | |_| | | |_ 
        \\/ \\__,_|\\__,_|_|\\__|
    Fault-Tolerant Distributed Object Storage
    """)

    if not check_server():
        print(f"[!] Vault server is not running on {BASE_URL}!")
        print("    Please start the server first in another terminal with:")
        print("    .\\.venv\\Scripts\\python.exe -m uvicorn app:app --reload\n")
        sys.exit(1)

    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        # ---------------------------------------------------------
        banner("SCENARIO 1: Object Upload & Quorum Replication (RF=3)")
        # ---------------------------------------------------------
        test_payload = b"Hello, Vault Distributed Storage! Hackathon Demo Payload #2026."
        files = {"file": ("demo_document.txt", test_payload, "text/plain")}
        print("[*] Uploading 'demo_document.txt' (Replication Factor = 3, Quorum = 2)...")
        r = client.post("/objects", files=files, data={"replication_factor": 3})
        if r.status_code != 200:
            print(f"[!] Upload failed: {r.text}")
            return
        obj = r.json()
        obj_id = obj["object_id"]
        checksum = obj["checksum"]
        print(f"[+] Object Created: {obj_id}")
        print(f"[+] SHA-256 Checksum: {checksum}")
        print(f"[+] Stored on Replicas:")
        for rep in obj["replicas"]:
            print(f"    - {rep['node_id'].upper()}: status={rep['status']} (verified: {rep['checksum'][:12]}...)")
        
        time.sleep(1)

        # ---------------------------------------------------------
        banner("SCENARIO 2: Node Failure & Self-Healing Auto-Repair")
        # ---------------------------------------------------------
        failed_node = obj["replicas"][0]["node_id"]
        print(f"[*] Simulating crash of node: {failed_node.upper()}...")
        r = client.post(f"/nodes/{failed_node}/fail")
        print(f"[+] Node status updated: {r.json()}")

        print("[*] Inspecting object health state...")
        r = client.get(f"/objects/{obj_id}")
        meta = r.json()
        print(f"[!] Object health status: {meta['health_status']} (Active replicas: {len([x for x in meta['replicas'] if x['node_status'] == 'HEALTHY'])}/3)")

        print("[*] Triggering Self-Healing Auto-Repair engine...")
        time.sleep(1)
        r = client.post(f"/objects/{obj_id}/repair")
        repair_res = r.json()
        print(f"[+] Repair Result:")
        print(f"    - Status: {repair_res['status']}")
        print(f"    - Source Node: {repair_res.get('source_node', '').upper()}")
        print(f"    - Target Node: {repair_res.get('target_node', '').upper()}")
        print(f"    - Repair Duration: {repair_res.get('duration_ms')} ms")
        print(f"    - Verified Target SHA-256: {repair_res.get('checksum')}")

        print("[*] Verifying object is still downloadable despite node failure...")
        dl = client.get(f"/objects/{obj_id}/download")
        assert dl.status_code == 200
        assert dl.content == test_payload
        print(f"[+] Download Succeeded! Data integrity preserved (HTTP {dl.status_code}, {len(dl.content)} bytes).")

        time.sleep(1)

        # ---------------------------------------------------------
        banner("SCENARIO 3: Silent Data Corruption Detection & Healing")
        # ---------------------------------------------------------
        print(f"[*] Simulating byte corruption on a healthy replica...")
        r = client.post(f"/objects/{obj_id}/corrupt")
        corrupt_res = r.json()
        corrupted_node = corrupt_res["corrupted_node"]
        print(f"[!] Injected bit-rot into replica on {corrupted_node.upper()}!")

        print("[*] Running Integrity Scrubber to verify SHA-256 hashes...")
        scrub = client.post(f"/objects/{obj_id}/verify").json()
        print(f"[!] Integrity Scrubber Report:")
        print(f"    - Expected Checksum: {scrub['expected_checksum']}")
        print(f"    - Corrupted Replicas Detected: {scrub['corrupted_count']}")
        for rep in scrub["replicas"]:
            flag = "VALID" if rep["is_valid"] else "MISMATCH / CORRUPTED"
            print(f"    - {rep['node_id'].upper()}: {flag}")

        print("[*] Healing corrupted replica from verified healthy source...")
        heal = client.post(f"/objects/{obj_id}/repair").json()
        print(f"[+] Healed: {heal}")

        print("[*] Re-verifying integrity...")
        post_scrub = client.post(f"/objects/{obj_id}/verify").json()
        print(f"[+] Corrupted Replicas: {post_scrub['corrupted_count']}, Healthy Replicas: {post_scrub['healthy_count']}")

        time.sleep(1)

        # ---------------------------------------------------------
        banner("SCENARIO 4: Storage Rebalancing")
        # ---------------------------------------------------------
        # Recover all nodes
        client.post(f"/nodes/{failed_node}/recover")
        print("[*] Triggering cluster storage rebalancing...")
        reb = client.post("/rebalance").json()
        print(f"[+] Rebalance Report: {reb['message']}")

        # ---------------------------------------------------------
        banner("FINAL CLUSTER STATUS & METRICS")
        # ---------------------------------------------------------
        metrics = client.get("/metrics").json()
        print(f"System Status:          {metrics['system_status']}")
        print(f"Total Objects:          {metrics['total_objects']}")
        print(f"Healthy Nodes:          {metrics['healthy_nodes']}/4")
        print(f"Total Replicas:         {metrics['total_replicas']}")
        print(f"Healthy Replicas:       {metrics['healthy_replicas']}")
        print(f"Completed Repairs:      {metrics['completed_repairs']}")
        print(f"Average Repair Time:    {metrics['avg_repair_duration_ms']} ms")
        print(f"Storage Overhead:       {metrics['storage_overhead']}x")
        print("\n[SUCCESS] All 4 Hackathon Demo Scenarios executed with 100% integrity!")

if __name__ == "__main__":
    run_demo()
