# 🛡️ VAULT: Fault-Tolerant Distributed Object Storage

> **Hackathon Prototype**  
> A lightweight, fault-tolerant distributed object storage system demonstrating quorum writes, self-healing automatic replica repair, SHA-256 integrity scrubbing, failover reads, and cluster rebalancing.

---

## 🏛️ System Architecture

```text
                           +------------------------+
                           |     WEB DASHBOARD      |
                           |  (HTML5 / CSS3 / ES6)  |
                           +-----------+------------+
                                       |
                                       v
                           +------------------------+
                           |    FASTAPI GATEWAY     |
                           | (Lifespan & Auto-Heal) |
                           +-----------+------------+
                                       |
                   +-------------------+-------------------+
                   |                                       |
                   v                                       v
        +--------------------+                   +--------------------+
        |  METADATA MANAGER  |                   |    NODE MANAGER    |
        |   (SQLite + WAL)   |                   | (Health & Repair)  |
        +--------------------+                   +----------+---------+
                   |                                        |
       [Objects / Replicas / Jobs]           +--------------+--------------+
                                             |              |              |
                                             v              v              v
                                        +---------+    +---------+    +---------+
                                        | NODE 1  |    | NODE 2  |    | NODE 3  |
                                        +---------+    +---------+    +---------+
                                             |              |              |
                                             +--------------+--------------+
                                                            |
                                                            v (Spare for repair/rebalance)
                                                       +---------+
                                                       | NODE 4  |
                                                       +---------+
```

Each storage node is simulated as an isolated directory with its own physical I/O boundaries:
`storage/node1/`, `storage/node2/`, `storage/node3/`, `storage/node4/`.

---

## 🚀 Quickstart

### 1. Prerequisites
- **Python 3.11+**
- PowerShell or Bash

### 2. Activate Virtual Environment & Run Gateway
```powershell
# Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# Start the Vault Gateway
python -m uvicorn app:app --reload --port 8000
```
Open your browser and navigate to:  
👉 **`http://127.0.0.1:8000`**

### 3. Run Automated 4-Scenario Hackathon Demo
In a separate terminal window:
```powershell
.\.venv\Scripts\python.exe demo.py
```

### 4. Run the Automated Test Suite (14 Tests)
```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

---

## 🎯 The Core Hackathon Demos

### Scenario 1: Object Upload & Quorum Replication
1. Select a file and click **"Upload New Object"** (default $RF=3$, write quorum $=2$).
2. The Gateway computes the **SHA-256** checksum.
3. Vault selects 3 healthy nodes with lowest utilization and writes replicas using crash-consistent atomic writes (`temp` $\rightarrow$ `fsync` $\rightarrow$ `replace`).
4. Replicas are verified on disk, metadata is recorded in SQLite (WAL mode), and the dashboard updates in real-time.

### Scenario 2: Node Failure & Self-Healing Auto-Repair (P0)
1. On **Node 1**, click **"Fail Node"**.
2. Node 1 is marked `FAILED` and stops receiving heartbeats.
3. Vault detects that objects stored on Node 1 are now `UNDER_REPLICATED` (2/3 replicas).
4. The background **Auto-Healer** triggers:
   - Identifies a healthy source replica on Node 2 or Node 3.
   - Selects spare healthy **Node 4**.
   - **Safety Invariant**: `COPY` $\rightarrow$ `VERIFY SHA-256` $\rightarrow$ `REGISTER TARGET METADATA` $\rightarrow$ `CLEANUP STALE`.
   - Never deletes or deregisters until the replacement is verified.
5. The dashboard shows $RF$ restored to 3/3 on active nodes.
6. The user can still download the file without data loss.

### Scenario 3: Silent Data Corruption Detection & Healing (P1)
1. In the Objects table, click **"💥 Corrupt"** on any object.
2. Injects simulated bit-rot / byte corruption directly into a physical replica on disk.
3. Click **"🔍 Verify"** (or **"Verify All Checksums"**):
   - The integrity scrubber computes disk checksums and compares against the recorded authoritative hash.
   - Detects the mismatch and flags the replica as `CORRUPTED`.
4. Click **"🩹 Repair"** (or let the background auto-healer run):
   - Copies clean data from a healthy replica.
   - Overwrites and heals the corrupted replica in-place.
   - Verifies SHA-256 and restores healthy status.

### Scenario 4: Storage Rebalancing (P2)
1. Click **"Rebalance Cluster"**.
2. Analyzes storage capacity disparity across healthy nodes.
3. Migrates replicas from overloaded nodes to under-utilized nodes with full SHA-256 verification before pruning old copies.

---

## 📡 API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/objects` | Upload file (multipart form with `file` & `replication_factor`) |
| `GET` | `/objects` | List all stored objects with replica placements and health |
| `GET` | `/objects/{object_id}` | Retrieve object metadata |
| `GET` | `/objects/{object_id}/download` | Stream object content with automatic failover across replicas |
| `DELETE` | `/objects/{object_id}` | Delete object and all physical replicas |
| `GET` | `/nodes` | Get all storage nodes, status, and disk utilization |
| `POST` | `/nodes/{node_id}/fail` | Simulate node crash |
| `POST` | `/nodes/{node_id}/recover` | Recover failed node |
| `POST` | `/nodes/{node_id}/disconnect` | Simulate network partition isolating a node |
| `POST` | `/objects/{object_id}/verify` | Run integrity check across an object's replicas |
| `POST` | `/verify/all` | Scrub entire cluster for bit-rot |
| `POST` | `/objects/{object_id}/corrupt` | Inject byte corruption into a replica (demo simulation) |
| `POST` | `/objects/{object_id}/repair` | Manually heal under-replicated or corrupted object |
| `POST` | `/rebalance` | Trigger cluster rebalancing |
| `GET` | `/health` | Cluster health summary |
| `GET` | `/metrics` | Operational metrics (latencies, repair times, overhead) |
| `GET` | `/events` | Audit log of system events |

---

## 📊 Dashboard Preview

The single-page dashboard features:
- **System Status Badge**: Live indicators (🟢 `HEALTHY`, 🟡 `DEGRADED`, 🔴 `CRITICAL`).
- **Nodes Grid**: Visual cards for Nodes 1–4 showing real-time disk consumption, active replica count, and failure simulation triggers.
- **Metrics Dashboard**: Total objects, logical size, physical stored size, storage overhead multiplier, completed repairs, and latencies.
- **Interactive Objects Table**: Lists all objects with replica placements (`node1: OK`, `node2: CORRUPTED`, etc.) and instant demo actions.
- **System Audit Log Stream**: Real-time log terminal showing color-coded tags (`[UPLOAD]`, `[REPLICATION]`, `[FAILURE]`, `[REPAIR]`, `[INTEGRITY]`, `[REBALANCE]`).

---

## ⚖️ What This Prototype Is & Isn't

> **Disclaimer**: Vault is an educational distributed systems prototype built to demonstrate fundamental concepts (quorum writes, replica repair, data scrubbing, failover, metadata consistency).  
> It does not claim enterprise production durability, multi-datacenter consensus (e.g. Raft/Paxos), or TLS encryption across nodes.
