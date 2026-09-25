// Vault Distributed Storage Dashboard Client
const API_BASE = "";

// Format helpers
function formatBytes(bytes) {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

function formatTime(timestamp) {
  const d = new Date(timestamp * 1000);
  return d.toTimeString().split(" ")[0];
}

// State
let selectedUploadFile = null;

// Initial Setup
document.addEventListener("DOMContentLoaded", () => {
  const fileInput = document.getElementById("file-input");
  fileInput.addEventListener("change", (e) => {
    if (e.target.files.length > 0) {
      selectedUploadFile = e.target.files[0];
      document.getElementById("selected-filename").innerText = `${selectedUploadFile.name} (${formatBytes(selectedUploadFile.size)})`;
      document.getElementById("btn-submit-upload").style.display = "inline-flex";
    }
  });

  // Start polling
  fetchAllData();
  setInterval(fetchAllData, 2000);
});

// Master poll
async function fetchAllData() {
  await Promise.all([
    fetchMetrics(),
    fetchNodes(),
    fetchObjects(),
    fetchEvents()
  ]);
}

// -----------------------------------------------------------
// Metrics & Health
// -----------------------------------------------------------
async function fetchMetrics() {
  try {
    const res = await fetch(`${API_BASE}/metrics`);
    if (!res.ok) return;
    const m = await res.json();

    // Update Status Pill
    const dot = document.getElementById("system-dot");
    const statusText = document.getElementById("system-status-text");
    dot.className = `status-indicator-dot ${m.system_status.toLowerCase()}`;
    statusText.innerText = m.system_status;

    // Storage
    document.getElementById("stat-objects").innerText = m.total_objects;
    document.getElementById("stat-logical-size").innerText = formatBytes(m.logical_bytes);
    document.getElementById("stat-physical-size").innerText = formatBytes(m.physical_bytes);
    document.getElementById("stat-overhead").innerText = `${m.storage_overhead}x`;

    // Reliability
    document.getElementById("stat-healthy-replicas").innerText = m.healthy_replicas;
    document.getElementById("stat-failed-nodes").innerText = m.failed_nodes + m.disconnected_nodes;
    document.getElementById("stat-corrupted-replicas").innerText = m.corrupted_replicas;
    document.getElementById("stat-repairs").innerText = m.completed_repairs;

    // Performance
    document.getElementById("stat-avg-repair").innerText = `${m.avg_repair_duration_ms} ms`;
    document.getElementById("stat-upload-latency").innerText = `${m.upload_latency_ms} ms`;
    document.getElementById("stat-download-latency").innerText = `${m.download_latency_ms} ms`;
  } catch (err) {
    console.error("Error fetching metrics:", err);
  }
}

// -----------------------------------------------------------
// Nodes Rendering & Failure Injection
// -----------------------------------------------------------
async function fetchNodes() {
  try {
    const res = await fetch(`${API_BASE}/nodes`);
    if (!res.ok) return;
    const nodes = await res.json();

    const container = document.getElementById("nodes-container");
    container.innerHTML = "";

    let healthyCount = 0;
    nodes.forEach((n) => {
      if (n.status === "HEALTHY") healthyCount++;
      const usedPct = Math.min(100, Math.round((n.used_storage / n.capacity) * 100));

      const card = document.createElement("div");
      card.className = `node-card ${n.status.toLowerCase()}`;

      // Action buttons based on current state
      let actionButtons = "";
      if (n.status === "HEALTHY") {
        actionButtons = `
          <button class="btn btn-sm btn-danger" onclick="failNode('${n.node_id}')">Fail Node</button>
          <button class="btn btn-sm btn-warning" onclick="disconnectNode('${n.node_id}')">Disconnect</button>
        `;
      } else {
        actionButtons = `
          <button class="btn btn-sm btn-success" onclick="recoverNode('${n.node_id}')">Recover Node</button>
        `;
      }

      card.innerHTML = `
        <div class="node-card-header">
          <div class="node-title">
            <span>💾</span> ${n.node_id.toUpperCase()}
          </div>
          <span class="node-status-pill ${n.status.toLowerCase()}">${n.status}</span>
        </div>
        
        <div class="node-storage-bar-bg">
          <div class="node-storage-bar-fill" style="width: ${Math.max(usedPct, 4)}%;"></div>
        </div>

        <div class="node-stats-row">
          <span>Used: <strong>${formatBytes(n.used_storage)}</strong> / ${formatBytes(n.capacity)}</span>
          <span>Replicas: <strong>${n.active_replicas_count}</strong></span>
        </div>

        <div class="node-actions">
          ${actionButtons}
        </div>
      `;
      container.appendChild(card);
    });

    document.getElementById("nodes-count-badge").innerText = `${healthyCount} of ${nodes.length} Nodes Healthy`;
  } catch (err) {
    console.error("Error fetching nodes:", err);
  }
}

async function failNode(nodeId) {
  try {
    await fetch(`${API_BASE}/nodes/${nodeId}/fail`, { method: "POST" });
    fetchAllData();
  } catch (err) {
    alert(`Failed to trigger failure on node ${nodeId}: ${err}`);
  }
}

async function recoverNode(nodeId) {
  try {
    await fetch(`${API_BASE}/nodes/${nodeId}/recover`, { method: "POST" });
    fetchAllData();
  } catch (err) {
    alert(`Failed to recover node ${nodeId}: ${err}`);
  }
}

async function disconnectNode(nodeId) {
  try {
    await fetch(`${API_BASE}/nodes/${nodeId}/disconnect`, { method: "POST" });
    fetchAllData();
  } catch (err) {
    alert(`Failed to disconnect node ${nodeId}: ${err}`);
  }
}

// -----------------------------------------------------------
// Objects Rendering & Demo Actions
// -----------------------------------------------------------
async function fetchObjects() {
  try {
    const res = await fetch(`${API_BASE}/objects`);
    if (!res.ok) return;
    const objects = await res.json();

    const tbody = document.getElementById("objects-table-body");
    tbody.innerHTML = "";

    document.getElementById("objects-count-text").innerText = `${objects.length} objects stored`;

    if (objects.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--text-muted); padding: 24px;">No objects in Vault yet. Upload a file above to begin the demo!</td></tr>`;
      return;
    }

    objects.forEach((obj) => {
      const tr = document.createElement("tr");

      // Placements chips
      let chipsHtml = "";
      obj.replicas.forEach((r) => {
        let chipClass = r.status.toLowerCase();
        let displayTxt = `${r.node_id}: ${r.status}`;
        if (r.node_status !== "HEALTHY") {
          chipClass += " node-down";
          displayTxt += ` (${r.node_status})`;
        }
        chipsHtml += `<span class="replica-chip ${chipClass}" title="Last verified: ${formatTime(r.last_verified)}">${displayTxt}</span>`;
      });

      // Health badge
      const healthClass = obj.health_status.toLowerCase();
      const healthBadge = `<span class="status-badge ${healthClass}">${obj.health_status.replace("_", " ")}</span>`;

      // Checksum display
      const shortHash = obj.checksum.substring(0, 10) + "...";

      tr.innerHTML = `
        <td><strong>${obj.filename}</strong></td>
        <td class="font-mono text-muted">${obj.object_id}</td>
        <td>${formatBytes(obj.size)}</td>
        <td class="font-mono">v${obj.version}</td>
        <td class="font-mono" title="${obj.checksum}">${shortHash}</td>
        <td><div class="replica-chips">${chipsHtml}</div></td>
        <td>${healthBadge}</td>
        <td>
          <div class="actions-cell">
            <button class="btn btn-sm btn-secondary" onclick="downloadObject('${obj.object_id}', '${obj.filename}')" title="Download file">⬇ Download</button>
            <button class="btn btn-sm btn-secondary" onclick="verifyObject('${obj.object_id}')" title="Verify SHA-256 Checksum">🔍 Verify</button>
            <button class="btn btn-sm btn-warning" onclick="corruptObject('${obj.object_id}')" title="Simulate silent byte corruption">💥 Corrupt</button>
            <button class="btn btn-sm btn-success" onclick="repairObject('${obj.object_id}')" title="Trigger self-healing repair">🩹 Repair</button>
            <button class="btn btn-sm btn-danger" onclick="deleteObject('${obj.object_id}')" title="Delete object">🗑</button>
          </div>
        </td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error("Error fetching objects:", err);
  }
}

// Upload
async function handleUpload() {
  if (!selectedUploadFile) return;

  const rf = document.getElementById("rf-select").value;
  const formData = new FormData();
  formData.append("file", selectedUploadFile);
  formData.append("replication_factor", rf);

  const btn = document.getElementById("btn-submit-upload");
  btn.disabled = true;
  btn.innerText = "Replicating...";

  try {
    const res = await fetch(`${API_BASE}/objects`, {
      method: "POST",
      body: formData
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Upload error");
    }
    // Reset file input
    document.getElementById("file-input").value = "";
    document.getElementById("selected-filename").innerText = "";
    btn.style.display = "none";
    selectedUploadFile = null;
    fetchAllData();
  } catch (err) {
    alert(`Upload failed: ${err.message}`);
  } finally {
    btn.disabled = false;
    btn.innerText = "Confirm Upload";
  }
}

// Download
function downloadObject(objectId, filename) {
  window.location.href = `${API_BASE}/objects/${objectId}/download`;
}

// Verify single object
async function verifyObject(objectId) {
  try {
    const res = await fetch(`${API_BASE}/objects/${objectId}/verify`, { method: "POST" });
    const data = await res.json();
    fetchAllData();
  } catch (err) {
    alert(`Verify failed: ${err}`);
  }
}

// Corrupt single object (Demo scenario)
async function corruptObject(objectId) {
  if (!confirm(`Are you sure you want to simulate byte corruption on a replica of ${objectId}?`)) return;
  try {
    const res = await fetch(`${API_BASE}/objects/${objectId}/corrupt`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Corruption error");
    }
    fetchAllData();
  } catch (err) {
    alert(`Corruption simulation failed: ${err.message}`);
  }
}

// Repair single object
async function repairObject(objectId) {
  try {
    const res = await fetch(`${API_BASE}/objects/${objectId}/repair`, { method: "POST" });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Repair error");
    }
    fetchAllData();
  } catch (err) {
    alert(`Repair failed: ${err.message}`);
  }
}

// Delete object
async function deleteObject(objectId) {
  if (!confirm(`Permanently delete object ${objectId} and all its replicas?`)) return;
  try {
    const res = await fetch(`${API_BASE}/objects/${objectId}`, { method: "DELETE" });
    fetchAllData();
  } catch (err) {
    alert(`Delete failed: ${err}`);
  }
}

// Scrub all
async function triggerScrubAll() {
  try {
    await fetch(`${API_BASE}/verify/all`, { method: "POST" });
    fetchAllData();
  } catch (err) {
    alert(`Scrub failed: ${err}`);
  }
}

// Trigger Auto-Repair for all under-replicated
async function triggerAutoRepair() {
  try {
    const res = await fetch(`${API_BASE}/objects`);
    const objects = await res.json();
    let repairedCount = 0;
    for (const obj of objects) {
      if (obj.health_status !== "HEALTHY") {
        await fetch(`${API_BASE}/objects/${obj.object_id}/repair`, { method: "POST" });
        repairedCount++;
      }
    }
    fetchAllData();
  } catch (err) {
    alert(`Auto-repair trigger failed: ${err}`);
  }
}

// Rebalance
async function triggerRebalance() {
  try {
    const res = await fetch(`${API_BASE}/rebalance`, { method: "POST" });
    const data = await res.json();
    alert(data.message || "Rebalance completed.");
    fetchAllData();
  } catch (err) {
    alert(`Rebalance failed: ${err}`);
  }
}

// -----------------------------------------------------------
// Event Logs Stream
// -----------------------------------------------------------
async function fetchEvents() {
  try {
    const res = await fetch(`${API_BASE}/events?limit=30`);
    if (!res.ok) return;
    const events = await res.json();

    const terminal = document.getElementById("terminal-logs");
    terminal.innerHTML = "";

    events.forEach((ev) => {
      const row = document.createElement("div");
      row.className = "log-entry";

      const timeStr = formatTime(ev.timestamp);
      row.innerHTML = `
        <span class="log-time">${timeStr}</span>
        <span class="log-tag ${ev.category}">${ev.category}</span>
        <span class="log-msg">${ev.message}</span>
      `;
      terminal.appendChild(row);
    });
  } catch (err) {
    console.error("Error fetching logs:", err);
  }
}
