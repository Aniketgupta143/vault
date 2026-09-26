"""
Main entrypoint for Vault Distributed Object Storage.
Starts the FastAPI gateway, background health monitor, self-healing workers,
and loads the single source of truth configuration.
Run with: python main.py
"""
import sys
import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

import config
from gateway import app
from metadata_service import init_db, log_event
import health_monitor
import repair_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("vault.main")

background_tasks = []

async def background_cluster_worker():
    """Continuously runs node heartbeats, failure detection, and self-healing auto-repair."""
    logger.info("Vault background monitor & auto-healer started.")
    while True:
        try:
            # 1. Pulse active healthy nodes
            health_monitor.pulse_active_nodes()

            # 2. Check for timed out nodes
            timed_out = health_monitor.check_heartbeats()
            if timed_out:
                logger.warning(f"Nodes timed out: {timed_out}")

            # 3. Trigger auto-repair cycle
            if config.AUTO_REPAIR_ENABLED:
                repaired = repair_service.run_auto_repair_cycle()
                if repaired:
                    logger.info(f"Auto-repair cycle executed: {len(repaired)} items healed.")

        except Exception as e:
            logger.error(f"Error in background worker: {e}")

        await asyncio.sleep(config.AUTO_REPAIR_INTERVAL_SECONDS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup sequence
    logger.info("Initializing Vault Distributed Object Storage...")
    init_db()
    health_monitor.pulse_active_nodes()
    log_event("SYSTEM", f"Vault Gateway started on port {config.PORT} (Storage mode: {config.STORAGE_MODE})")

    # Start background healing and monitoring worker
    worker_task = asyncio.create_task(background_cluster_worker())
    background_tasks.append(worker_task)

    yield

    # Shutdown sequence
    logger.info("Shutting down Vault Gateway...")
    for t in background_tasks:
        t.cancel()
    log_event("SYSTEM", "Vault Gateway shutdown clean")

# Attach lifespan to gateway app
app.router.lifespan_context = lifespan

def main():
    print(r"""
    __      __         _ _   
    \ \    / /        | | |  
     \ \  / /_ _ _   _| | |_ 
      \ \/ / _` | | | | | __|
       \  / (_| | |_| | | |_ 
        \/ \__,_|\__,_|_|\__|
    Fault-Tolerant Distributed Object Storage
    """)
    print(f"[*] Starting Vault on http://{config.HOST}:{config.PORT}")
    print(f"[*] Storage mode: {config.STORAGE_MODE}")
    print(f"[*] Single source of truth: config.py")
    print(f"[*] Dashboard: http://127.0.0.1:{config.PORT}/")

    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_level="info"
    )

if __name__ == "__main__":
    main()
