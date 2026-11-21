"""Standalone Independent Notification Background Worker for Trader Tracking System (TTS).

Runs independently of the FastAPI web process:
  python notification_worker.py

Processes PostgreSQL transactional outbox (`notification_jobs`) using `FOR UPDATE SKIP LOCKED`,
recovers abandoned jobs after restarts, and delivers Resend emails (with 6-sheet Excel reports)
and Meta WhatsApp Cloud API template messages with exponential backoff retries.
"""
import asyncio
import logging
import signal

from app.database import init_models
from app.notifications import NotificationEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("tts.notification_worker")


async def run_worker():
    await init_models()
    engine = NotificationEngine()
    stop_event = asyncio.Event()

    def _handle_stop(*_):
        log.info("Shutdown signal received; stopping notification worker...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_stop)
        except Exception:
            pass

    await engine.start_worker()
    log.info("Standalone TTS Notification Worker is running (id=%s)", engine.worker_id)
    try:
        await stop_event.wait()
    finally:
        await engine.stop_worker()


if __name__ == "__main__":
    asyncio.run(run_worker())
