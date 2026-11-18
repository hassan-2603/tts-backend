"""Standalone Market Hours, EOD Finalization & Midnight Archiving Scheduler Service.

Runs as an independent service coordinating market polls, EOD reconciliation,
and midnight archiving using distributed leader election (PostgreSQL advisory lock or Redis).
"""
import asyncio
import logging
import signal
import sys
from app.config import CFG
from app.database import init_models
from app.scheduler import MarketScheduler
from app.main import (
    poll_all_async,
    daily_summary_async,
    get_settings_dict,
    finalize_eod_async,
    midnight_archive_async,
    ZEBU,
    ALERT,
)

log = logging.getLogger("tts.scheduler_service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [SCHEDULER] %(name)s: %(message)s")

scheduler = MarketScheduler(
    poll_fn=lambda: poll_all_async("schedule"),
    summary_fn=daily_summary_async,
    get_settings_fn=get_settings_dict,
    finalize_eod_fn=finalize_eod_async,
    midnight_archive_fn=midnight_archive_async,
)


async def run_scheduler():
    log.info("Starting TTS Standalone Scheduler Service...")
    await init_models()
    await scheduler.start()

    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("Received termination signal, shutting down scheduler...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _signal_handler)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        await scheduler.stop()
        await ZEBU.aclose()
        await ALERT.aclose()
        log.info("TTS Scheduler Service shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(run_scheduler())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
