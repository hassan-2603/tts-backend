"""Standalone Background Broker & WebSocket Event Worker.

Runs as an independent long-running service responsible for maintaining
live WebSocket streams and processing fills for all active Zebu trading accounts.
"""
import asyncio
import logging
import signal
import sys
from app.config import CFG
from app.database import init_models
from app.scheduler import now_ist
from app.main import DB, ZEBU, CONN_MGR, poll_account_async, token_for_account_async

log = logging.getLogger("tts.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [WORKER] %(name)s: %(message)s")


async def handle_live_fill(account: dict, fill_event: dict):
    """Processes incoming real-time fill from WebSocket by running full account sync & rule check."""
    now = now_ist()
    aid = account["id"]
    log.info(f"Processing live fill for account {aid}: {fill_event.get('norenordno')} {fill_event.get('tsym')}")
    try:
        await poll_account_async(account, now)
    except Exception as e:
        log.error(f"Error syncing account {aid} on live fill: {e}")


async def run_worker():
    log.info("Starting TTS Background Broker Worker...")
    await init_models()
    await CONN_MGR.start()

    accounts = await DB.get_all_accounts_async()
    log.info(f"Found {len(accounts)} accounts in database.")

    for acc in accounts:
        tok = await token_for_account_async(acc["id"])
        if tok and acc.get("zebu_uid"):
            await CONN_MGR.register_account_stream(
                account_id=acc["id"],
                uid=acc["zebu_uid"],
                actid=acc.get("zebu_actid") or acc["zebu_uid"],
                token=tok,
                on_fill=lambda msg, a=acc: handle_live_fill(a, msg),
            )

    log.info("TTS Broker Worker active and monitoring streams.")

    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("Received termination signal, shutting down worker...")
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
        await CONN_MGR.stop()
        await ZEBU.aclose()
        log.info("TTS Broker Worker shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
