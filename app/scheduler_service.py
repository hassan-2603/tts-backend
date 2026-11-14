"""Standalone Market Hours & EOD Reconciliation Scheduler Service.

Runs as an independent service coordinating market polls and daily summaries
using distributed leader election (PostgreSQL advisory lock or Redis).
"""
import asyncio
import logging
import signal
import sys
from app.config import CFG
from app.database import init_models
from app.store import Store
from app.zebu import ZebuClient
from app.alerts import Alerter
from app.connection_manager import ConnectionManager
from app.scheduler import MarketScheduler, now_ist
from app import engine

log = logging.getLogger("tts.scheduler_service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [SCHEDULER] %(name)s: %(message)s")

DB = Store(CFG.DB_PATH)
ZEBU = ZebuClient(CFG.ZEBU_BASE_URL, CFG.ZEBU_CLIENT_ID, CFG.ZEBU_SECRET_KEY, timeout=CFG.ZEBU_TIMEOUT)
ALERT = Alerter(dict(sys.modules.get("os").environ))
CONN_MGR = ConnectionManager(ZEBU)


async def poll_scheduled():
    log.info("Executing scheduled market poll...")
    # Trigger account polling logic
    now = now_ist()
    accounts = await DB.get_all_accounts_async()
    for acc in accounts:
        if acc.get("broker") == "Zebu" and acc.get("zebu_uid"):
            log.info(f"Checking account {acc['id']}")


async def daily_summary_scheduled(now):
    log.info(f"Executing scheduled EOD summary for {now.date().isoformat()}...")
    s = (await DB._get_setting("main")) or {}
    all_trades = await DB._all_trades()
    trades = [t for t in all_trades if t.get("date") == now.date().isoformat()]
    total_pnl = sum(engine.net_pnl(t) for t in trades)
    text_summary = f"📊 Daily summary · {now.date().isoformat()}\nTotal Trades: {len(trades)} · P&L: ₹{total_pnl:,.2f}"
    await ALERT.send_async(text_summary, s.get("alerts", {}), severity="Medium")


async def get_scheduler_settings():
    return (await DB._get_setting("main")) or {}


scheduler = MarketScheduler(
    poll_fn=poll_scheduled,
    summary_fn=daily_summary_scheduled,
    get_settings_fn=get_scheduler_settings,
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
        log.info("TTS Scheduler Service shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(run_scheduler())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
