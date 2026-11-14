"""Standalone Background Broker & WebSocket Event Worker.

Runs as an independent long-running service responsible for maintaining
live WebSocket streams and processing fills for all active Zebu trading accounts.
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from app.config import CFG
from app.database import init_models, AsyncSessionLocal
from app.store import Store
from app.zebu import ZebuClient
from app.connection_manager import ConnectionManager
from app.scheduler import now_ist
from app import engine

log = logging.getLogger("tts.worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [WORKER] %(name)s: %(message)s")

DB = Store(CFG.DB_PATH)
ZEBU = ZebuClient(CFG.ZEBU_BASE_URL, CFG.ZEBU_CLIENT_ID, CFG.ZEBU_SECRET_KEY, timeout=CFG.ZEBU_TIMEOUT)
CONN_MGR = ConnectionManager(ZEBU)


async def handle_live_fill(account: dict, fill_event: dict):
    """Processes incoming real-time fill from WebSocket."""
    now = now_ist()
    aid = account["id"]
    log.info(f"Processing live fill for account {aid}: {fill_event.get('norenordno')} {fill_event.get('tsym')}")

    today_str = now.date().isoformat()
    fid = f"{aid}|{fill_event.get('norenordno')}|{fill_event.get('flid') or fill_event.get('fltm', '')}"
    await DB._put_many_fills([(fid, {**fill_event, "_acc": aid, "_day": today_str})])

    # Rebuild trades for this account
    cutoff = (now.date() - timedelta(days=CFG.REBUILD_LOOKBACK_DAYS)).isoformat()
    all_fills = await DB._all_fills()
    acc_fills = [f for f in all_fills if f.get("_acc") == aid and f.get("_day", "") >= cutoff]
    charges_cfg = {
        "brokerage_per_order": CFG.CHG_BROKERAGE_PER_ORDER,
        "stt_sell_pct": CFG.CHG_STT_SELL_PCT,
        "txn_pct": CFG.CHG_TXN_PCT,
        "stamp_buy_pct": CFG.CHG_STAMP_BUY_PCT,
    }
    product_map = json.loads(CFG.PRODUCT_MAP)
    built = engine.build_trades(account, acc_fills, charges_cfg, product_map, lambda o: False, now)

    # Save rebuilt trades
    await DB._put_many_trades([(t["id"], t) for t in built])
    log.info(f"Rebuilt {len(built)} trades for account {aid} upon live fill.")


async def run_worker():
    log.info("Starting TTS Background Broker Worker...")
    await init_models()
    await CONN_MGR.start()

    # Discover and connect accounts
    accounts = await DB.get_all_accounts_async()
    log.info(f"Found {len(accounts)} accounts in database.")

    for acc in accounts:
        tok_data = await DB.get_token_async(acc["id"])
        if tok_data and tok_data.get("access_token") and acc.get("zebu_uid"):
            await CONN_MGR.register_account_stream(
                account_id=acc["id"],
                uid=acc["zebu_uid"],
                actid=acc.get("zebu_actid") or acc["zebu_uid"],
                token=tok_data["access_token"],
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
            # Windows signal handler fallback
            pass

    try:
        await stop_event.wait()
    finally:
        await CONN_MGR.stop()
        await ZEBU.aclose()
        log.info("TTS Broker Worker shutdown completed cleanly.")


if __name__ == "__main__":
    import json
    from datetime import timedelta
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
