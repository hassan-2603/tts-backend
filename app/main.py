"""Trader Tracking System (TTS) — Modern Real-Time Trading Monitoring Server.

Features:
- High-performance asynchronous FastAPI & PostgreSQL architecture
- Up to 50+ concurrent linked Zebu trading accounts with independent streams & error boundaries
- Zebu / Noren real-time WebSocket ingestion & REST reconciliation (OrderBook, TradeBook, PositionBook)
- Decimal-accurate FIFO trade lifecycle, partial closure handling, and overnight position continuity
- End-of-Day (03:30 PM IST) reconciliation and immutable daily finalization
- Midnight (12:00 AM IST) trade cleanup, daily archiving, and startup recovery
- Automatic rule monitoring with persistent structured red flags & violation explanations
- Account-level authorization: Admin, Trader, Viewer
- Preserves 100% compatibility with existing static/index.html dashboard
- Live Dashboard WebSocket endpoint (/api/ws)
"""
import asyncio
import hashlib
import httpx
import json
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select

from app.config import CFG, ROOT_DIR
from app.database import init_models, AsyncSessionLocal
from app.models import (
    User,
    Session as UserSession,
    LinkNonce,
    AuditLog,
    Trader,
    Account,
    BrokerSession,
    Group,
    Setting,
    Trade,
    Fill,
    Order,
    Position,
    TradingRule,
    RuleLog,
    Alert,
    SentFlag,
    AccountSyncState,
    DailySyncRecord,
    DailyAccountReport,
)
from app.security import hash_pw, check_pw, generate_session_token, generate_link_nonce, encrypt_token, decrypt_token
from app.store import Store
from app.zebu import ZebuClient, SessionExpired, ZebuError
from app.alerts import Alerter
from app.connection_manager import ConnectionManager
from app.financial_summary import FinancialSummaryService
from app.scheduler import MarketScheduler, now_ist, IST
from app.metrics import METRICS
from app import engine

log = logging.getLogger("tts")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# Global singletons
DB = Store(CFG.DB_PATH)
ZEBU = ZebuClient(CFG.ZEBU_BASE_URL, CFG.ZEBU_CLIENT_ID, CFG.ZEBU_SECRET_KEY, timeout=CFG.ZEBU_TIMEOUT)
ALERT = Alerter(dict(os.environ))
CONN_MGR = ConnectionManager(ZEBU)

DEFAULT_SETTINGS = {
    "dataSource": "live",
    "lotSize": 30,
    "strikeStep": 100,
    "testDays": 30,
    "extendDays": 15,
    "pass": {"ruleFollow": 95, "highFlags": 2, "maxDDpct": 10, "accuracy": 50, "rr": 1.5, "pf": 1.3},
    "capitalSteps": "₹5 L → ₹15 L → ₹50 L",
    "alerts": {"telegram": "", "whatsapp": "", "email": "", "onHigh": True, "dailySummary": True, "summaryTime": "15:35"},
}


# ---------------- Spot Price & Order History Cache ----------------
_spot_cache = {}
_index_tokens = {}
_order_hist_cache: Dict[str, List[dict]] = {}


async def index_token_async(underlying: str) -> Optional[Tuple[str, str]]:
    if underlying in _index_tokens:
        return _index_tokens[underlying]

    name = {
        "BANKNIFTY": "NIFTY BANK",
        "NIFTY": "NIFTY 50",
        "FINNIFTY": "NIFTY FIN SERVICE",
        "MIDCPNIFTY": "NIFTY MID SELECT",
        "SENSEX": "SENSEX",
        "BANKEX": "BANKEX",
    }.get(underlying, underlying).upper()

    tok, uid = await any_token_async()
    if tok and uid:
        for exch in ("NSE", "BSE"):
            try:
                for v in await ZEBU.index_list_async(tok, uid, exch):
                    if str(v.get("idxname", "")).upper().replace("  ", " ") == name:
                        _index_tokens[underlying] = (exch, str(v["token"]))
                        return _index_tokens[underlying]
            except Exception as e:
                log.info(f"GetIndexList {exch}: {e}")

    for pair in CFG.INDEX_TOKENS.split(","):
        if "=" in pair:
            sym, fb = pair.split("=", 1)
            if sym.strip() == underlying and ":" in fb:
                exch, t = fb.strip().split(":")
                _index_tokens[underlying] = (exch, t)
                return _index_tokens[underlying]

    return None


async def spot_at_async(underlying: str, ts: datetime) -> Optional[float]:
    key = (underlying, ts.date())
    if key not in _spot_cache or (ts.date() == now_ist().date() and _spot_cache[key].get("_at", 0) < time.time() - 120):
        it = await index_token_async(underlying)
        tok, uid = await any_token_async()
        candles = {}
        if it and tok and uid:
            st = int(datetime.combine(ts.date(), datetime.min.time()).replace(hour=9, minute=15, tzinfo=IST).timestamp())
            et = int(datetime.combine(ts.date(), datetime.min.time()).replace(hour=15, minute=30, tzinfo=IST).timestamp())
            try:
                for c in await ZEBU.tp_series_async(tok, uid, it[0], it[1], st, et, "1"):
                    d = engine.parse_ts(c.get("time"))
                    if d:
                        candles[d.strftime("%H:%M")] = (float(c.get("into") or c.get("intc") or 0), float(c.get("intc") or 0))
            except Exception as e:
                log.warning(f"TPSeries {underlying}: {e}")
        candles["_at"] = time.time()
        _spot_cache[key] = candles

    c = _spot_cache[key]
    hm = ts.strftime("%H:%M")
    if hm in c:
        return c[hm][0]
    earlier = sorted(k for k in c if k != "_at" and k <= hm)
    return c[earlier[-1]][1] if earlier else None


# ---------------- Active Dashboard WebSockets ----------------
class DashboardWebSocketManager:
    def __init__(self):
        # Maps WebSocket -> authenticated user scope dict {user, role, trader_id, assigned_accounts}
        self.active_connections: Dict[WebSocket, Dict[str, Any]] = {}

    async def connect(self, websocket: WebSocket, user_scope: Dict[str, Any]):
        await websocket.accept()
        self.active_connections[websocket] = user_scope

    def disconnect(self, websocket: WebSocket):
        self.active_connections.pop(websocket, None)

    @staticmethod
    def _is_authorized_for_account(scope: Dict[str, Any], account_id: str, trader_id: Optional[str] = None) -> bool:
        role = scope.get("role")
        if role == "admin":
            return True
        if role == "trader":
            u_tid = scope.get("trader_id")
            if not u_tid:
                return False
            return (trader_id is not None and trader_id == u_tid) or (account_id == u_tid)
        if role == "viewer":
            allowed = scope.get("assigned_accounts") or []
            return account_id in allowed
        return False

    async def broadcast(self, message: dict):
        acc_id = message.get("account_id")
        tr_id = message.get("trader_id")
        for connection, scope in list(self.active_connections.items()):
            # Enforce strict per-account isolation for any account-scoped message
            if acc_id and not self._is_authorized_for_account(scope, str(acc_id), tr_id):
                continue
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)


WS_HUB = DashboardWebSocketManager()


async def _ensure_account_ws_stream(acc: dict, token: str):
    """Ensures a single Zebu WebSocket stream is active for `acc` with OrderUpdate + Touchline MTM wiring."""
    aid = acc["id"]
    uid = (acc.get("zebu_uid") or "").strip().upper()
    actid = (acc.get("zebu_actid") or uid).strip().upper()
    if not uid or not token:
        return

    async def _on_fill(msg: dict, a=acc):
        FIN_SUMMARY.schedule_reconcile(a, reason="order_update")
        await poll_account_async(a, now_ist())

    async def _on_touchline(msg: dict, account_id=aid):
        await FIN_SUMMARY.on_touchline_tick(account_id, msg)

    async def _on_status_change(new_status: str, _msg: str, account_id=aid, a=acc):
        await FIN_SUMMARY.on_ws_status_change(account_id, new_status, acc=a)

    await CONN_MGR.register_account_stream(
        account_id=aid,
        uid=uid,
        actid=actid,
        token=token,
        on_fill=_on_fill,
        on_touchline=_on_touchline,
        on_status_change=_on_status_change,
    )


async def _assert_user_can_access_account(user: dict, acc: dict):
    """Strictly enforces per-user account isolation (prevents cross-account parameter tampering)."""
    role = user.get("role")
    if role == "admin":
        return
    u = await DB.get_user_async(user.get("user", ""))
    if role == "trader":
        tid = u.get("trader_id") if u else None
        if not tid or (acc.get("trader") != tid and acc.get("id") != tid):
            raise HTTPException(403, "Not authorized to access this account's financial data")
        return
    if role == "viewer":
        allowed = u.get("assigned_accounts") if u else None
        if not allowed or acc.get("id") not in allowed:
            raise HTTPException(403, "Not authorized to access this account's financial data")
        return
    raise HTTPException(403, "Not authorized to access this account's financial data")


# ---------------- Polling & Synchronization ----------------
def is_algo_order(order: dict) -> bool:
    algo_sources = [s.strip().upper() for s in CFG.ALGO_ORDERSOURCES.split(",") if s.strip()]
    if order.get("ordersource", "").upper() in algo_sources:
        return True
    p = CFG.ALGO_REMARK_PREFIX
    return bool(p and str(order.get("remarks", "")).startswith(p))


def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


async def token_for_account_async(acc_id: str) -> Optional[str]:
    """Returns valid access token for the account (checking both account.id and zebu_uid)."""
    async with AsyncSessionLocal() as session:
        stmt = select(BrokerSession).where(BrokerSession.account_id == acc_id)
        bs = (await session.execute(stmt)).scalar_one_or_none()
        if not bs or not bs.access_token_enc:
            acc_stmt = select(Account).where((Account.id == acc_id) | (Account.zebu_uid == acc_id))
            cur_acc = (await session.execute(acc_stmt)).scalars().first()
            if cur_acc and cur_acc.zebu_uid:
                match_stmt = (
                    select(BrokerSession)
                    .join(Account, Account.id == BrokerSession.account_id)
                    .where(
                        (Account.zebu_uid == cur_acc.zebu_uid) | (BrokerSession.account_id == cur_acc.zebu_uid),
                        BrokerSession.access_token_enc.isnot(None),
                    )
                )
                bs = (await session.execute(match_stmt)).scalars().first()
        if not bs or not bs.access_token_enc:
            return None
        if bs.status in ("Not linked", "Disconnected", "Login expired", "REAUTH_REQUIRED"):
            return None
        exp = ensure_utc(bs.expires_at)
        if exp and exp < datetime.now(timezone.utc):
            bs.status = "Login expired"
            await session.commit()
            return None
        return decrypt_token(bs.access_token_enc)


async def any_token_async() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as session:
        stmt = select(Account, BrokerSession).join(BrokerSession, Account.id == BrokerSession.account_id)
        res = (await session.execute(stmt)).all()
        now_utc = datetime.now(timezone.utc)
        for acc, bs in res:
            if bs.access_token_enc and acc.zebu_uid and bs.status == "Linked":
                exp = ensure_utc(bs.expires_at)
                if not exp or exp >= now_utc:
                    return decrypt_token(bs.access_token_enc), acc.zebu_uid
    return None, None


async def _is_day_locked_for_account_async(aid: str, now: datetime) -> bool:
    """Returns True if today's data for `aid` is locked after EOD finalization (or hard cutoff) until 00:00 IST midnight."""
    cutoff_hr = int(getattr(CFG, "POST_MARKET_HARD_CUTOFF_HOUR", 21) or 21)
    if now.hour >= cutoff_hr:
        return True
    # After market close (15:30 IST), once EOD finalization has locked today's report, automatic polls must not mutate today's data
    if (now.hour, now.minute) > (15, 30):
        today_str = now.date().isoformat()
        reps = await DB.get_daily_reports_async(account_id=aid)
        for r in reps:
            if r.get("account") == aid and r.get("date") == today_str and r.get("status") in ("FINALIZED", "ARCHIVED"):
                return True
    return False


async def _record_post_eod_versioned_reconciliation_async(acc: dict, now: datetime, positions: List[dict]):
    """When a manual forced sync runs after EOD finalization, records a versioned correction note
    on today's DailyAccountReport without creating duplicate rows or mutating past historical dates."""
    aid = acc["id"]
    today_str = now.date().isoformat()
    reps = await DB.get_daily_reports_async(account_id=aid)
    existing = next((r for r in reps if r.get("account") == aid and r.get("date") == today_str), None)
    if not existing or existing.get("status") not in ("FINALIZED", "ARCHIVED"):
        return
    # Compute version number from prior notes
    prev_notes = str(existing.get("reconciliation_notes") or "").strip()
    version = prev_notes.count("[v") + 2
    broker_rpnl = sum((Decimal(str(p.get("realized_pnl") if p.get("realized_pnl") is not None else (p.get("rpnl") or 0))) for p in positions), Decimal("0"))
    broker_urmtom = sum((Decimal(str(p.get("unrealized_pnl") if p.get("unrealized_pnl") is not None else (p.get("urmtom") or 0))) for p in positions), Decimal("0"))
    all_tr = await DB._all_trades(from_date=today_str)
    closed_today = [
        t for t in all_tr
        if t.get("account") == aid and not t.get("open") and ((t.get("exitDate") or t.get("date")) == today_str)
    ]
    open_today = [t for t in all_tr if t.get("account") == aid and t.get("open")]
    fifo_realized = sum((Decimal(str(t.get("realized_pnl") if t.get("realized_pnl") is not None else (t.get("pnl") or 0))) for t in closed_today), Decimal("0"))
    charges_dec = sum((Decimal(str(t.get("charges") or 0)) for t in closed_today), Decimal("0"))
    note_entry = (
        f"[v{version} @ {now.strftime('%H:%M:%S')} IST] Manual forced reconciliation: "
        f"broker_rpnl={broker_rpnl:.2f}, broker_urmtom={broker_urmtom:.2f}, fifo_closed={fifo_realized:.2f} "
        f"(prev_realized={existing.get('realized_pnl', 0):.2f})"
    )
    combined_notes = (prev_notes + "\n" + note_entry).strip() if prev_notes else note_entry
    await DB.put_daily_report_async(f"{aid}|{today_str}", {
        **existing,
        "realized_pnl": float(fifo_realized),
        "unrealized_pnl": float(broker_urmtom),
        "charges": float(charges_dec),
        "net_pnl": float(fifo_realized - charges_dec),
        "closed_trades": len(closed_today),
        "open_trades": len(open_today),
        "total_trades": len(closed_today) + len(open_today),
        "winning_trades": len([t for t in closed_today if float(t.get("realized_pnl") or t.get("pnl") or 0) > 0]),
        "losing_trades": len([t for t in closed_today if float(t.get("realized_pnl") or t.get("pnl") or 0) <= 0]),
        "status": "FINALIZED",
        "reconciliation_notes": combined_notes,
        "finalized_at": datetime.now(timezone.utc),
    }, allow_overwrite_finalized=True)


async def poll_account_async(acc: dict, now: datetime, reconcile_today: bool = False, force: bool = False) -> str:
    """Fetches OrderBook, TradeBook, and PositionBook for an account, reconciles open/closed trades, and evaluates rules."""
    aid = acc["id"]
    # Post-market hard lock for automatic background polls: once market closes & day is finalized (or after 9:00 PM IST),
    # never automatically fetch or modify P&L/trade data from Zebu until 12:00 AM IST midnight rule.
    if not force and await _is_day_locked_for_account_async(aid, now):
        return "locked · market closed (final data locked until 12:00 AM IST)"

    # Never unlock finalized trades after market close (15:30 IST) unless explicitly forced by user manual sync
    if not force and (now.hour, now.minute) > (15, 30):
        reconcile_today = False
    now_utc = datetime.now(timezone.utc)
    await DB.put_sync_state_async(aid, {"last_sync_start": now_utc})

    tok_doc = await DB.get_token_async(aid)
    tok = await token_for_account_async(aid)
    if not tok:
        status_str = (tok_doc or {}).get("status") or "Not linked"
        if status_str in ("Login expired", "REAUTH_REQUIRED"):
            await DB.put_sync_state_async(aid, {
                "status": "REAUTH_REQUIRED",
                "is_stale": True,
                "last_error": "Session expired — reauthorization required",
            })
            return "expired"
        await DB.put_sync_state_async(aid, {"status": "DISCONNECTED", "is_stale": False})
        return "not_linked"

    uid, actid = acc.get("zebu_uid"), acc.get("zebu_actid") or acc.get("zebu_uid")
    proxy_url = acc.get("proxy_url")

    try:
        # Fetch TradeBook, PositionBook, and OrderBook concurrently from actual broker API
        fills_res, positions_res, orders_res = await asyncio.gather(
            ZEBU.trade_book_async(tok, uid, actid, proxy=proxy_url),
            ZEBU.position_book_async(tok, uid, actid, proxy=proxy_url),
            ZEBU.order_book_async(tok, uid, proxy=proxy_url),
            return_exceptions=True,
        )
        # Check if primary endpoints (TradeBook or PositionBook) raised SessionExpired
        for r in (fills_res, positions_res):
            if isinstance(r, SessionExpired):
                raise r

        # If TradeBook or PositionBook failed with another error, raise it so we don't overwrite with partial data
        if isinstance(fills_res, Exception):
            raise fills_res
        if isinstance(positions_res, Exception):
            raise positions_res

        fills = fills_res if isinstance(fills_res, list) else []
        positions = positions_res if isinstance(positions_res, list) else []
        orders = orders_res if isinstance(orders_res, list) else []

    except SessionExpired as se:
        # Attempt automatic token refresh if refresh_token is available
        ref_tok = (tok_doc or {}).get("refresh_token")
        refreshed = None
        if ref_tok:
            refreshed = await ZEBU.refresh_token_async(
                refresh_token=ref_tok,
                uid=uid or "",
                client_id=acc.get("zebu_client_id") or uid or CFG.ZEBU_CLIENT_ID,
                secret_key=acc.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY,
                proxy=proxy_url,
            )
        if refreshed and refreshed.get("access_token"):
            log.info(f"Automatically refreshed expired token for account {aid}")
            await DB.put_token_async(aid, {
                "access_token": refreshed["access_token"],
                "refresh_token": refreshed.get("refresh_token") or ref_tok,
                "status": "Linked",
                "last_sync_at": now_utc,
                "sync_message": "Token auto-refreshed",
            })
            return await poll_account_async(acc, now)

        log.warning(f"Zebu session expired for account {aid}: {se}")
        await DB.put_token_async(aid, {
            "status": "Login expired",
            "ws_status": "REAUTH_REQUIRED",
            "sync_message": f"Session expired: {se}",
        })
        await DB.put_sync_state_async(aid, {
            "status": "REAUTH_REQUIRED",
            "is_stale": True,
            "last_error": f"Broker session expired: {se}. Please re-authorize account.",
        })
        conn_st = CONN_MGR.get_account_state(aid)
        conn_st.status = "REAUTH_REQUIRED"
        raise SessionExpired(str(se))

    except Exception as e:
        # Broker API temporarily unavailable: retain last data and mark as STALE (Req 1.9, 1.10, 1.11)
        log.error(f"Broker sync failure for account {aid}: {e}")
        cur_sync = (await DB.get_sync_state_async(aid)) or {}
        err_cnt = int(cur_sync.get("error_count") or 0) + 1
        consec = int(cur_sync.get("consecutive_failures") or 0) + 1
        await DB.put_sync_state_async(aid, {
            "status": "STALE",
            "is_stale": True,
            "last_error": str(e)[:300],
            "error_count": err_cnt,
            "consecutive_failures": consec,
        })
        await DB.put_token_async(aid, {
            "sync_message": f"STALE — sync error at {now.strftime('%H:%M:%S')} IST: {str(e)[:180]}",
        })
        raise

    today_str = now.date().isoformat()

    # Safety guard: outside market hours (before 09:15 or after 15:45 IST), if Zebu's nightly reset
    # returns empty TradeBook and empty PositionBook, NEVER overwrite or delete today's stored trades.
    in_market_window = (9, 15) <= (now.hour, now.minute) <= (15, 45)
    if not fills and not positions and not in_market_window:
        await DB.put_sync_state_async(aid, {
            "status": "LIVE",
            "is_stale": False,
            "last_sync_success": now_utc,
            "last_error": None,
            "consecutive_failures": 0,
            "circuit_open": False,
        })
        return "ok · after hours (retained existing trades)"

    # 1. Enrich orders with SingleOrdHist for modified/rejected/partially-filled orders & persist Orders
    if orders:
        hist_candidates = [
            o for o in orders
            if o.get("norenordno")
            and f"{aid}|{o.get('norenordno')}" not in _order_hist_cache
            and (
                str(o.get("status", "")).upper() in ("REJECTED", "CANCELED", "REJECT", "OPEN")
                or (float(o.get("fillshares") or 0) > 0 and float(o.get("fillshares") or 0) < float(o.get("qty") or 0))
            )
        ][:5]
        for ho in hist_candidates:
            ord_no = str(ho.get("norenordno"))
            cache_k = f"{aid}|{ord_no}"
            try:
                hist = await asyncio.wait_for(
                    ZEBU.single_order_history_async(tok, uid, ord_no, proxy=proxy_url),
                    timeout=3.0,
                )
                if isinstance(hist, list) and hist:
                    _order_hist_cache[cache_k] = hist
                    ho["order_history"] = hist
            except Exception as e:
                log.debug(f"SingleOrdHist lookup skipped for {ord_no}: {e}")
        for o in orders:
            ord_no = str(o.get("norenordno") or "")
            if ord_no and f"{aid}|{ord_no}" in _order_hist_cache and "order_history" not in o:
                o["order_history"] = _order_hist_cache[f"{aid}|{ord_no}"]
        await DB.put_many_orders_async(aid, orders)

    # 2. Atomically replace current PositionBook snapshot (handling valid empty responses during market hours)
    #    and subscribe open position tokens to real-time WebSocket Touchline feed.
    try:
        await DB.replace_account_positions_snapshot_async(
            account_id=aid,
            positions=positions,
            expected_uid=uid,
            expected_actid=actid,
            allow_empty=in_market_window,
            snapshot_time=now_utc,
        )
    except ValueError as pve:
        log.error(f"PositionBook snapshot validation failed for {aid} (retaining previous snapshot): {pve}")
        await DB.put_sync_state_async(aid, {"status": "STALE", "is_stale": True, "last_error": str(pve)[:300]})
    if positions:
        open_scrips = [
            f"{p.get('exch', 'NFO')}|{p.get('token')}"
            for p in positions
            if float(p.get("netqty") or 0) != 0 and p.get("token")
        ]
        if open_scrips:
            await CONN_MGR.subscribe_account_touchline(aid, open_scrips)

    # 3. Persist genuine TradeBook fills only (atomically replace today's fills when TradeBook is non-empty so duplicates can never accumulate)
    should_unlock = reconcile_today and bool(fills or positions)
    await DB.purge_synthetic_fills_and_pb_trades_async(aid, today_str, unlock_today=should_unlock)

    fill_items = []
    seen_fids = set()
    for f in fills:
        ord_no = str(f.get("norenordno") or "")
        fid = f"{aid}|{ord_no}|{f.get('flid') or f.get('fltm')}"
        if fid in seen_fids:
            continue
        seen_fids.add(fid)
        ft = engine.parse_ts(f.get("fltm"), f.get("exch_tm"), f.get("norentm"))
        f_day = ft.date().isoformat() if ft else today_str
        fill_items.append((fid, {**f, "_acc": aid, "_day": f_day}))

    if fill_items:
        today_fill_items = [it for it in fill_items if it[1].get("_day") == today_str]
        other_fill_items = [it for it in fill_items if it[1].get("_day") != today_str]
        if today_fill_items:
            await DB.replace_day_fills_async(aid, today_str, today_fill_items)
        if other_fill_items:
            await DB._put_many_fills(other_fill_items)

    # 4. Build trades from account fills while strictly preserving finalized historical trades
    ltp = {}
    for p in positions:
        if p.get("tsym") and p.get("lp") is not None and p.get("lp") != "":
            ltp[p["tsym"]] = p["lp"]

    cutoff = (now.date() - timedelta(days=CFG.REBUILD_LOOKBACK_DAYS)).isoformat()
    acc_fills = await DB._all_fills(account_id=aid)
    recent_fills = [f for f in acc_fills if f.get("_day", "") >= cutoff]

    current_trades = await DB._all_trades()
    old_trades = {t["id"]: t for t in current_trades if t.get("account") == aid and t.get("origin") == "zebu"}
    finalized_ids = {tid for tid, t in old_trades.items() if t.get("is_finalized")}

    charges_cfg = {
        "brokerage_per_order": CFG.CHG_BROKERAGE_PER_ORDER,
        "stt_sell_pct": CFG.CHG_STT_SELL_PCT,
        "txn_pct": CFG.CHG_TXN_PCT,
        "stamp_buy_pct": CFG.CHG_STAMP_BUY_PCT,
    }
    # Reconcile open and active positions with PositionBook (authoritative broker LTP, rpnl, and urmtom)
    product_map = json.loads(CFG.PRODUCT_MAP)
    built = engine.build_trades(
        acc,
        recent_fills,
        charges_cfg,
        product_map,
        is_algo_order,
        now,
        ltp,
        positions=positions,
    )

    for p in positions:
        tsym = p.get("tsym")
        if not tsym:
            continue
        day_buy_q = float(p.get("daybuyqty") or 0)
        day_sell_q = float(p.get("daysellqty") or 0)
        cf_buy_q = float(p.get("cfbuyqty") or 0)
        cf_sell_q = float(p.get("cfsellqty") or 0)
        rpnl = float(p.get("rpnl") or 0)
        urmtom_raw = p.get("urmtom")
        urmtom = float(urmtom_raw or 0)
        m2m = float(p.get("m2m") or 0)
        net_qty = float(p.get("netqty") or 0)
        lp_raw = p.get("lp")
        lp = float(lp_raw or 0) if (lp_raw is not None and lp_raw != "") else None
        net_pos_pnl = rpnl + urmtom
        if net_pos_pnl == 0 and m2m != 0:
            net_pos_pnl = m2m

        sym_trades_today = [
            t for t in built
            if t.get("tsym") == tsym and t["id"] not in finalized_ids and (t.get("open") or t.get("date") == today_str or t.get("exitDate") == today_str)
        ]

        if sym_trades_today:
            if net_qty != 0:
                open_trs = [t for t in sym_trades_today if t.get("open")]
                if open_trs:
                    # Keep TradeBook FIFO closed legs untouched; assign open position unrealized MTM
                    # directly from Zebu PositionBook urmtom so MTM is never mixed with realized P&L or double-counted.
                    has_broker_urmtom = (urmtom_raw is not None and urmtom_raw != "")
                    if has_broker_urmtom:
                        target_open_total = round(urmtom, 2)
                        tot_open_qty = sum(float(otr.get("qty") or (otr.get("lots", 1) * otr.get("lotSize", 1))) for otr in open_trs) or 1.0
                        assigned = 0.0
                        for idx_o, otr in enumerate(open_trs):
                            if idx_o == len(open_trs) - 1:
                                share = round(target_open_total - assigned, 2)
                            else:
                                oq = float(otr.get("qty") or (otr.get("lots", 1) * otr.get("lotSize", 1)))
                                share = round(target_open_total * (oq / tot_open_qty), 2)
                                assigned = round(assigned + share, 2)
                            otr["pnl"] = share
                            otr["unrealized_pnl"] = share
                            otr["net_pnl"] = round(share - float(otr.get("charges") or 0), 2)
                            otr["pnl_source"] = "broker_urmtom"
                            if lp is not None and lp > 0:
                                otr["exit"] = round(lp, 2)
                    elif lp is not None and lp > 0:
                        for otr in open_trs:
                            otr["exit"] = round(lp, 2)
                            t_dir = -1 if otr.get("side") == "SELL" else 1
                            t_qty = float(otr.get("qty") or (otr.get("lots", 1) * otr.get("lotSize", 1)))
                            calc_unrl = round((lp - float(otr["entry"])) * t_qty * t_dir, 2)
                            otr["pnl"] = calc_unrl
                            otr["unrealized_pnl"] = calc_unrl
                            otr["net_pnl"] = round(calc_unrl - float(otr.get("charges") or 0), 2)
                            otr["pnl_source"] = "broker_ltp"
            # Note: For closed legs (net_qty == 0 or partial closes), preserve genuine TradeBook FIFO execution P&L
            # on individual trade rows. Authoritative broker rpnl is exposed separately from PositionBook without
            # fabricating or arbitrarily redistributing differences onto individual trades.
        elif day_buy_q > 0 or day_sell_q > 0 or cf_buy_q > 0 or cf_sell_q > 0 or rpnl != 0 or net_qty != 0 or urmtom != 0 or m2m != 0:
            # Position exists in Zebu PositionBook (e.g. carry-forward position or position whose entry fill wasn't in local fills)
            is_pos_closed = (net_qty == 0)
            if is_pos_closed and (now.hour < 9 or (now.hour == 9 and now.minute < 15)):
                continue

            pos_tid = f"Z-POS-{aid}-{tsym}"
            if pos_tid in finalized_ids:
                continue

            parsed = engine.parse_instrument(tsym) or {}
            ls = int(float(p.get("ls") or 1)) or 1
            qty = abs(net_qty) if net_qty != 0 else max(day_buy_q, day_sell_q, cf_buy_q, cf_sell_q)
            if qty <= 0:
                continue
            lots = round(qty / ls, 2)
            is_open = (net_qty != 0)

            if is_open:
                is_buy = net_qty > 0
            elif (day_buy_q + cf_buy_q) != (day_sell_q + cf_sell_q):
                is_buy = (day_buy_q + cf_buy_q) >= (day_sell_q + cf_sell_q)
            else:
                is_buy = net_pos_pnl >= 0
            side_str = "BUY" if is_buy else "SELL"
            t_dir = 1 if is_buy else -1

            raw_entry = (
                p.get("netavgprc")
                or p.get("totbuyavgprc" if is_buy else "totsellavgprc")
                or p.get("daybuyavgprc" if is_buy else "daysellavgprc")
                or p.get("cfbuyavgprc" if is_buy else "cfsellavgprc")
                or p.get("openprc")
                or lp
            )
            if raw_entry is None or str(raw_entry).strip() == "" or float(raw_entry or 0) <= 0:
                continue
            entry_px = round(float(raw_entry), 2)
            if is_open and lp is not None and lp > 0:
                exit_px = round(lp, 2)
            else:
                exit_px = round(entry_px + (net_pos_pnl / qty) * t_dir, 2)

            prd_code = p.get("prd", "MIS")
            norm_prd = product_map.get(prd_code, prd_code or "MIS")
            norm_prd = "NRML" if norm_prd in ("NRML", "CNC") else "MIS"
            est_chg = round(CFG.CHG_BROKERAGE_PER_ORDER * (int((day_buy_q + cf_buy_q) > 0) + int((day_sell_q + cf_sell_q) > 0)), 2)
            pnl_val = round(urmtom if is_open else net_pos_pnl, 2)

            prev_pos = old_trades.get(pos_tid)
            entry_date_str = prev_pos["date"] if (prev_pos and prev_pos.get("date")) else today_str
            entry_time_str = prev_pos["time"] if (prev_pos and prev_pos.get("time")) else "09:15"

            built.append({
                "id": pos_tid,
                "parent_trade_id": pos_tid,
                "origin": "zebu",
                "date": entry_date_str,
                "time": entry_time_str,
                "trader": acc.get("trader") or acc.get("zebu_uid") or aid,
                "account": aid,
                "symbol": parsed.get("underlying") or tsym,
                "strike": parsed.get("strike") or 0,
                "opt": parsed.get("opt") or "",
                "expiry": parsed.get("expiry").isoformat() if parsed.get("expiry") else "",
                "expiryType": parsed.get("expiryType", ""),
                "tsym": tsym,
                "exch": p.get("exch", "NFO"),
                "side": side_str,
                "lots": lots,
                "lotSize": ls,
                "qty": qty,
                "entry": entry_px,
                "exit": exit_px,
                "pnl": pnl_val,
                "realized_pnl": 0.0 if is_open else pnl_val,
                "unrealized_pnl": pnl_val if is_open else 0.0,
                "net_pnl": round(pnl_val - est_chg, 2),
                "pnl_source": "broker_position_book",
                "open": is_open,
                "exitDate": None if is_open else today_str,
                "exitTime": "OPEN" if is_open else now.strftime("%H:%M"),
                "spot": prev_pos.get("spot") if prev_pos else None,
                "product": norm_prd,
                "source": "Manual",
                "charges": est_chg,
                "chargesEstimated": True,
                "hedged": False,
            })

    # Remove ghost open positions for contracts that have already expired (< today_str) or have netqty == 0 in PositionBook
    pos_net_by_sym = {}
    for p in positions:
        tsym_p = p.get("tsym")
        if tsym_p:
            pos_net_by_sym[tsym_p] = pos_net_by_sym.get(tsym_p, 0.0) + float(p.get("netqty") or 0)
    built = [
        t for t in built
        if not (
            t.get("open")
            and (
                (t.get("expiry") and str(t["expiry"]) < today_str)
                or (t.get("tsym") in pos_net_by_sym and pos_net_by_sym[t["tsym"]] == 0.0)
            )
        )
    ]

    # Preserve last known exit/LTP/P&L on open trades if broker PositionBook temporarily omits lp
    for t in built:
        if t.get("open") and t.get("pnl") is None:
            prev = old_trades.get(t["id"])
            if prev and prev.get("pnl") is not None:
                t["exit"] = prev.get("exit", t.get("exit"))
                t["pnl"] = prev.get("pnl")
                t["unrealized_pnl"] = prev.get("unrealized_pnl", prev.get("pnl"))
                t["net_pnl"] = prev.get("net_pnl")
                t["pnl_source"] = prev.get("pnl_source", "retained_ltp")

    # Resolve spot price at entry (bounded timeout so slow candle API never blocks trade sync)
    for t in built:
        if t["id"] in finalized_ids:
            continue
        prev = old_trades.get(t["id"])
        if prev and prev.get("spot"):
            t["spot"] = prev["spot"]
        else:
            ts_val = engine.parse_ts(t.get("_ts")) or now
            try:
                t["spot"] = await asyncio.wait_for(spot_at_async(t["symbol"], ts_val), timeout=2.5)
            except Exception:
                t["spot"] = None
        t.pop("_ts", None)

    # Delete only non-finalized trades from the current rebuild window that are no longer present in `built`
    built_ids = {t["id"] for t in built}
    for tid, t in old_trades.items():
        if tid in finalized_ids:
            continue
        if tid not in built_ids:
            if t.get("date", "") >= cutoff or t.get("open") or (t.get("exitDate") and t.get("exitDate") >= cutoff):
                await DB._delete_trade(tid)

    # Upsert only non-finalized trades
    to_save = [(t["id"], t) for t in built if t["id"] not in finalized_ids]
    if to_save:
        await DB._put_many_trades(to_save, allow_overwrite_finalized=False)

    # Update sync state & session timestamps (Req 1.9, 1.10, 1.11)
    today_fills_cnt = sum(1 for _, f in fill_items if f.get("_day") == today_str)
    today_trades_cnt = sum(1 for t in built if t.get("date") == today_str or t.get("exitDate") == today_str or t.get("open"))
    await DB.put_sync_state_async(aid, {
        "status": "LIVE",
        "is_stale": False,
        "last_sync_success": now_utc,
        "last_error": None,
        "consecutive_failures": 0,
        "circuit_open": False,
        "total_fills_today": today_fills_cnt,
        "total_trades_today": today_trades_cnt,
    })
    await DB.put_token_async(aid, {
        "status": "Linked",
        "last_sync_at": now_utc,
        "sync_message": f"ok · {today_fills_cnt} fills today · {len(built)} trades active/rebuilt",
    })

    # Continuous rule evaluation & persistent violation logging for this account (Req 7.5, 7.6)
    await evaluate_and_record_violations_async(now, account_id=aid)

    # Broadcast updates to dashboard websockets
    asyncio.create_task(WS_HUB.broadcast({
        "type": "trades_updated",
        "account": aid,
        "last_sync_at": now.isoformat(timespec="seconds"),
    }))

    return f"ok · {today_fills_cnt} fills today · {len(built)} trades rebuilt"


async def poll_all_async(reason: str = "schedule", force: bool = False) -> dict:
    """Executes asynchronous concurrent polling across all connected accounts using ConnectionManager."""
    now = now_ist()
    if not force and now.hour >= 21:
        return {"status": "locked · 9:00 PM IST cutoff (no Zebu fetch until 12:00 AM IST)"}
    accounts = await DB.get_all_accounts_async()
    results = {}

    async def _sync_one(acc_dict):
        async def _sync_single(a=acc_dict):
            return await poll_account_async(a, now, reconcile_today=(reason == "manual"), force=force)
        res = await CONN_MGR.sync_account_bounded(acc_dict, _sync_single)
        return acc_dict["id"], res

    tasks = [
        _sync_one(acc)
        for acc in accounts
        if acc.get("broker", "Zebu") == "Zebu" and acc.get("zebu_uid")
    ]
    if tasks:
        synced_results = await asyncio.gather(*tasks, return_exceptions=True)
        for item in synced_results:
            if isinstance(item, tuple):
                results[item[0]] = item[1]
            elif isinstance(item, Exception):
                log.error(f"Error during parallel poll: {item}")

    await DB._put_status("poll", {"last": now.isoformat(timespec="seconds"), "reason": reason, "results": results})
    await evaluate_and_record_violations_async(now)
    await notify_expired_async(now, results)
    return results


# ---------------- End-of-Day Finalization & Midnight Archiving ----------------

async def finalize_eod_async(now: Optional[datetime] = None, target_date: Optional[str] = None) -> Dict[str, Any]:
    """Performs End-of-Day (after 03:30 PM IST) final synchronization, reconciliation, and permanent finalization.

    Idempotent: running multiple times never creates duplicate trades or duplicate P&L records,
    and never overwrites already finalized historical records.
    """
    now = now or now_ist()
    day_str = target_date or now.date().isoformat()
    now_utc = datetime.now(timezone.utc)
    accounts = await DB.get_all_accounts_async()
    finalized_count = 0
    pending_count = 0
    account_reports = {}

    for acc in accounts:
        aid = acc["id"]
        tid = acc.get("trader") or aid
        rep_id = f"{aid}|{day_str}"

        # Check if already finalized
        existing_reps = await DB.get_daily_reports_async(account_id=aid)
        existing_for_day = next((r for r in existing_reps if r["account"] == aid and r["date"] == day_str), None)
        if existing_for_day and existing_for_day.get("status") in ("FINALIZED", "ARCHIVED"):
            account_reports[aid] = existing_for_day["status"]
            finalized_count += 1
            continue

        # 1. Perform final synchronization if account is a linked Zebu account, target_date is today, and before 9:00 PM IST cutoff
        sync_ok = True
        sync_note = "verified_from_database"
        tok = await token_for_account_async(aid)
        if acc.get("broker", "Zebu") == "Zebu" and acc.get("zebu_uid") and tok and day_str == now.date().isoformat() and now.hour < 21:
            try:
                sync_res = await poll_account_async(acc, now)
                sync_note = f"zebu_eod_sync: {sync_res}"
            except Exception as e:
                sync_ok = False
                sync_note = f"EOD broker sync failed: {e}"
                log.warning(f"EOD finalization sync failed for {aid} on {day_str}: {e}")

        # 2. Check for incomplete or pending orders if syncing today
        orders = await DB.get_orders_async(account_id=aid)
        day_orders = [o for o in orders if (o.get("order_time") or "").startswith(day_str)]
        pending_orders = [o for o in day_orders if o.get("status") in ("OPEN", "TRIGGER PENDING", "PENDING")]

        if not sync_ok:
            # Req 2.9: If final broker data is unavailable, do not finalize incomplete information as accurate.
            # Mark report as PENDING_RECONCILIATION and retry safely.
            await DB.put_daily_report_async(rep_id, {
                "account": aid,
                "trader": tid,
                "date": day_str,
                "status": "PENDING_RECONCILIATION",
                "source_info": "zebu_eod_reconciliation",
                "reconciliation_notes": sync_note,
            })
            pending_count += 1
            account_reports[aid] = "PENDING_RECONCILIATION"
            continue

        # 3. Reconcile all trades for this account and trading date
        all_trades = await DB._all_trades()
        acc_trades = [t for t in all_trades if t.get("account") == aid]

        # Closed trades whose exitDate is day_str (or date == day_str if exitDate missing)
        closed_today = [
            t for t in acc_trades
            if not t.get("open") and ((t.get("exitDate") or t.get("date")) == day_str)
        ]
        # Open positions still open at EOD that were entered on or before day_str
        open_eod = [
            t for t in acc_trades
            if t.get("open") and (t.get("date") or "") <= day_str
        ]
        # Identify partially closed trades (closed legs that share parent_trade_id with an open leg or multiple exit legs)
        open_parents = {t.get("parent_trade_id") or t["id"] for t in open_eod}
        partial_closed = [t for t in closed_today if (t.get("parent_trade_id") in open_parents) or ("-X-" in t["id"])]

        # 4. Lock all closed trades for day_str as finalized (Req 2.6, 2.7)
        realized_dec = Decimal("0")
        charges_dec = Decimal("0")
        net_dec = Decimal("0")
        wins = 0
        losses = 0
        finalized_trade_details = []

        for ct in closed_today:
            g_val = engine._dec(engine.gross_pnl(ct), Decimal("0"))
            c_val = engine._dec(ct.get("charges"), Decimal("0"))
            n_val = engine._q2(g_val - c_val)
            realized_dec += g_val
            charges_dec += c_val
            net_dec += n_val
            if g_val > Decimal("0"):
                wins += 1
            else:
                losses += 1

            ct["realized_pnl"] = float(engine._q2(g_val))
            ct["pnl"] = float(engine._q2(g_val))
            ct["net_pnl"] = float(n_val)
            ct["is_finalized"] = True
            ct["finalized_at"] = now_utc
            await DB._put_trade(ct["id"], ct, allow_overwrite_finalized=True)

            finalized_trade_details.append({
                "id": ct["id"],
                "parent_trade_id": ct.get("parent_trade_id") or ct["id"],
                "account": aid,
                "trader": tid,
                "date": ct.get("date"),
                "time": ct.get("time"),
                "exitDate": ct.get("exitDate") or ct.get("date"),
                "exitTime": ct.get("exitTime"),
                "symbol": ct.get("symbol"),
                "tsym": ct.get("tsym"),
                "strike": ct.get("strike"),
                "opt": ct.get("opt"),
                "side": ct.get("side"),
                "lots": ct.get("lots"),
                "lotSize": ct.get("lotSize"),
                "qty": ct.get("qty"),
                "entry": ct.get("entry"),
                "exit": ct.get("exit"),
                "realized_pnl": float(engine._q2(g_val)),
                "charges": float(engine._q2(c_val)),
                "net_pnl": float(n_val),
                "product": ct.get("product"),
                "source": ct.get("source"),
            })

        unrealized_dec = sum(
            (engine._dec(t.get("pnl"), Decimal("0")) for t in open_eod if t.get("pnl") is not None),
            Decimal("0"),
        )

        notes = sync_note
        if pending_orders:
            notes += f" | {len(pending_orders)} pending orders noted at close"

        await DB.put_daily_report_async(
            rep_id,
            {
                "account": aid,
                "trader": tid,
                "date": day_str,
                "status": "FINALIZED",
                "realized_pnl": float(engine._q2(realized_dec)),
                "unrealized_pnl": float(engine._q2(unrealized_dec)),
                "charges": float(engine._q2(charges_dec)),
                "net_pnl": float(engine._q2(net_dec)),
                "total_trades": len(closed_today) + len(open_eod),
                "closed_trades": len(closed_today),
                "open_trades": len(open_eod),
                "partially_closed_trades": len(partial_closed),
                "winning_trades": wins,
                "losing_trades": losses,
                "finalized_at": now_utc,
                "source_info": "zebu_eod_reconciliation",
                "reconciliation_notes": notes,
                "trade_details": finalized_trade_details,
            },
            allow_overwrite_finalized=False,
        )
        finalized_count += 1
        account_reports[aid] = "FINALIZED"

    log.info(f"EOD Finalization complete for {day_str}: {finalized_count} finalized, {pending_count} pending reconciliation")
    return {
        "date": day_str,
        "finalized_accounts": finalized_count,
        "pending_accounts": pending_count,
        "accounts": account_reports,
    }


async def midnight_archive_async(now: Optional[datetime] = None) -> Dict[str, Any]:
    """At/after 12:00 AM IST:
    1. Identifies all closed trades belonging to previous trading days (< today_str).
    2. Finalizes any past trading day that missed EOD finalization (e.g. server slept/restarted).
    3. Marks previous days' closed trades as archived (`is_archived = True`) so they are removed from the active Today Trader Report while permanently preserved in the database and Daily P&L Calendar.
    4. Keeps overnight open positions (`is_open == True`) active and unarchived for the new trading day.
    """
    now = now or now_ist()
    today_str = now.date().isoformat()
    now_utc = datetime.now(timezone.utc)

    all_trades = await DB._all_trades()
    past_dates = sorted({
        (t.get("exitDate") or t.get("date"))
        for t in all_trades
        if not t.get("open") and (t.get("exitDate") or t.get("date")) and (t.get("exitDate") or t.get("date")) < today_str
    })

    # Ensure every past trading date has a finalized daily report before archiving
    for d_str in past_dates:
        await finalize_eod_async(now=now, target_date=d_str)

    archived_trades_count = 0
    active_overnight_count = 0

    async with AsyncSessionLocal() as session:
        stmt = select(Trade)
        rows = (await session.execute(stmt)).scalars().all()
        for tr in rows:
            if tr.is_open:
                active_overnight_count += 1
                continue
            ex_d = tr.exit_date or tr.trade_date
            if ex_d and ex_d < today_str:
                if not getattr(tr, "is_finalized", False):
                    tr.is_finalized = True
                    tr.finalized_at = now_utc
                if not getattr(tr, "is_archived", False):
                    tr.is_archived = True
                    tr.archived_at = now_utc
                    archived_trades_count += 1

        # Transition finalized DailyAccountReport rows for dates < today_str to ARCHIVED
        rep_stmt = select(DailyAccountReport).where(
            DailyAccountReport.trade_date < today_str,
            DailyAccountReport.status == "FINALIZED",
        )
        rep_rows = (await session.execute(rep_stmt)).scalars().all()
        for rep in rep_rows:
            rep.status = "ARCHIVED"
            rep.archived_at = now_utc

        await session.commit()

    # Update historical rule violations from past days to HISTORICAL status
    async with AsyncSessionLocal() as session:
        alt_stmt = select(Alert).where(
            Alert.trade_date.isnot(None),
            Alert.trade_date < today_str,
            Alert.violation_status == "ACTIVE",
        )
        alt_rows = (await session.execute(alt_stmt)).scalars().all()
        for alt in alt_rows:
            alt.violation_status = "HISTORICAL"
        await session.commit()

    log.info(
        f"Midnight archiving completed for dates < {today_str}: "
        f"{archived_trades_count} closed trades archived, {active_overnight_count} overnight open positions retained active."
    )
    return {
        "today": today_str,
        "archived_trades": archived_trades_count,
        "active_overnight_positions": active_overnight_count,
        "processed_dates": past_dates,
    }


async def reconcile_daily_calendar_async(account_id: Optional[str] = None) -> Dict[str, Any]:
    """Audits and verifies that DailyAccountReport totals match underlying finalized closed trades (Req 6.10, 6.14)."""
    all_trades = await DB._all_trades()
    reports = await DB.get_daily_reports_async(account_id=account_id)
    mismatches = []
    verified_days = 0

    for rep in reports:
        if rep["status"] not in ("FINALIZED", "ARCHIVED"):
            continue
        aid = rep["account"]
        d_str = rep["date"]
        day_closed = [
            t for t in all_trades
            if t.get("account") == aid and not t.get("open") and (t.get("exitDate") or t.get("date")) == d_str
        ]
        calc_realized = float(engine._q2(sum((engine._dec(engine.gross_pnl(t)) for t in day_closed), Decimal("0"))))
        calc_charges = float(engine._q2(sum((engine._dec(t.get("charges")) for t in day_closed), Decimal("0"))))
        calc_net = float(engine._q2(Decimal(str(calc_realized)) - Decimal(str(calc_charges))))

        if abs(calc_realized - rep["realized_pnl"]) > 0.01 or abs(calc_net - rep["net_pnl"]) > 0.01 or len(day_closed) != rep["closed_trades"]:
            mismatches.append({
                "account": aid,
                "date": d_str,
                "report_realized_pnl": rep["realized_pnl"],
                "trades_realized_pnl": calc_realized,
                "report_net_pnl": rep["net_pnl"],
                "trades_net_pnl": calc_net,
                "report_closed_trades": rep["closed_trades"],
                "actual_closed_trades": len(day_closed),
            })
        else:
            verified_days += 1

    return {
        "ok": len(mismatches) == 0,
        "verified_days": verified_days,
        "mismatches": mismatches,
    }


# ---------------- Rule Evaluation & Alerting ----------------

async def all_flags_async() -> Dict[str, List[dict]]:
    rules_map = await get_rules_map_async()
    trades = await DB._all_trades()
    today_str = now_ist().date().isoformat()
    return engine.check_trades(trades, rules_map, today_str=today_str)


async def get_rules_map_async() -> Dict[str, dict]:
    all_rules = await DB._all_rules()
    res = {r["trader"]: {**engine.DEFAULT_RULES, **r["rules"]} for r in all_rules}
    for r in all_rules:
        if r.get("account") and r["account"] not in res:
            res[r["account"]] = {**engine.DEFAULT_RULES, **r["rules"]}
    accounts = await DB.get_all_accounts_async()
    for acc in accounts:
        aid = acc["id"]
        uid = acc.get("zebu_uid")
        tid = acc.get("trader")
        # If an account has no specific rule row yet, inherit from its assigned trader or DEFAULT_RULES
        base_r = res.get(aid) or (res.get(uid) if uid else None) or (res.get(tid) if tid else None) or engine.DEFAULT_RULES
        if aid not in res:
            res[aid] = {**engine.DEFAULT_RULES, **base_r}
        if uid and uid not in res:
            res[uid] = {**engine.DEFAULT_RULES, **base_r}
    return res


async def evaluate_and_record_violations_async(now: datetime, account_id: Optional[str] = None):
    """Evaluates rules across active/historical trades, persists structured Red Flags in DB, and dispatches alerts."""
    t0 = time.perf_counter()
    today = now.date().isoformat()
    s = (await DB._get_setting("main")) or {}
    alerts_cfg = s.get("alerts") or DEFAULT_SETTINGS["alerts"]

    all_trades = await DB._all_trades()
    if account_id:
        all_trades = [t for t in all_trades if t.get("account") == account_id or t.get("trader") == account_id]
    trades_by_id = {t["id"]: t for t in all_trades}

    all_traders = await DB._all_traders()
    traders_by_id = {t["id"]: t for t in all_traders}
    rules_map = await get_rules_map_async()

    flags_dict = engine.check_trades(all_trades, rules_map, today_str=today)

    for tid, fl in flags_dict.items():
        t = trades_by_id.get(tid)
        if not t:
            continue
        for f in fl:
            dedup_key = f"VIOL|{t.get('account')}|{tid}|{f['rule']}"
            v_status = f.get("status") or ("HISTORICAL" if t["date"] < today and not t.get("open") else "ACTIVE")
            name = traders_by_id.get(t.get("trader"), {}).get("name") or t.get("account") or t.get("trader")
            explanation = f"{f['did']} (Rule condition: {f.get('condition', f['rule'])})"

            await DB.upsert_rule_violation_async(dedup_key, {
                "account_id": t.get("account"),
                "trader_id": t.get("trader"),
                "trade_id": tid,
                "order_id": t.get("entry_order_id"),
                "trade_date": t.get("date"),
                "trade_time": t.get("time"),
                "rule_name": f["rule"],
                "configured_condition": f.get("condition", f["rule"]),
                "observed_value": f.get("observed", f["did"]),
                "explanation": f["did"],
                "violation_status": v_status,
                "severity": f["level"],
                "title": f"🚩 {f['rule']} · {name}",
                "message": explanation,
            })

            # External notification (Telegram/WhatsApp/Email) for today's High severity flags
            if alerts_cfg.get("onHigh") and t["date"] == today and f["level"] == "High":
                sent_key = f"{tid}|{f['rule']}"
                if not await DB._get_sent_flag(sent_key):
                    text_msg = (
                        f"🚩 Red flag · {name} · {t['time']}\nRule: {f['rule']}\nWhat he did: {f['did']}\n"
                        f"{t['symbol']} {t.get('strike') or 0:g} {t.get('opt') or ''} · {t['lots']:g} lots · {t['account']}"
                    )
                    await ALERT.send_async(
                        text=text_msg,
                        settings_alerts=alerts_cfg,
                        dedup_key=sent_key,
                        severity="High",
                        metadata={
                            "account_id": t["account"],
                            "trader_id": t["trader"],
                            "trade_id": tid,
                            "rule_name": f["rule"],
                        },
                    )

    METRICS.record_risk_latency((time.perf_counter() - t0) * 1000)


async def notify_expired_async(now: datetime, res: dict):
    today = now.date().isoformat()
    s = (await DB._get_setting("main")) or {}
    alerts_cfg = s.get("alerts") or DEFAULT_SETTINGS["alerts"]
    for acc_id, r in res.items():
        if r == "expired":
            key = f"expired|{acc_id}|{today}"
            if not await DB._get_sent_flag(key):
                await ALERT.send_async(
                    f"⚠️ Zebu login expired for {acc_id}. Open the dashboard → Trader profiles → Link Zebu login.\n{CFG.PUBLIC_URL}",
                    settings_alerts=alerts_cfg,
                    dedup_key=key,
                    severity="High",
                    metadata={"account_id": acc_id},
                )


async def daily_summary_async(now: datetime):
    s = (await DB._get_setting("main")) or {}
    alerts_cfg = s.get("alerts") or DEFAULT_SETTINGS["alerts"]
    today = now.date().isoformat()
    flags = await all_flags_async()
    lines = [f"📊 Daily summary · {today}"]
    all_trades = await DB._all_trades()
    trades = [t for t in all_trades if (t.get("exitDate") or t.get("date")) == today and not t.get("open")]

    all_traders = await DB._all_traders()
    for tr in all_traders:
        mine = [t for t in trades if t.get("trader") == tr["id"] or t.get("account") == tr["id"]]
        if not mine:
            continue
        pnl = sum(engine.net_pnl(t) for t in mine)
        nf = sum(len(flags.get(t["id"], [])) for t in mine)
        clean = sum(1 for t in mine if not flags.get(t["id"]))
        lines.append(f"{tr['name']}: {len(mine)} trades · Net P&L ₹{pnl:,.2f} · {nf} flags · rule-follow {clean / len(mine) * 100:.0f}%")

    total = sum(engine.net_pnl(t) for t in trades)
    lines.append(f"Total: ₹{total:,.2f} · {len(trades)} trades")
    await ALERT.send_async("\n".join(lines), alerts_cfg, severity="Medium")


async def get_settings_dict() -> dict:
    s = (await DB._get_setting("main")) or {}
    merged = {**DEFAULT_SETTINGS, **s}
    merged["pass"] = {**DEFAULT_SETTINGS["pass"], **(s.get("pass") or {})}
    merged["alerts"] = {**DEFAULT_SETTINGS["alerts"], **(s.get("alerts") or {})}
    return merged


SCHEDULER = MarketScheduler(
    poll_fn=lambda: poll_all_async("schedule"),
    summary_fn=daily_summary_async,
    get_settings_fn=get_settings_dict,
    finalize_eod_fn=finalize_eod_async,
    midnight_archive_fn=midnight_archive_async,
)


# ---------------- Lifespan & App Setup ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Initialize DB tables and schema migrations
    await init_models()

    # 2. Ensure initial admin exists
    admin_u = CFG.ADMIN_USER
    admin_existing = await DB.get_user_async(admin_u)
    if not admin_existing:
        await DB.put_user_async(admin_u, {"user": admin_u, "role": "admin", "pw": hash_pw(CFG.ADMIN_PASSWORD)})

    settings_existing = await DB._get_setting("main")
    if not settings_existing:
        await DB._put_setting("main", DEFAULT_SETTINGS)

    # 3. Run startup recovery for midnight archiving and past unfinalized trading days (Req 3.8, 3.9, 9.6)
    try:
        await midnight_archive_async(now_ist())
    except Exception as e:
        log.error(f"Startup recovery notice in lifespan: {e}")

    # 4. Start Connection Manager & Scheduler if configured for in-process execution
    if CFG.RUN_WORKER_IN_PROCESS:
        await CONN_MGR.start()
        all_accounts = await DB.get_all_accounts_async()
        for acc in all_accounts:
            tok = await token_for_account_async(acc["id"])
            if tok and acc.get("zebu_uid"):
                await _ensure_account_ws_stream(acc, tok)
        now_s = now_ist()
        if (9, 15) <= (now_s.hour, now_s.minute) <= (15, 45):
            asyncio.create_task(poll_all_async("startup"))

    if CFG.RUN_SCHEDULER_IN_PROCESS and not CFG.DISABLE_SCHEDULER:
        await SCHEDULER.start()

    log.info("Trader Tracking System startup complete.")
    yield

    # Shutdown
    if CFG.RUN_SCHEDULER_IN_PROCESS and not CFG.DISABLE_SCHEDULER:
        await SCHEDULER.stop()
    if CFG.RUN_WORKER_IN_PROCESS:
        await CONN_MGR.stop()
    await ZEBU.aclose()
    await ALERT.aclose()
    log.info("Trader Tracking System shutdown complete.")


app = FastAPI(title="Trader Tracking System (TTS)", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CFG.get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    if CFG.PUBLIC_URL.startswith("https"):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# Rate limiter for login attempts
_login_rate_limits: Dict[str, List[float]] = {}


def check_login_rate_limit(client_ip: str):
    now = time.time()
    attempts = _login_rate_limits.setdefault(client_ip, [])
    _login_rate_limits[client_ip] = [t for t in attempts if t > now - 60]
    if len(_login_rate_limits[client_ip]) >= CFG.RATE_LIMIT_LOGIN_PER_MINUTE:
        raise HTTPException(429, "Too many login attempts. Please try again in 1 minute.")
    _login_rate_limits[client_ip].append(now)


# ---------------- Auth Dependencies ----------------
async def current_user(request: Request):
    h = request.headers.get("authorization", "")
    tok = h[7:] if h.lower().startswith("bearer ") else ""
    s = (await DB.get_session_async(tok)) if tok else None
    if not s or s.get("exp", 0) < time.time():
        raise HTTPException(401, "Please sign in")
    return s


def admin_only(user=Depends(current_user)):
    if user["role"] != "admin":
        raise HTTPException(403, "Admin only")
    return user


def _link_state_from_doc(t: Optional[dict]) -> str:
    if not t or not t.get("access_token"):
        return "Not linked"
    st = t.get("status") or "Linked"
    if st in ("Login expired", "REAUTH_REQUIRED", "Not linked", "Disconnected"):
        return st
    if t.get("expires_at") and float(t["expires_at"]) < time.time():
        return "Login expired"
    return "Linked"


async def link_state(acc: dict) -> str:
    """Returns true connection status without masking expired or disconnected sessions (Req 8.6, 8.7, 8.8)."""
    t = await DB.get_token_async(acc["id"])
    return _link_state_from_doc(t)


FIN_SUMMARY = FinancialSummaryService(
    zebu=ZEBU,
    conn_mgr=CONN_MGR,
    ws_hub=WS_HUB,
    token_resolver=token_for_account_async,
    link_state_resolver=link_state,
    position_persister=DB.replace_account_positions_snapshot_async,
)


@app.get("/api/overview/financial-summary")
async def get_overview_financial_summary(
    account_id: str,
    force: int = 0,
    user=Depends(current_user),
):
    """Strictly read-only endpoint returning verified Realized P&L (rpnl) and Unrealized MTM (urmtom)
    for the exact selected account from official Zebu PositionBook + live WebSocket Touchline feed.
    """
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(400, "account_id is required")

    acc = await DB.get_account_async(aid)
    if not acc:
        raise HTTPException(404, f"Account '{aid}' not found")

    await _assert_user_can_access_account(user, acc)
    return await FIN_SUMMARY.get_summary(
        acc=acc,
        user_id=user.get("user", "unknown"),
        force=bool(force),
        ensure_ws_cb=_ensure_account_ws_stream if CFG.RUN_WORKER_IN_PROCESS else None,
    )


@app.get("/api/overview/financial-summary/diagnostics")
async def get_overview_financial_summary_diagnostics(_=Depends(admin_only)):
    """Secure server-side diagnostics for financial summary latency, cache hit/miss, and WS status."""
    return FIN_SUMMARY.get_diagnostics()


_FUNDS_CACHE: Dict[str, Dict[str, Any]] = {}
_FUNDS_CACHE_MONO: Dict[str, float] = {}
_FUNDS_LAST_VERIFIED: Dict[str, Dict[str, Any]] = {}
_FUNDS_INFLIGHT: Dict[str, asyncio.Task] = {}
_FUNDS_CACHE_TTL = 15.0


def _parse_limits_decimal(val: Any, required: bool = False) -> Tuple[Optional[Decimal], bool]:
    """Strictly parses a Zebu Limits numeric string into Decimal. Returns (value, is_valid)."""
    if val is None or (isinstance(val, str) and not val.strip()):
        return (None, False) if required else (Decimal("0"), True)
    if isinstance(val, bool):
        return (None, False)
    try:
        d = Decimal(str(val).strip())
        if not d.is_finite():
            return (None, False)
        return (d, True)
    except Exception:
        return (None, False)


def _compute_account_funds_from_limits(limits: Any) -> Dict[str, Any]:
    """Computes Available Funds and Utilized Funds from official Zebu Limits API response.

    Official Zebu / Noren Limits fields:
      - cash:       baseline cash margin available in ledger (required)
      - brkcollamt: pre-valued collateral margin (optional, 0 if omitted)
      - payin:      same-day pay-in credited (optional, 0 if omitted)
      - payout:     same-day payout requested (optional, 0 if omitted)
      - marginused: total margin / funds currently utilized (optional -> 0 if omitted on OK response)

    Formulas (exact Decimal):
      utilized_funds  = marginused
      available_funds = cash + brkcollamt + payin - payout - marginused
    """
    if not isinstance(limits, dict) or str(limits.get("stat", "")).strip().lower() != "ok":
        return {"ok": False, "error": "Invalid Limits response (stat != Ok)"}

    cash, ok_cash = _parse_limits_decimal(limits.get("cash"), required=True)
    brkcollamt, ok_coll = _parse_limits_decimal(limits.get("brkcollamt"), required=False)
    payin, ok_payin = _parse_limits_decimal(limits.get("payin"), required=False)
    payout, ok_payout = _parse_limits_decimal(limits.get("payout"), required=False)
    marginused, ok_mu = _parse_limits_decimal(limits.get("marginused"), required=False)

    if not (ok_cash and ok_coll and ok_payin and ok_payout and ok_mu) or cash is None:
        return {"ok": False, "error": "Missing or invalid numeric fields in Zebu Limits response"}

    utilized = engine._q2(marginused)
    available = engine._q2(cash + brkcollamt + payin - payout - marginused)
    return {
        "ok": True,
        "available_funds": available,
        "utilized_funds": utilized,
        "fields": {
            "cash": str(engine._q2(cash)),
            "brkcollamt": str(engine._q2(brkcollamt)),
            "payin": str(engine._q2(payin)),
            "payout": str(engine._q2(payout)),
            "marginused": str(utilized),
        },
    }


async def _fetch_overview_funds_summary_async(accounts: List[dict], scope_key: str) -> Dict[str, Any]:
    t0 = time.perf_counter()
    # Deduplicate by unique broker account (uid, actid) so shared accounts are fetched & counted exactly once
    unique_accounts: Dict[str, dict] = {}
    for a in accounts:
        if (a.get("broker") or "Zebu") != "Zebu" or not a.get("zebu_uid"):
            continue
        u = ZEBU._clean_uid(a.get("zebu_uid"))
        act = ZEBU._clean_uid(a.get("zebu_actid") or u)
        key = f"{u}|{act}"
        if key not in unique_accounts:
            unique_accounts[key] = a

    linked: List[Tuple[str, dict, str]] = []
    for key, a in unique_accounts.items():
        tok = await token_for_account_async(a["id"])
        if tok:
            linked.append((key, a, tok))

    async def _fetch_one(key: str, acc: dict, tok: str):
        u = ZEBU._clean_uid(acc.get("zebu_uid"))
        act = ZEBU._clean_uid(acc.get("zebu_actid") or u)
        async with CONN_MGR.semaphore:
            try:
                raw = await ZEBU.limits_async(tok, u, act, proxy=acc.get("proxy_url"))
                parsed = _compute_account_funds_from_limits(raw)
                return key, acc, parsed
            except SessionExpired as se:
                return key, acc, {"ok": False, "error": f"Session expired: {se}", "error_type": "SESSION_EXPIRED"}
            except Exception as e:
                return key, acc, {"ok": False, "error": str(e)[:200], "error_type": "BROKER_ERROR"}

    results = await asyncio.gather(*[_fetch_one(k, a, t) for k, a, t in linked])

    total_avail = Decimal("0")
    total_util = Decimal("0")
    succeeded = 0
    failed_accounts = []
    per_account = []

    for key, acc, res in results:
        if res.get("ok"):
            succeeded += 1
            total_avail += res["available_funds"]
            total_util += res["utilized_funds"]
            per_account.append({
                "account_id": acc["id"],
                "account_name": acc.get("name") or acc["id"],
                "broker_account": key,
                "ok": True,
                "available_funds": str(res["available_funds"]),
                "utilized_funds": str(res["utilized_funds"]),
                "fields": res.get("fields"),
            })
        else:
            err_msg = res.get("error") or "Failed to fetch Limits"
            failed_accounts.append(f"{acc.get('name') or acc['id']} ({err_msg})")
            per_account.append({
                "account_id": acc["id"],
                "account_name": acc.get("name") or acc["id"],
                "broker_account": key,
                "ok": False,
                "error": err_msg,
            })

    now_utc = datetime.now(timezone.utc)
    now_ist_str = now_utc.astimezone(IST).strftime("%d %b %Y, %H:%M:%S IST")
    base = {
        "accounts_linked": len(linked),
        "accounts_succeeded": succeeded,
        "accounts_failed": len(failed_accounts),
        "accounts": per_account,
        "last_updated_at": now_utc.isoformat(),
        "last_updated_ist": now_ist_str,
        "latency_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        "read_only": True,
    }

    if not linked:
        return {**base, "status": "UNAVAILABLE", "available_funds": None, "utilized_funds": None, "error": "No linked Zebu accounts"}

    if succeeded == 0:
        prev = _FUNDS_LAST_VERIFIED.get(scope_key)
        err_summary = "All linked accounts failed: " + "; ".join(failed_accounts[:4])
        if prev:
            return {
                **base,
                "status": "STALE",
                "available_funds": prev["available_funds"],
                "utilized_funds": prev["utilized_funds"],
                "last_updated_at": prev["last_updated_at"],
                "last_updated_ist": prev["last_updated_ist"],
                "error": err_summary,
            }
        return {**base, "status": "UNAVAILABLE", "available_funds": None, "utilized_funds": None, "error": err_summary}

    avail_str = str(engine._q2(total_avail))
    util_str = str(engine._q2(total_util))
    if len(failed_accounts) == 0:
        _FUNDS_LAST_VERIFIED[scope_key] = {
            "available_funds": avail_str,
            "utilized_funds": util_str,
            "last_updated_at": base["last_updated_at"],
            "last_updated_ist": now_ist_str,
        }
        return {**base, "status": "LIVE", "available_funds": avail_str, "utilized_funds": util_str, "error": None}

    return {
        **base,
        "status": "PARTIAL",
        "available_funds": avail_str,
        "utilized_funds": util_str,
        "error": f"{len(failed_accounts)} of {len(linked)} account(s) failed: " + "; ".join(failed_accounts[:4]),
    }


@app.get("/api/overview/funds-summary")
async def get_overview_funds_summary(force: int = 0, user=Depends(current_user)):
    """READ-ONLY: Aggregates Total Available Funds and Total Utilized Funds from Zebu Limits API
    across every unique linked broker account authorized for the current user."""
    all_acc = await DB.get_all_accounts_async()
    allowed = []
    for a in all_acc:
        try:
            await _assert_user_can_access_account(user, a)
            allowed.append(a)
        except HTTPException:
            continue

    scope_key = "admin" if user.get("role") == "admin" else f"user:{user.get('user')}"
    now_m = time.monotonic()
    if not force and scope_key in _FUNDS_CACHE and (now_m - _FUNDS_CACHE_MONO.get(scope_key, 0.0)) < _FUNDS_CACHE_TTL:
        return {**_FUNDS_CACHE[scope_key], "cache_hit": True}

    task = _FUNDS_INFLIGHT.get(scope_key)
    if task is None or task.done():
        task = asyncio.create_task(_fetch_overview_funds_summary_async(allowed, scope_key))
        _FUNDS_INFLIGHT[scope_key] = task
    try:
        res = await asyncio.shield(task)
    finally:
        if _FUNDS_INFLIGHT.get(scope_key) is task and task.done():
            _FUNDS_INFLIGHT.pop(scope_key, None)

    _FUNDS_CACHE[scope_key] = res
    _FUNDS_CACHE_MONO[scope_key] = time.monotonic()
    return {**res, "cache_hit": False}


# ---------------- Health & Metrics Endpoints ----------------
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/ready")
async def ready():
    m = await METRICS.get_system_metrics()
    if not m["database"]["healthy"]:
        raise HTTPException(503, "Database unavailable")
    return {"status": "ready", "metrics": m}


@app.get("/api/metrics")
async def get_metrics(user=Depends(admin_only)):
    return await METRICS.get_system_metrics()


@app.get("/api/health")
def api_health():
    return {"ok": True, "mode": "live", "zebu_configured": bool(CFG.ZEBU_CLIENT_ID and CFG.ZEBU_SECRET_KEY)}


# ---------------- Live Dashboard WebSocket ----------------
@app.websocket("/api/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Authenticate WebSocket via TTS user session token (?token=... or Authorization header)
    tok = (websocket.query_params.get("token") or "").strip()
    if not tok:
        auth_hdr = websocket.headers.get("authorization", "")
        if auth_hdr.lower().startswith("bearer "):
            tok = auth_hdr[7:].strip()
    sess = (await DB.get_session_async(tok)) if tok else None
    if not sess or sess.get("exp", 0) < time.time():
        await websocket.close(code=1008, reason="Unauthorized WebSocket session")
        return

    u_doc = await DB.get_user_async(sess.get("user", ""))
    user_scope = {
        "user": sess.get("user"),
        "role": sess.get("role"),
        "trader_id": (u_doc or {}).get("trader_id"),
        "assigned_accounts": (u_doc or {}).get("assigned_accounts") or [],
    }
    await WS_HUB.connect(websocket, user_scope)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        WS_HUB.disconnect(websocket)


@app.get("/api/test-proxy")
async def test_proxy(proxy: str = ""):
    t0 = time.time()
    tcp_results = {}
    http_res = None
    try:
        p = ZEBU._clean_proxy(proxy)
        async with httpx.AsyncClient(proxy=p, timeout=12.0) as cl:
            r = await cl.get("https://api.ipify.org?format=json")
            http_res = {"ok": True, "ip": r.json().get("ip"), "via_proxy": bool(p)}
    except Exception as e:
        http_res = {"ok": False, "error": str(e), "type": type(e).__name__}

    sec_preview = (CFG.ZEBU_SECRET_KEY[:4] + "..." + CFG.ZEBU_SECRET_KEY[-4:]) if len(CFG.ZEBU_SECRET_KEY) >= 8 else "NOT_SET"
    cid_cfg = CFG.ZEBU_CLIENT_ID or "NOT_SET"

    return {
        "tcp": tcp_results,
        "http_client": http_res,
        "server_cfg": {"client_id": cid_cfg, "secret_key": sec_preview},
        "duration": round(time.time() - t0, 2),
    }


@app.get("/api/account-pnl/{aid}")
async def get_raw_account_pnl(aid: str, _=Depends(current_user)):
    acc = await DB.get_account_async(aid)
    if not acc:
        all_acc = await DB.get_all_accounts_async()
        for a in all_acc:
            if a.get("zebu_uid") == aid or a.get("trader") == aid:
                acc = a
                break
    if not acc:
        return {"error": f"Account {aid} not found", "accounts": [a.get("id") for a in await DB.get_all_accounts_async()]}

    tok = await token_for_account_async(acc["id"])
    uid = acc.get("zebu_uid")
    actid = acc.get("zebu_actid") or uid
    proxy_url = acc.get("proxy_url")

    token_info = {"present": bool(tok), "uid": uid, "actid": actid, "link": await link_state(acc)}
    zebu_data = {}
    poll_result = None
    now_cur = now_ist()
    if await _is_day_locked_for_account_async(acc["id"], now_cur):
        zebu_data = {"ok": True, "locked": True, "message": "Market closed — final data locked until 12:00 AM IST"}
        poll_result = "locked · market closed (final data locked until 12:00 AM IST)"
    elif tok and uid:
        try:
            positions = await ZEBU.position_book_async(tok, uid, actid, proxy=proxy_url)
            total_rpnl = sum(float(p.get("rpnl") or 0) for p in positions)
            total_urmtom = sum(float(p.get("urmtom") or 0) for p in positions)
            total_m2m = sum(float(p.get("m2m") or 0) for p in positions)
            total_pnl = total_rpnl + total_urmtom
            if total_pnl == 0 and total_m2m != 0:
                total_pnl = total_m2m
            zebu_data = {
                "ok": True,
                "total_pnl": round(total_pnl, 2),
                "rpnl": round(total_rpnl, 2),
                "urmtom": round(total_urmtom, 2),
                "positions_count": len(positions),
                "positions": positions,
            }
            poll_result = await poll_account_async(acc, now_ist(), reconcile_today=True)
        except Exception as e:
            zebu_data = {"ok": False, "error": str(e), "error_type": type(e).__name__}
            poll_result = f"error: {e}"

    today_str = now_ist().date().isoformat()
    all_tr = await DB._all_trades()
    today_trades = [
        t for t in all_tr
        if t.get("account") == acc["id"]
        and (t.get("open") or (t.get("exitDate") or t.get("date")) == today_str)
    ]
    sync_state = await DB.get_sync_state_async(acc["id"])
    return {
        "account": acc["id"],
        "zebu_uid": uid,
        "token": token_info,
        "sync_state": sync_state,
        "zebu": zebu_data,
        "poll_result": poll_result,
        "stored_today_trades_count": len(today_trades),
        "stored_today_trades_pnl": round(sum(float(t.get("pnl") or 0) for t in today_trades), 2),
        "stored_today_trades": today_trades,
    }


# ---------------- API Endpoints (Preserving 100% contracts) ----------------
@app.post("/api/login")
async def login(request: Request):
    client_ip = request.client.host if request.client else "127.0.0.1"
    check_login_rate_limit(client_ip)
    t0 = time.perf_counter()
    d = await request.json()
    u = await DB.get_user_async(str(d.get("user", "")))
    if not u or not check_pw(str(d.get("password", "")), u["pw"]):
        raise HTTPException(401, "Wrong user or password")

    tok = generate_session_token()
    await DB.put_session_async(tok, {"user": u["user"], "role": u["role"], "exp": time.time() + 12 * 3600})
    METRICS.record_api_latency((time.perf_counter() - t0) * 1000)
    return {"token": tok, "user": u["user"], "role": u["role"]}


@app.get("/api/me")
async def me(user=Depends(current_user)):
    return {"user": user["user"], "role": user["role"]}


@app.get("/api/users")
async def get_users(_=Depends(admin_only)):
    users = await DB.get_all_users_async()
    return [{"user": u["user"], "role": u["role"]} for u in users]


@app.put("/api/users/{name}")
async def put_user(name: str, request: Request, _=Depends(admin_only)):
    d = await request.json()
    old = (await DB.get_user_async(name)) or {}
    doc = {
        "user": name,
        "role": d.get("role", old.get("role", "viewer")),
        "pw": hash_pw(d["password"]) if d.get("password") else old.get("pw"),
        "trader_id": d.get("trader_id", old.get("trader_id")),
        "assigned_accounts": d.get("assigned_accounts", old.get("assigned_accounts", [])),
    }
    if not doc["pw"]:
        raise HTTPException(400, "Password needed")
    await DB.put_user_async(name, doc)
    return {"ok": True}


def _resolve_token_doc_fast(acc: dict, tokens_by_id: Dict[str, dict], tokens_by_uid: Dict[str, dict]) -> dict:
    aid = acc["id"]
    t = tokens_by_id.get(aid)
    if t and t.get("access_token"):
        return t
    uid = (acc.get("zebu_uid") or "").strip().upper()
    if uid and uid in tokens_by_uid:
        return tokens_by_uid[uid]
    return t or {}


@app.get("/api/traders")
async def get_traders(user=Depends(current_user)):
    all_traders = await DB._all_traders()

    if user["role"] == "admin":
        return all_traders
    elif user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        return [t for t in all_traders if t["id"] == tid] if tid else all_traders
    return all_traders


@app.put("/api/traders/{tid}")
async def put_trader(tid: str, request: Request, _=Depends(admin_only)):
    d = await request.json()
    d["id"] = tid
    await DB._put_trader(tid, d)
    if not await DB._get_rule(tid):
        await DB._put_rule(tid, {"trader": tid, "rules": engine.DEFAULT_RULES})
    asyncio.create_task(WS_HUB.broadcast({"type": "state_updated", "entity": "traders"}))
    return d


@app.delete("/api/traders/{tid}")
async def delete_trader(tid: str, _=Depends(admin_only)):
    await CONN_MGR.unregister_account(tid)
    await DB.delete_trader_permanent_async(tid)
    asyncio.create_task(WS_HUB.broadcast({"type": "state_updated", "entity": "traders"}))
    return {"ok": True}


@app.post("/api/traders/delete-bulk")
async def delete_traders_bulk(request: Request, _=Depends(admin_only)):
    d = await request.json()
    ids = d.get("ids", [])
    count = 0
    for tid in ids:
        await CONN_MGR.unregister_account(tid)
        await DB.delete_trader_permanent_async(tid)
        count += 1
    asyncio.create_task(WS_HUB.broadcast({"type": "state_updated", "entity": "traders"}))
    return {"ok": True, "deleted": count}


@app.get("/api/accounts")
async def get_accounts(user=Depends(current_user)):
    out = []
    all_acc, tokens_list, sync_states_map, all_positions = await asyncio.gather(
        DB.get_all_accounts_async(),
        DB._all_tokens(),
        DB.get_all_sync_states_async(),
        DB.get_positions_async(),
    )
    today_ist_date = now_ist().date()
    today_ist_str = today_ist_date.isoformat()
    # Group today's positions per account and select ONLY the latest snapshot batch (within 5s of max updated_at)
    # while excluding expired contracts (expiry < today_ist_date) so stale rows can NEVER leak into today's P&L.
    today_pos_by_acc: Dict[str, List[Tuple[datetime, dict]]] = {}
    for pos in all_positions:
        p_aid = pos.get("account")
        if not p_aid:
            continue
        upd_raw = pos.get("updated_at")
        if not upd_raw:
            continue
        try:
            upd_dt = datetime.fromisoformat(str(upd_raw).replace("Z", "+00:00"))
            if upd_dt.tzinfo is None:
                upd_dt = upd_dt.replace(tzinfo=timezone.utc)
            if upd_dt.astimezone(IST).date().isoformat() != today_ist_str:
                continue
        except Exception:
            continue
        # Exclude expired contracts from prior trading days
        parsed_inst = engine.parse_instrument(str(pos.get("tsym") or ""))
        if parsed_inst and parsed_inst.get("expiry") and parsed_inst["expiry"] < today_ist_date:
            continue
        today_pos_by_acc.setdefault(p_aid, []).append((upd_dt, pos))

    broker_pnl_by_acc: Dict[str, Dict[str, Any]] = {}
    for p_aid, items in today_pos_by_acc.items():
        max_upd = max(dt for dt, _ in items)
        latest_batch = [
            (dt, pos) for dt, pos in items
            if (max_upd - dt).total_seconds() <= 5.0
        ]
        seen_keys: set[str] = set()
        rpnl_sum = Decimal("0")
        urmtom_sum = Decimal("0")
        row_cnt = 0
        for dt, pos in latest_batch:
            ckey = f"{str(pos.get('exch') or 'NFO').upper()}|{str(pos.get('tsym') or '').strip()}|{str(pos.get('product') or 'MIS').upper()}"
            if ckey in seen_keys:
                continue
            seen_keys.add(ckey)
            rpnl_sum += Decimal(str(pos.get("realized_pnl") or 0))
            urmtom_sum += Decimal(str(pos.get("unrealized_pnl") or 0))
            row_cnt += 1
        broker_pnl_by_acc[p_aid] = {
            "rpnl": rpnl_sum,
            "urmtom": urmtom_sum,
            "count": row_cnt,
            "updated_at": max_upd.isoformat(),
        }
    tokens_by_id = {t["account"]: t for t in tokens_list}
    uid_by_aid = {a["id"]: (a.get("zebu_uid") or "").strip().upper() for a in all_acc}
    tokens_by_uid = {}
    for t in tokens_list:
        if t.get("access_token"):
            u = uid_by_aid.get(t["account"]) or t["account"].upper()
            if u:
                tokens_by_uid[u] = t

    if user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        if tid:
            all_acc = [a for a in all_acc if a.get("trader") == tid or a.get("id") == tid]
    elif user["role"] == "viewer":
        u = await DB.get_user_async(user["user"])
        allowed = u.get("assigned_accounts") if u else None
        if allowed:
            all_acc = [a for a in all_acc if a.get("id") in allowed]

    for a in all_acc:
        t = _resolve_token_doc_fast(a, tokens_by_id, tokens_by_uid)
        sync_st = sync_states_map.get(a["id"]) or {}
        conn_state = CONN_MGR.get_account_state(a["id"])
        l_state = _link_state_from_doc(t)
        # Never expose sensitive secret keys to frontend (Req 8.10)
        safe_acc = {k: v for k, v in a.items() if k != "zebu_secret_key"}
        safe_acc["has_secret_key"] = bool(a.get("zebu_secret_key"))
        ws_st = conn_state.status if l_state == "Linked" else ("REAUTH_REQUIRED" if l_state == "Login expired" else "DISCONNECTED")
        b_agg = broker_pnl_by_acc.get(a["id"])
        out.append({
            **safe_acc,
            "link": l_state,
            "linkedAt": t.get("linked_at"),
            "cliname": t.get("cliname"),
            "ws_status": ws_st,
            "circuit_open": conn_state.circuit_open,
            "is_stale": bool(sync_st.get("is_stale", False)),
            "sync_status": sync_st.get("status", "IDLE"),
            "last_sync_at": sync_st.get("last_sync_success") or t.get("last_sync_at"),
            "sync_error": sync_st.get("last_error"),
            "sync_message": t.get("sync_message"),
            "broker_rpnl": float(b_agg["rpnl"]) if b_agg else None,
            "broker_urmtom": float(b_agg["urmtom"]) if b_agg else None,
            "broker_total_pnl": float(b_agg["rpnl"] + b_agg["urmtom"]) if b_agg else None,
            "broker_positions_count": b_agg["count"] if b_agg else 0,
            "broker_pos_updated_at": b_agg["updated_at"] if b_agg else None,
        })
    return out


@app.put("/api/accounts/{aid}")
async def put_account(aid: str, request: Request, _=Depends(admin_only)):
    d = await request.json()
    doc = {k: d.get(k) for k in ("name", "broker", "trader", "zebu_uid", "zebu_actid", "zebu_client_id", "zebu_secret_key", "proxy_url")}
    doc["id"] = aid
    raw_uid = (doc.get("zebu_uid") or "").strip().upper()
    doc["zebu_uid"] = raw_uid[:-2] if raw_uid.endswith("_U") else raw_uid
    raw_actid = (doc.get("zebu_actid") or doc["zebu_uid"]).strip().upper()
    doc["zebu_actid"] = raw_actid[:-2] if raw_actid.endswith("_U") else raw_actid
    raw_cid = (doc.get("zebu_client_id") or doc["zebu_uid"]).strip()
    doc["zebu_client_id"] = raw_cid
    if doc.get("zebu_secret_key"):
        doc["zebu_secret_key"] = doc["zebu_secret_key"].strip()
    if doc.get("proxy_url"):
        doc["proxy_url"] = doc["proxy_url"].strip()
    await DB.put_account_async(aid, doc)
    if not await DB._get_rule(aid):
        base_rule = (await DB._get_rule(doc.get("trader") or "")) if doc.get("trader") else None
        await DB._put_rule(aid, {"trader": aid, "account": aid, "rules": (base_rule or {}).get("rules") or engine.DEFAULT_RULES})
    asyncio.create_task(WS_HUB.broadcast({"type": "state_updated", "entity": "accounts"}))
    safe_ret = {k: v for k, v in doc.items() if k != "zebu_secret_key"}
    return safe_ret


@app.delete("/api/accounts/{aid}")
async def delete_account(aid: str, _=Depends(admin_only)):
    await CONN_MGR.unregister_account(aid)
    await DB.delete_account_permanent_async(aid)
    asyncio.create_task(WS_HUB.broadcast({"type": "state_updated", "entity": "accounts"}))
    return {"ok": True}


@app.post("/api/accounts/delete-bulk")
async def delete_accounts_bulk(request: Request, _=Depends(admin_only)):
    d = await request.json()
    ids = d.get("ids", [])
    count = 0
    for aid in ids:
        await CONN_MGR.unregister_account(aid)
        await DB.delete_account_permanent_async(aid)
        count += 1
    asyncio.create_task(WS_HUB.broadcast({"type": "state_updated", "entity": "accounts"}))
    return {"ok": True, "deleted": count}


LINK_NONCE_MAP: Dict[str, Dict[str, Any]] = {}


@app.post("/api/accounts/{aid}/link")
async def link_account(aid: str, request: Request, _=Depends(admin_only)):
    acc = await DB.get_account_async(aid)
    if not acc:
        raise HTTPException(404, "No such account")

    fe_url = None
    try:
        data = await request.json()
        fe_url = data.get("frontend_url")
    except Exception:
        pass

    if not fe_url:
        fe_url = request.headers.get("origin") or request.headers.get("referer") or CFG.FRONTEND_URL
    fe_url = str(fe_url).split("?")[0].rstrip("/")

    n = generate_link_nonce()
    client_id = acc.get("zebu_client_id") or acc.get("zebu_uid") or CFG.ZEBU_CLIENT_ID
    secret_key = acc.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY
    proxy_url = acc.get("proxy_url")
    nonce_doc = {
        "account": aid,
        "client_id": client_id,
        "secret_key": secret_key,
        "proxy_url": proxy_url,
        "uid": acc.get("zebu_uid") or "",
        "exp": time.time() + 900,
        "frontend_url": fe_url,
    }
    LINK_NONCE_MAP[n] = nonce_doc
    await DB._put_link_nonce(n, nonce_doc)
    return {"ok": True, "nonce": n, "url": f"/auth/zebu/start?n={n}"}


@app.post("/api/accounts/{aid}/unlink")
async def unlink_account(aid: str, _=Depends(admin_only)):
    await DB.delete_token_async(aid)
    await CONN_MGR.unregister_account(aid)
    FIN_SUMMARY.clear_account(aid)
    await DB.put_sync_state_async(aid, {
        "status": "DISCONNECTED",
        "is_stale": False,
        "last_error": None,
    })
    return {"ok": True}


@app.post("/api/accounts/{aid}/token")
async def save_manual_token(aid: str, request: Request, _=Depends(admin_only)):
    acc = await DB.get_account_async(aid)
    if not acc:
        raise HTTPException(404, "No such account")
    data = await request.json()
    token_str = (data.get("token") or "").strip()
    if not token_str:
        raise HTTPException(400, "Token or code is required")

    uid = (acc.get("zebu_uid") or acc.get("zebu_actid") or "").strip().upper()

    is_code = "code=" in token_str or "?" in token_str or "&" in token_str or "http" in token_str
    if is_code:
        import urllib.parse
        parsed = urllib.parse.urlparse(token_str)
        qs = urllib.parse.parse_qs(parsed.query or (token_str if "?" not in token_str else token_str.split("?", 1)[1]))
        if "code" in qs:
            token_str = qs["code"][0]

    final_token = token_str
    refresh_token = None
    exp_in = 86400 * 3650  # Persistent authorization until broker token expires or user disconnects

    if is_code or len(token_str) < 50:
        try:
            client_id = acc.get("zebu_client_id") or uid or CFG.ZEBU_CLIENT_ID
            secret_key = acc.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY
            proxy_url = acc.get("proxy_url")
            exchanged = await ZEBU.exchange_code_async(token_str, uid=uid, client_id=client_id, secret_key=secret_key, proxy=proxy_url)
            tok = (exchanged.get("access_token") or exchanged.get("susertoken")) if isinstance(exchanged, dict) else None
            if tok:
                final_token = tok
                refresh_token = exchanged.get("refresh_token")
                log.info(f"Successfully exchanged code for access token for account {aid} ({uid})")
        except Exception as e:
            log.warning(f"Failed to exchange code: {e}")

    cliname = None
    clean_uid = (uid or "").strip().upper()
    if clean_uid.endswith("_U"):
        clean_uid = clean_uid[:-2]
    clean_actid = (acc.get("zebu_actid") or clean_uid).strip().upper()
    if clean_actid.endswith("_U"):
        clean_actid = clean_actid[:-2]
    try:
        cd = await ZEBU.client_details_async(final_token, clean_uid, clean_actid, proxy=acc.get("proxy_url"))
        if isinstance(cd, dict) and cd.get("stat") != "Not_Ok":
            cliname = cd.get("cliname")
            returned_actid = (cd.get("actid") or cd.get("uid") or "").strip().upper()
            c_ret = returned_actid[:-2] if returned_actid.endswith("_U") else returned_actid
            if c_ret and clean_uid and c_ret != clean_uid:
                raise ZebuError(f"Session belongs to client '{returned_actid}', but this account is configured for '{clean_uid}'. Please log in as '{clean_uid}'.")
    except ZebuError:
        raise
    except Exception as e:
        log.warning(f"Token validation note for {aid}: {e}")

    doc = {
        "access_token": final_token,
        "refresh_token": refresh_token,
        "expires_at": time.time() + exp_in,
        "linked_at": now_ist().isoformat(timespec="seconds"),
        "status": "Linked",
        "ws_status": "REAL-TIME",
        "cliname": cliname,
        "sync_message": "Connected to Zebu",
    }
    await DB.put_token_async(aid, doc)
    # Also update any other account row sharing the same zebu_uid (e.g. AC-01 and 47000S19) and clear any old expired/stale flags
    all_acc = await DB.get_all_accounts_async()
    for a_row in all_acc:
        if a_row["id"] == aid or (clean_uid and (a_row.get("zebu_uid") or "").upper() == clean_uid):
            await DB.put_token_async(a_row["id"], doc)
            await DB.put_sync_state_async(a_row["id"], {
                "status": "LIVE",
                "is_stale": False,
                "last_error": None,
                "consecutive_failures": 0,
                "circuit_open": False,
            })
            st_obj = CONN_MGR.get_account_state(a_row["id"])
            st_obj.record_success()
            st_obj.status = "REAL-TIME"
    acc["zebu_uid"] = clean_uid
    acc["zebu_actid"] = clean_actid
    await DB.put_account_async(aid, acc)

    await _ensure_account_ws_stream(acc, final_token)
    asyncio.create_task(poll_account_async(acc, now_ist()))
    return {"ok": True, "cliname": doc.get("cliname")}


@app.get("/auth/zebu/start")
async def zebu_start(request: Request, n: str = "", account: str = "", fe: str = ""):
    fe_param = fe or request.query_params.get("fe") or ""
    acc = None
    if account:
        acc = await DB.get_account_async(account)

    if acc and not n:
        n = generate_link_nonce()
        frontend_target = fe_param or request.headers.get("referer") or request.headers.get("origin") or CFG.FRONTEND_URL
        frontend_target = str(frontend_target).split("?")[0].rstrip("/")
        if not frontend_target or "onrender.com" in frontend_target or "localhost:8000" in frontend_target or "/auth" in frontend_target:
            frontend_target = CFG.FRONTEND_URL.rstrip("/")
        client_id = acc.get("zebu_client_id") or acc.get("zebu_uid") or CFG.ZEBU_CLIENT_ID
        clean_cid = client_id.strip() if client_id else ""
        if clean_cid and not clean_cid.endswith("_U") and "_" not in clean_cid:
            clean_cid = f"{clean_cid}_U"
        secret_key = acc.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY
        proxy_url = acc.get("proxy_url")
        nonce_uid = (acc.get("zebu_uid") or "").strip().upper()
        if nonce_uid.endswith("_U"):
            nonce_uid = nonce_uid[:-2]
        nonce_doc = {
            "account": account,
            "client_id": clean_cid,
            "secret_key": secret_key,
            "proxy_url": proxy_url,
            "uid": nonce_uid,
            "exp": time.time() + 900,
            "frontend_url": frontend_target,
        }
        LINK_NONCE_MAP[n] = nonce_doc
        await DB._put_link_nonce(n, nonce_doc)

    d = LINK_NONCE_MAP.get(n) or await DB._get_link_nonce(n)
    if not d or d.get("exp", 0) < time.time():
        return HTMLResponse("<h3>Link expired. Go back to the dashboard and click Connect Zebu again.</h3>", 400)

    cb_url = "https://tts-backend-ezm3.onrender.com/auth/zebu/callback"
    if "localhost" in str(request.base_url) or "127.0.0.1" in str(request.base_url):
        cb_url = f"{CFG.PUBLIC_URL.rstrip('/')}/auth/zebu/callback"
    client_id = d.get("client_id") or CFG.ZEBU_CLIENT_ID
    clean_cid = client_id.strip() if client_id else ""
    if clean_cid and not clean_cid.endswith("_U") and "_" not in clean_cid:
        clean_cid = f"{clean_cid}_U"
    r = RedirectResponse(ZEBU.authorize_url(client_id=clean_cid, state=n, redirect_uri=cb_url))
    ref = d.get("frontend_url") or fe_param or request.headers.get("referer") or request.headers.get("origin") or CFG.FRONTEND_URL
    if ref:
        r.set_cookie("tts_fe_url", str(ref).split("?")[0].rstrip("/"), max_age=900, httponly=True, samesite="lax", secure=True)
    r.set_cookie("tts_link", n, max_age=900, httponly=True, samesite="lax", secure=True)
    return r


@app.get("/auth/zebu/callback")
async def zebu_callback(request: Request, code: str = "", state: str = ""):
    n = state or request.cookies.get("tts_link", "")
    fe_url = request.cookies.get("tts_fe_url", "")
    d = LINK_NONCE_MAP.get(n) or ((await DB._get_link_nonce(n)) if n else None)
    if not d or d.get("exp", 0) < time.time():
        async with AsyncSessionLocal() as session:
            stmt = select(LinkNonce).where(LinkNonce.exp > datetime.now(timezone.utc)).order_by(LinkNonce.created_at.desc())
            latest_nonce = (await session.execute(stmt)).scalars().first()
            if latest_nonce:
                exp_ts = latest_nonce.exp.timestamp() if latest_nonce.exp else time.time() + 900
                d = LINK_NONCE_MAP.get(latest_nonce.nonce) or {"account": latest_nonce.account_id, "exp": exp_ts}
                n = latest_nonce.nonce

    if not d or not d.get("account"):
        all_acc = await DB.get_all_accounts_async()
        zebu_accs = [a for a in all_acc if a.get("broker") == "Zebu"]
        if zebu_accs:
            d = {"account": zebu_accs[0]["id"], "exp": time.time() + 900, "frontend_url": CFG.FRONTEND_URL}

    target_frontend = (d.get("frontend_url") if d else "") or fe_url or CFG.FRONTEND_URL
    if not target_frontend or "onrender.com" in target_frontend or "localhost:8000" in target_frontend or "127.0.0.1:8000" in target_frontend or "/auth" in target_frontend:
        target_frontend = CFG.FRONTEND_URL
    target_url = f"{target_frontend.rstrip('/')}/?linked=true#traders"

    if not d or not d.get("account"):
        return HTMLResponse(f"<h3>Link session expired. <a href='{target_url}'>Click here to return to dashboard</a></h3>", 400)
    if not code:
        return HTMLResponse(f"<h3>Zebu did not return a code. <a href='{target_url}'>Click here to return to dashboard</a></h3>", 400)

    acc = await DB.get_account_async(d["account"])
    if not acc:
        all_acc = await DB.get_all_accounts_async()
        zebu_accs = [a for a in all_acc if a.get("broker") == "Zebu"]
        if zebu_accs:
            acc = zebu_accs[0]
            d["account"] = acc["id"]
        else:
            return HTMLResponse(f"<h3>No Zebu account found to link. <a href='{target_url}'>Return to dashboard</a></h3>", 404)
    uid = (acc.get("zebu_uid") if acc else "") or (acc.get("zebu_actid") if acc else "")
    uid = uid.strip().upper()
    client_id = d.get("client_id") or acc.get("zebu_client_id") or uid or CFG.ZEBU_CLIENT_ID
    secret_key = d.get("secret_key") or acc.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY
    proxy_url = d.get("proxy_url") or (acc.get("proxy_url") if acc else None)
    try:
        tok = await ZEBU.exchange_code_async(code, uid=uid, client_id=client_id, secret_key=secret_key, proxy=proxy_url)
    except Exception as e:
        server_ip_str = ""
        try:
            async with httpx.AsyncClient(timeout=5, proxy=proxy_url) as cl:
                res = await cl.get("https://api.ipify.org?format=json")
                server_ip_str = res.json().get("ip", "")
        except Exception:
            pass

        ip_hint = f"""<div style='background:#1e293b;border:1px solid #f59e0b;border-radius:8px;padding:14px;margin:16px auto;max-width:440px;text-align:left;'>
            <div style='color:#f59e0b;font-weight:600;font-size:13px;'>⚠️ Zebu IP Whitelist Notice</div>
            <div style='color:#cbd5e1;font-size:12px;margin-top:6px;'>
                Outbound IP reaching Zebu: <code style='background:#0f172a;color:#38bdf8;padding:2px 6px;border-radius:4px;font-weight:700;'>{server_ip_str or 'unknown'}</code>
            </div>
        </div>""" if "INVALID_IP" in str(e) else ""

        return HTMLResponse(
            f"""<!DOCTYPE html>
<html>
<head><title>Zebu Login Failed</title></head>
<body style='font-family:sans-serif;padding:40px;text-align:center;background:#0f172a;color:#f8fafc;'>
    <div style='max-width:480px;margin:40px auto;background:#1e293b;border-radius:12px;border:1px solid #ef4444;padding:24px;'>
        <h2 style='color:#ef4444;margin-top:0;'>❌ Zebu Login Failed</h2>
        <p style='color:#f8fafc;font-size:14px;'>{e}</p>
        <p style='color:#94a3b8;font-size:13px;'>Attempted to link account <b>{acc.get('name', '')}</b> with client ID <code>{uid}</code>.</p>
        {ip_hint}
        <p style='margin-top:20px;'><a href='{target_url}' style='color:#38bdf8;text-decoration:none;font-weight:600;'>← Return to Dashboard</a></p>
    </div>
</body>
</html>""", 400
        )

    access_token = tok["access_token"]
    cliname = None
    clean_uid = (tok.get("uid") or uid).strip().upper()
    if clean_uid.endswith("_U"):
        clean_uid = clean_uid[:-2]
    clean_actid = (tok.get("actid") or acc.get("zebu_actid") or clean_uid).strip().upper()
    if clean_actid.endswith("_U"):
        clean_actid = clean_actid[:-2]

    try:
        cd = await ZEBU.client_details_async(access_token, clean_uid, clean_actid, proxy=proxy_url)
        if isinstance(cd, dict) and cd.get("stat") != "Not_Ok":
            cliname = cd.get("cliname")
            returned_actid = (cd.get("actid") or cd.get("uid") or "").strip().upper()
            c_ret = returned_actid[:-2] if returned_actid.endswith("_U") else returned_actid
            if c_ret:
                clean_uid = c_ret
                clean_actid = c_ret
    except Exception as ex:
        log.warning(f"Zebu callback ClientDetails note for {acc['id']}: {ex}")

    exp_in = 86400 * 3650

    doc = {
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token"),
        "expires_at": time.time() + exp_in,
        "linked_at": now_ist().isoformat(timespec="seconds"),
        "status": "Linked",
        "ws_status": "REAL-TIME",
        "cliname": cliname,
        "sync_message": "Connected to Zebu",
    }
    await DB.put_token_async(d["account"], doc)
    # Also update any other account row sharing the same zebu_uid (e.g. AC-01 and 47000S19) and clear any old expired/stale flags
    all_acc = await DB.get_all_accounts_async()
    for a_row in all_acc:
        if a_row["id"] == acc["id"] or (clean_uid and (a_row.get("zebu_uid") or "").upper() == clean_uid):
            await DB.put_token_async(a_row["id"], doc)
            await DB.put_sync_state_async(a_row["id"], {
                "status": "LIVE",
                "is_stale": False,
                "last_error": None,
                "consecutive_failures": 0,
                "circuit_open": False,
            })
            st_obj = CONN_MGR.get_account_state(a_row["id"])
            st_obj.record_success()
            st_obj.status = "REAL-TIME"
    acc["zebu_uid"] = clean_uid
    acc["zebu_actid"] = clean_actid
    await DB.put_account_async(acc["id"], acc)
    await DB._delete_link_nonce(n)
    LINK_NONCE_MAP.pop(n, None)

    await _ensure_account_ws_stream(acc, doc["access_token"])

    asyncio.create_task(poll_all_async("linked"))

    return HTMLResponse(
        f"""<!DOCTYPE html>
<html>
<head>
    <title>Zebu Account Linked</title>
</head>
<body style="font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0f172a;color:#f8fafc;">
    <div style="text-align:center;padding:24px;background:#1e293b;border-radius:12px;border:1px solid #334155;max-width:440px;">
        <h2 style="color:#22c55e;margin-top:0;">✅ {acc['name']} linked to Zebu!</h2>
        <p style="color:#94a3b8;font-size:14px;">Your Zebu account is connected and live streaming is active.</p>
        <p style="color:#64748b;font-size:13px;">Returning to your dashboard in 2 seconds...</p>
        <p style="margin-top:16px;"><a href="{target_url}" style="color:#38bdf8;text-decoration:none;font-weight:600;">← Back to Dashboard</a></p>
    </div>
    <script>
        try {{
            const bc = new BroadcastChannel('tts_auth');
            bc.postMessage({{ type: 'zebu_linked', account: '{acc['id']}' }});
        }} catch(e) {{}}

        try {{
            if (window.opener && !window.opener.closed) {{
                try {{ window.opener.location.reload(); }} catch(e) {{}}
                setTimeout(function() {{ window.close(); }}, 1500);
            }} else {{
                setTimeout(function() {{ window.location.href = "{target_url}"; }}, 1500);
            }}
        }} catch(e) {{
            window.location.href = "{target_url}";
        }}
    </script>
</body>
</html>"""
    )


@app.get("/api/rules")
async def get_rules(_=Depends(current_user)):
    return await get_rules_map_async()


async def _related_rule_ids_async(tid: str) -> List[str]:
    ids = {tid}
    all_acc = await DB.get_all_accounts_async()
    for a in all_acc:
        if a.get("id") == tid or a.get("trader") == tid or a.get("zebu_uid") == tid:
            if a.get("id"):
                ids.add(a["id"])
            if a.get("trader"):
                ids.add(a["trader"])
            if a.get("zebu_uid"):
                ids.add(a["zebu_uid"])
    return list(ids)


@app.put("/api/rules/{tid}")
async def put_rules(tid: str, request: Request, user=Depends(admin_only)):
    d = await request.json()
    rules_payload = d.get("rules", d)
    rel_ids = await _related_rule_ids_async(tid)
    for rid in rel_ids:
        await DB._put_rule(rid, {"trader": rid, "account": rid, "rules": rules_payload})

    await DB._put_rule_log(secrets.token_hex(6), {
        "trader": tid,
        "at": now_ist().strftime("%Y-%m-%d %H:%M"),
        "by": user["user"],
        "change": d.get("change", "Rules updated"),
    })
    # Clear all old violations for this trader & linked accounts, then re-evaluate against the new rules
    for rid in rel_ids:
        await DB.clear_rule_violations_async(rid)
    for rid in rel_ids:
        await evaluate_and_record_violations_async(now_ist(), account_id=rid)
    return {"ok": True}


@app.delete("/api/rules/{tid}")
async def delete_rules(tid: str, user=Depends(admin_only)):
    no_rules = {
        "instrument": "ANY",
        "expiry": "ANY",
        "strikeMode": "ANY",
        "strikeDist": 0,
        "side": "ANY",
        "product": "ANY",
        "timeFrom": "",
        "timeTo": "",
        "maxTradesDay": 0,
        "maxLots": 0,
        "slPerLot": 0,
        "dailyLoss": 0,
        "hedge": False,
        "source": "ANY",
        "maxOpenPositions": 0,
        "maxExposure": 0,
        "requireHedgeOverLots": 0,
    }
    rel_ids = await _related_rule_ids_async(tid)
    for rid in rel_ids:
        await DB._put_rule(rid, {"trader": rid, "account": rid, "rules": no_rules})
        await DB.clear_rule_violations_async(rid)
    await DB._put_rule_log(secrets.token_hex(6), {
        "trader": tid,
        "at": now_ist().strftime("%Y-%m-%d %H:%M"),
        "by": user["user"],
        "change": "All rules deleted for account",
    })
    return {"ok": True, "rules": no_rules}


@app.get("/api/rulelog")
async def get_rulelog(_=Depends(current_user)):
    logs = await DB._all_rule_logs()
    return sorted(logs, key=lambda x: x["at"])


@app.get("/api/violations")
async def get_violations(account: Optional[str] = None, user=Depends(current_user)):
    violations = await DB.get_rule_violations_async(account_id=account)
    if user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        if tid:
            violations = [v for v in violations if v.get("trader") == tid or v.get("account") == tid]
    elif user["role"] == "viewer":
        u = await DB.get_user_async(user["user"])
        allowed = u.get("assigned_accounts") if u else None
        if allowed:
            violations = [v for v in violations if v.get("account") in allowed]
    return violations


@app.get("/api/daily-reports")
async def get_daily_reports(account: Optional[str] = None, from_date: Optional[str] = None, user=Depends(current_user)):
    reports = await DB.get_daily_reports_async(account_id=account, from_date=from_date)
    if user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        if tid:
            reports = [r for r in reports if r.get("trader") == tid or r.get("account") == tid]
    elif user["role"] == "viewer":
        u = await DB.get_user_async(user["user"])
        allowed = u.get("assigned_accounts") if u else None
        if allowed:
            reports = [r for r in reports if r.get("account") in allowed]
    return reports


@app.post("/api/finalize-eod")
async def trigger_finalize_eod(request: Request, date: Optional[str] = None, _=Depends(admin_only)):
    target_date = date or request.query_params.get("date")
    if not target_date:
        try:
            body = await request.json()
            target_date = body.get("date")
        except Exception:
            pass
    return await finalize_eod_async(now=now_ist(), target_date=target_date)


@app.post("/api/midnight-archive")
async def trigger_midnight_archive(_=Depends(admin_only)):
    return await midnight_archive_async(now=now_ist())


@app.get("/api/reconcile-calendar")
async def get_calendar_reconciliation(account: Optional[str] = None, _=Depends(current_user)):
    return await reconcile_daily_calendar_async(account_id=account)


@app.get("/api/trades")
async def get_trades(request: Request, user=Depends(current_user)):
    t0 = time.perf_counter()
    frm = request.query_params.get("from", "")
    acc_filter = request.query_params.get("account", "")
    limit_raw = request.query_params.get("limit", "")
    offset_raw = request.query_params.get("offset", "")
    limit_val = int(limit_raw) if limit_raw.isdigit() else None
    offset_val = int(offset_raw) if offset_raw.isdigit() else None
    all_trades = await DB._all_trades(from_date=frm if frm else None, limit=limit_val, offset=offset_val)

    if acc_filter:
        all_trades = [t for t in all_trades if t.get("account") == acc_filter or t.get("trader") == acc_filter]

    # Account-level isolation
    if user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        if tid:
            all_trades = [t for t in all_trades if t.get("trader") == tid or t.get("account") == tid]
    elif user["role"] == "viewer":
        u = await DB.get_user_async(user["user"])
        allowed = u.get("assigned_accounts") if u else None
        if allowed:
            all_trades = [t for t in all_trades if t.get("account") in allowed]

    METRICS.record_api_latency((time.perf_counter() - t0) * 1000)
    return all_trades


@app.post("/api/trades/import")
async def import_trades(request: Request, _=Depends(admin_only)):
    try:
        rows = await request.json()
        items = []
        for r in rows:
            r["origin"] = "import"
            r["id"] = r.get("id") or "I-" + secrets.token_hex(5)
            aid = str(r.get("account") or "AC-01").strip()
            tid = str(r.get("trader") or aid).strip()
            r["account"] = aid
            r["trader"] = tid
            if not await DB.get_account_async(aid):
                if not await DB._get_trader(tid):
                    await DB._put_trader(tid, {"id": tid, "name": f"Trader {tid}", "status": "Testing"})
                await DB.put_account_async(aid, {"id": aid, "name": f"Account {aid}", "broker": "Manual", "trader": tid})
            elif not await DB._get_trader(tid):
                await DB._put_trader(tid, {"id": tid, "name": f"Trader {tid}", "status": "Testing"})
            if "qty" not in r or r["qty"] is None:
                r["qty"] = float(r.get("lots") or 1) * int(r.get("lotSize") or 1)
            items.append((r["id"], r))
        await DB._put_many_trades(items)
        await evaluate_and_record_violations_async(now_ist())
        return {"ok": True, "n": len(items)}
    except Exception as e:
        log.exception(f"Import trades error: {e}")
        raise HTTPException(500, f"Import failed: {str(e)}")


@app.delete("/api/trades/{tid}")
async def delete_trade(tid: str, _=Depends(admin_only)):
    await DB._delete_trade(tid, allow_delete_finalized=True)
    return {"ok": True}


@app.post("/api/trades/delete-bulk")
async def delete_trades_bulk(request: Request, _=Depends(admin_only)):
    d = await request.json()
    ids = d.get("ids", [])
    count = 0
    for tid in ids:
        await DB._delete_trade(tid, allow_delete_finalized=True)
        count += 1
    return {"ok": True, "deleted": count}


@app.get("/api/groups")
async def get_groups(_=Depends(current_user)):
    return await DB._all_groups()


@app.put("/api/groups/{gid}")
async def put_group(gid: str, request: Request, _=Depends(admin_only)):
    d = await request.json()
    d["id"] = gid
    await DB._put_group(gid, d)
    return d


@app.delete("/api/groups/{gid}")
async def del_group(gid: str, _=Depends(admin_only)):
    await DB._delete_group(gid)
    return {"ok": True}


@app.get("/api/settings")
async def get_settings(_=Depends(current_user)):
    s = (await DB._get_setting("main")) or {}
    merged = {**DEFAULT_SETTINGS, **s}
    merged["pass"] = {**DEFAULT_SETTINGS["pass"], **(s.get("pass") or {})}
    merged["alerts"] = {**DEFAULT_SETTINGS["alerts"], **(s.get("alerts") or {})}
    return merged


@app.put("/api/settings")
async def put_settings(request: Request, _=Depends(admin_only)):
    d = await request.json()
    d.pop("apiKey", None)
    curr = (await DB._get_setting("main")) or {}
    merged = {**DEFAULT_SETTINGS, **curr, **d}
    await DB._put_setting("main", merged)
    return merged


@app.post("/api/sync")
async def sync_now(request: Request, account: Optional[str] = None, trader: Optional[str] = None, force: bool = False, user=Depends(current_user)):
    target_aid = account or trader or request.query_params.get("account") or request.query_params.get("trader")
    force_flag = force or str(request.query_params.get("force", "")).lower() in ("1", "true", "yes")
    if not target_aid:
        try:
            body = await request.json()
            target_aid = body.get("account") or body.get("trader")
            if body.get("force"):
                force_flag = True
        except Exception:
            pass

    if target_aid:
        all_acc = await DB.get_all_accounts_async()
        # Priority 1: Resolve ALL accounts belonging to the selected trader or matching the account/UID
        direct = [a for a in all_acc if a.get("id") == target_aid]
        if direct:
            matched_accounts = direct
        else:
            matched_accounts = [
                a for a in all_acc
                if a.get("trader") == target_aid or a.get("zebu_uid") == target_aid
            ]

        # Enforce account-level authorization so users can only sync authorized accounts
        authorized_accounts = []
        for a in matched_accounts:
            try:
                await _assert_user_can_access_account(user, a)
                authorized_accounts.append(a)
            except HTTPException:
                continue

        if not authorized_accounts and matched_accounts:
            raise HTTPException(403, "Not authorized to synchronize the requested account(s)")

        if authorized_accounts:
            now = now_ist()

            async def _sync_one_account(acc_obj: dict):
                aid_i = acc_obj["id"]
                async with CONN_MGR.semaphore:
                    try:
                        res_str = await poll_account_async(acc_obj, now, reconcile_today=True, force=force_flag)
                        ok = not (res_str.startswith("error:") or res_str in ("expired", "not_linked"))
                        # Record versioned reconciliation note if forced after EOD finalization
                        if force_flag and ok:
                            pos_rows = await DB.get_positions_async(account_id=aid_i)
                            await _record_post_eod_versioned_reconciliation_async(acc_obj, now, pos_rows)
                        return aid_i, {"ok": ok, "result": res_str, "account_name": acc_obj.get("name") or aid_i}
                    except SessionExpired as se:
                        return aid_i, {"ok": False, "result": "expired", "error": str(se), "account_name": acc_obj.get("name") or aid_i}
                    except Exception as e:
                        return aid_i, {"ok": False, "result": f"error: {str(e)[:200]}", "error": str(e)[:200], "account_name": acc_obj.get("name") or aid_i}

            pairs = await asyncio.gather(*[_sync_one_account(a) for a in authorized_accounts], return_exceptions=True)
            results_map: Dict[str, Any] = {}
            succeeded = 0
            failed = 0
            for item in pairs:
                if isinstance(item, tuple):
                    k, v = item
                    results_map[k] = v
                    if v.get("ok"):
                        succeeded += 1
                    else:
                        failed += 1

            first_aid = authorized_accounts[0]["id"]
            return {
                "target": target_aid,
                "account": first_aid,
                "result": results_map.get(first_aid, {}).get("result", "ok"),
                "accounts_synced": len(authorized_accounts),
                "succeeded": succeeded,
                "failed": failed,
                "results": results_map,
            }

    return await poll_all_async("manual", force=force_flag)


@app.post("/api/alerts/test")
async def test_alert(_=Depends(admin_only)):
    s = (await DB._get_setting("main")) or {}
    alerts_cfg = s.get("alerts") or DEFAULT_SETTINGS["alerts"]
    sent = await ALERT.send_async(
        "✅ Test alert from modernized Trader Tracking System",
        settings_alerts=alerts_cfg,
        severity="Low",
    )
    return {"sent": sent}


@app.get("/api/status")
async def status(_=Depends(current_user)):
    poll_stat, acc_sync_states = await asyncio.gather(
        DB._get_status("poll"),
        DB.get_all_sync_states_async(),
    )
    return {
        "poll": poll_stat or {},
        "account_sync": acc_sync_states,
        "zebu_configured": bool(CFG.ZEBU_CLIENT_ID and CFG.ZEBU_SECRET_KEY),
        "redirect_url": CFG.PUBLIC_URL + "/auth/zebu/callback",
        "poll_seconds": CFG.POLL_SECONDS,
        "market": f"{CFG.MARKET_OPEN}–{CFG.MARKET_CLOSE} IST",
        "max_concurrent_accounts": CFG.MAX_CONCURRENT_ACCOUNTS,
        "active_streams": len([a for a, s in CONN_MGR.accounts.items() if s.status == "REAL-TIME"]),
    }


@app.get("/api/orders")
async def get_orders_endpoint(account: Optional[str] = None, _=Depends(current_user)):
    return await DB.get_orders_async(account_id=account)


@app.get("/api/orders/{norenordno}/history")
async def get_single_order_history(norenordno: str, account: Optional[str] = None, _=Depends(current_user)):
    """Official Zebu SingleOrdHist endpoint proxy with local cache fallback."""
    all_acc = await DB.get_all_accounts_async()
    target_acc = None
    if account:
        target_acc = next((a for a in all_acc if a["id"] == account or a.get("zebu_uid") == account), None)
    if not target_acc and all_acc:
        target_acc = all_acc[0]
    if not target_acc:
        return []
    aid = target_acc["id"]
    cache_k = f"{aid}|{norenordno}"
    if cache_k in _order_hist_cache:
        return _order_hist_cache[cache_k]
    tok = await token_for_account_async(aid)
    uid = target_acc.get("zebu_uid")
    if tok and uid:
        try:
            hist = await ZEBU.single_order_history_async(tok, uid, norenordno, proxy=target_acc.get("proxy_url"))
            if isinstance(hist, list) and hist:
                _order_hist_cache[cache_k] = hist
                return hist
        except Exception as e:
            log.warning(f"SingleOrdHist live fetch error for {norenordno}: {e}")
    return _order_hist_cache.get(cache_k, [])


@app.get("/api/bootstrap")
async def bootstrap_dashboard(request: Request, user=Depends(current_user)):
    """Ultra-fast single-request bootstrap endpoint returning all dashboard state concurrently."""
    t0 = time.perf_counter()
    frm = request.query_params.get("from", "")
    (
        traders_res,
        accounts_res,
        rules_res,
        trades_res,
        groups_res,
        settings_res,
        rulelog_res,
        status_res,
        daily_reports_res,
        violations_res,
        has_older,
    ) = await asyncio.gather(
        get_traders(user=user),
        get_accounts(user=user),
        get_rules(_=user),
        get_trades(request=request, user=user),
        get_groups(_=user),
        get_settings(_=user),
        get_rulelog(_=user),
        status(_=user),
        get_daily_reports(user=user),
        get_violations(user=user),
        DB.has_trades_before_async(frm) if frm else asyncio.sleep(0, result=False),
    )
    METRICS.record_api_latency((time.perf_counter() - t0) * 1000)
    return {
        "user": {"user": user["user"], "role": user["role"]},
        "traders": traders_res,
        "accounts": accounts_res,
        "rules": rules_res,
        "trades": trades_res,
        "has_older_trades": bool(has_older),
        "groups": groups_res,
        "settings": settings_res,
        "ruleLog": rulelog_res,
        "status": status_res,
        "dailyReports": daily_reports_res,
        "violations": violations_res,
    }


@app.get("/api/server-ip")
async def server_ip():
    try:
        async with httpx.AsyncClient(timeout=4) as cl:
            r = await cl.get("https://api.ipify.org?format=json")
            return r.json()
    except Exception as e:
        return {"error": str(e)}


def get_frontend_dir() -> Path:
    if (ROOT_DIR / "static").exists():
        return ROOT_DIR / "static"
    if (ROOT_DIR.parent / "frontend").exists():
        return ROOT_DIR.parent / "frontend"
    return ROOT_DIR / "static"


@app.get("/")
def index():
    fe_dir = get_frontend_dir()
    index_file = fe_dir / "index.html"
    if not index_file.exists():
        return JSONResponse({"status": "TTS Backend API is running", "docs": "/docs"})
    return FileResponse(index_file)


_fe_dir = get_frontend_dir()
if _fe_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_fe_dir)), name="static")
