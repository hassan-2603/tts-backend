"""Trader Tracking System (TTS) — Modern Real-Time Trading Monitoring Server.

Features:
- High-performance asynchronous FastAPI & PostgreSQL architecture
- Up to 50+ concurrent linked Zebu trading accounts with independent streams & error boundaries
- Zebu / Noren real-time WebSocket ingestion & REST reconciliation
- Account-level authorization: Admin, Trader, Viewer
- Preserves 100% compatibility with existing static/index.html dashboard
- Live Dashboard WebSocket endpoint (/api/ws)
"""
import asyncio
import hashlib
import json
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
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
    TradingRule,
    RuleLog,
    Alert,
    SentFlag,
    DailySyncRecord,
)
from app.security import hash_pw, check_pw, generate_session_token, generate_link_nonce, encrypt_token, decrypt_token
from app.store import Store
from app.zebu import ZebuClient, SessionExpired, ZebuError
from app.alerts import Alerter
from app.connection_manager import ConnectionManager
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


# ---------------- Spot Price Cache ----------------
_spot_cache = {}
_index_tokens = {}


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

    # Look for active token
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

    # Fallback to configured tokens
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
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)


WS_HUB = DashboardWebSocketManager()


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
    async with AsyncSessionLocal() as session:
        stmt = select(BrokerSession).where(BrokerSession.account_id == acc_id)
        bs = (await session.execute(stmt)).scalar_one_or_none()
        if not bs:
            return None
        exp = ensure_utc(bs.expires_at)
        if exp and exp < datetime.now(timezone.utc):
            return None
        return decrypt_token(bs.access_token_enc)


async def any_token_async() -> Tuple[Optional[str], Optional[str]]:
    async with AsyncSessionLocal() as session:
        stmt = select(Account, BrokerSession).join(BrokerSession, Account.id == BrokerSession.account_id)
        res = (await session.execute(stmt)).all()
        now_utc = datetime.now(timezone.utc)
        for acc, bs in res:
            if bs.access_token_enc and acc.zebu_uid:
                exp = ensure_utc(bs.expires_at)
                if not exp or exp >= now_utc:
                    return decrypt_token(bs.access_token_enc), acc.zebu_uid
    return None, None


async def poll_account_async(acc: dict, now: datetime) -> str:
    """Fetches TradeBook and PositionBook for an account and builds trades."""
    tok = await token_for_account_async(acc["id"])
    if not tok:
        return "expired"

    uid, actid = acc.get("zebu_uid"), acc.get("zebu_actid") or acc.get("zebu_uid")

    try:
        fills = await ZEBU.trade_book_async(tok, uid, actid)
        positions = await ZEBU.position_book_async(tok, uid, actid)
    except SessionExpired:
        async with AsyncSessionLocal() as session:
            stmt = select(BrokerSession).where(BrokerSession.account_id == acc["id"])
            bs = (await session.execute(stmt)).scalar_one_or_none()
            if bs:
                bs.status = "Login expired"
                bs.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
                await session.commit()
        return "expired"

    today_str = now.date().isoformat()
    items = []
    for f in fills:
        fid = f"{acc['id']}|{f.get('norenordno')}|{f.get('flid') or f.get('fltm')}"
        items.append((fid, {**f, "_acc": acc["id"], "_day": today_str}))

    await DB._put_many_fills(items)

    ltp = {p.get("tsym"): p.get("lp") for p in positions}
    cutoff = (now.date() - timedelta(days=CFG.REBUILD_LOOKBACK_DAYS)).isoformat()

    all_fills = await DB._all_fills()
    acc_fills = [f for f in all_fills if f.get("_acc") == acc["id"] and f.get("_day", "") >= cutoff]
    current_trades = await DB._all_trades()
    old_trades = {t["id"]: t for t in current_trades if t.get("account") == acc["id"] and t.get("origin") == "zebu"}

    charges_cfg = {
        "brokerage_per_order": CFG.CHG_BROKERAGE_PER_ORDER,
        "stt_sell_pct": CFG.CHG_STT_SELL_PCT,
        "txn_pct": CFG.CHG_TXN_PCT,
        "stamp_buy_pct": CFG.CHG_STAMP_BUY_PCT,
    }
    product_map = json.loads(CFG.PRODUCT_MAP)

    built = engine.build_trades(acc, acc_fills, charges_cfg, product_map, is_algo_order, now, ltp)

    for t in built:
        prev = old_trades.get(t["id"])
        if prev and prev.get("spot"):
            t["spot"] = prev["spot"]
        else:
            ts_val = engine.parse_ts(t.get("_ts")) or now
            t["spot"] = await spot_at_async(t["symbol"], ts_val)
        t.pop("_ts", None)

    for tid, t in old_trades.items():
        if t["date"] >= cutoff:
            await DB._delete_trade(tid)

    await DB._put_many_trades([(t["id"], t) for t in built])

    # Broadcast updates to dashboard websockets
    asyncio.create_task(WS_HUB.broadcast({"type": "trades_updated", "account": acc["id"]}))

    return f"ok · {len(fills)} fills today · {len(built)} trades rebuilt"


async def poll_all_async(reason: str = "schedule") -> dict:
    """Executes asynchronous concurrent polling across all accounts using ConnectionManager."""
    now = now_ist()
    accounts = await DB.get_all_accounts_async()
    results = {}

    tasks = []
    for acc in accounts:
        if acc.get("broker", "Zebu") != "Zebu" or not acc.get("zebu_uid"):
            continue

        async def _sync_single(a=acc):
            return await poll_account_async(a, now)

        tasks.append((acc["id"], CONN_MGR.sync_account_bounded(acc, _sync_single)))

    for aid, task in tasks:
        results[aid] = await task

    await DB._put_status("poll", {"last": now.isoformat(timespec="seconds"), "reason": reason, "results": results})
    await send_flag_alerts_async(now)
    await notify_expired_async(now, results)
    return results


async def all_flags_async():
    rules_list = await DB._all_rules()
    rules = {x["trader"]: x["rules"] for x in rules_list}
    trades = await DB._all_trades()
    return engine.check_trades(trades, rules)


async def send_flag_alerts_async(now: datetime):
    t0 = time.perf_counter()
    s = (await DB._get_setting("main")) or {}
    alerts_cfg = s.get("alerts") or DEFAULT_SETTINGS["alerts"]
    if not alerts_cfg.get("onHigh"):
        return
    today = now.date().isoformat()
    all_trades = await DB._all_trades()
    trades = {t["id"]: t for t in all_trades}
    all_traders = await DB._all_traders()
    traders = {t["id"]: t for t in all_traders}

    flags_dict = await all_flags_async()
    for tid, fl in flags_dict.items():
        t = trades.get(tid)
        if not t or t["date"] != today:
            continue
        for f in fl:
            if f["level"] != "High":
                continue
            key = f"{tid}|{f['rule']}"
            if await DB._get_sent_flag(key):
                continue
            name = traders.get(t["trader"], {}).get("name", t["trader"])
            text_msg = (
                f"🚩 Red flag · {name} · {t['time']}\nRule: {f['rule']}\nWhat he did: {f['did']}\n"
                f"{t['symbol']} {t['strike']:g} {t['opt']} · {t['lots']:g} lots · {t['account']}"
            )
            await ALERT.send_async(
                text=text_msg,
                settings_alerts=alerts_cfg,
                dedup_key=key,
                severity="High",
                metadata={"account_id": t["account"], "trader_id": t["trader"], "trade_id": tid, "rule_name": f["rule"]},
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
    trades = [t for t in all_trades if t["date"] == today]

    all_traders = await DB._all_traders()
    for tr in all_traders:
        mine = [t for t in trades if t["trader"] == tr["id"]]
        if not mine:
            continue
        pnl = sum(engine.net_pnl(t) for t in mine)
        nf = sum(len(flags.get(t["id"], [])) for t in mine)
        clean = sum(1 for t in mine if not flags.get(t["id"]))
        lines.append(f"{tr['name']}: {len(mine)} trades · P&L ₹{pnl:,.0f} · {nf} flags · rule-follow {clean / len(mine) * 100:.0f}%")

    total = sum(engine.net_pnl(t) for t in trades)
    lines.append(f"Total: ₹{total:,.0f} · {len(trades)} trades")
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
)


# ---------------- Lifespan & App Setup ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Initialize DB tables
    await init_models()

    # 2. Ensure initial admin exists
    admin_u = CFG.ADMIN_USER
    admin_existing = await DB.get_user_async(admin_u)
    if not admin_existing:
        await DB.put_user_async(admin_u, {"user": admin_u, "role": "admin", "pw": hash_pw(CFG.ADMIN_PASSWORD)})

    settings_existing = await DB._get_setting("main")
    if not settings_existing:
        await DB._put_setting("main", DEFAULT_SETTINGS)

    # 3. Start Connection Manager & Scheduler if configured for in-process execution
    if CFG.RUN_WORKER_IN_PROCESS:
        await CONN_MGR.start()
        # 4. Attach real-time streams for accounts with valid tokens
        all_accounts = await DB.get_all_accounts_async()
        for acc in all_accounts:
            tok = await token_for_account_async(acc["id"])
            if tok and acc.get("zebu_uid"):
                await CONN_MGR.register_account_stream(
                    account_id=acc["id"],
                    uid=acc["zebu_uid"],
                    actid=acc.get("zebu_actid") or acc["zebu_uid"],
                    token=tok,
                    on_fill=lambda msg, a=acc: poll_account_async(a, now_ist()),
                )

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
    # Keep attempts from last 60 seconds
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


async def link_state(acc: dict) -> str:
    t = await DB.get_token_async(acc["id"])
    if not t:
        return "Not linked"
    if t.get("expires_at") and t["expires_at"] < time.time():
        return "Login expired"
    return "Linked"


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
    await WS_HUB.connect(websocket)
    try:
        while True:
            # Keepalive / listen for ping
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        WS_HUB.disconnect(websocket)


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
    }
    if not doc["pw"]:
        raise HTTPException(400, "Password needed")
    await DB.put_user_async(name, doc)
    return {"ok": True}


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
    return d


@app.get("/api/accounts")
async def get_accounts(user=Depends(current_user)):
    out = []
    all_acc = await DB.get_all_accounts_async()

    # Account-level access control
    if user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        if tid:
            all_acc = [a for a in all_acc if a.get("trader") == tid]
    elif user["role"] == "viewer":
        u = await DB.get_user_async(user["user"])
        allowed = u.get("assigned_accounts") if u else None
        if allowed:
            all_acc = [a for a in all_acc if a.get("id") in allowed]

    for a in all_acc:
        t = (await DB.get_token_async(a["id"])) or {}
        conn_state = CONN_MGR.get_account_state(a["id"])
        out.append({
            **a,
            "link": await link_state(a),
            "linkedAt": t.get("linked_at"),
            "cliname": t.get("cliname"),
            "ws_status": conn_state.status,
            "circuit_open": conn_state.circuit_open,
        })
    return out


@app.put("/api/accounts/{aid}")
async def put_account(aid: str, request: Request, _=Depends(admin_only)):
    d = await request.json()
    doc = {k: d.get(k) for k in ("name", "broker", "trader", "zebu_uid", "zebu_actid")}
    doc["id"] = aid
    doc["zebu_uid"] = (doc.get("zebu_uid") or "").strip().upper()
    doc["zebu_actid"] = (doc.get("zebu_actid") or doc["zebu_uid"]).strip().upper()
    await DB.put_account_async(aid, doc)
    return doc


@app.post("/api/accounts/{aid}/link")
async def link_account(aid: str, _=Depends(admin_only)):
    if not await DB.get_account_async(aid):
        raise HTTPException(404, "No such account")
    if not CFG.ZEBU_CLIENT_ID:
        raise HTTPException(400, "ZEBU_CLIENT_ID is not set on the server")
    n = generate_link_nonce()
    await DB._put_link_nonce(n, {"account": aid, "exp": time.time() + 900})
    return {"url": f"{CFG.PUBLIC_URL}/auth/zebu/start?n={n}"}


@app.post("/api/accounts/{aid}/unlink")
async def unlink_account(aid: str, _=Depends(admin_only)):
    await DB.delete_token_async(aid)
    await CONN_MGR.unregister_account(aid)
    return {"ok": True}


@app.post("/api/accounts/{aid}/token")
async def save_manual_token(aid: str, request: Request, _=Depends(admin_only)):
    acc = await DB.get_account_async(aid)
    if not acc:
        raise HTTPException(404, "No such account")
    data = await request.json()
    token_str = (data.get("token") or "").strip()
    if not token_str:
        raise HTTPException(400, "Token is required")

    doc = {
        "access_token": token_str,
        "expires_at": time.time() + 86400,
        "linked_at": now_ist().isoformat(timespec="seconds"),
        "status": "Linked",
    }
    try:
        cd = await ZEBU.client_details_async(token_str, acc.get("zebu_uid"), acc.get("zebu_actid") or acc.get("zebu_uid"))
        doc["cliname"] = cd.get("cliname") if isinstance(cd, dict) else None
    except Exception as e:
        log.warning(f"Could not verify client details with manual token: {e}")

    await DB.put_token_async(aid, doc)

    # Attach WebSocket stream immediately
    await CONN_MGR.register_account_stream(
        account_id=acc["id"],
        uid=acc.get("zebu_uid", ""),
        actid=acc.get("zebu_actid") or acc.get("zebu_uid", ""),
        token=token_str,
        on_fill=lambda msg, a=acc: poll_account_async(a, now_ist()),
    )
    asyncio.create_task(poll_all_async("manual_token"))
    return {"ok": True, "cliname": doc.get("cliname")}


@app.get("/auth/zebu/start")
async def zebu_start(n: str):
    d = await DB._get_link_nonce(n)
    if not d or d["exp"] < time.time():
        return HTMLResponse("<h3>Link expired. Go back to the dashboard and click Link again.</h3>", 400)
    r = RedirectResponse(ZEBU.authorize_url())
    r.set_cookie("tts_link", n, max_age=900, httponly=True, samesite="lax", secure=CFG.PUBLIC_URL.startswith("https"))
    return r


@app.get("/auth/zebu/callback")
async def zebu_callback(request: Request, code: str = ""):
    n = request.cookies.get("tts_link", "")
    d = (await DB._get_link_nonce(n)) if n else None
    if not d or d["exp"] < time.time():
        return HTMLResponse("<h3>Link session missing or expired. Start again from the dashboard.</h3>", 400)
    if not code:
        return HTMLResponse("<h3>Zebu did not return a code.</h3>", 400)

    acc = await DB.get_account_async(d["account"])
    try:
        tok = await ZEBU.exchange_code_async(code)
    except ZebuError as e:
        return HTMLResponse(f"<h3>Zebu login failed: {e}</h3>", 400)

    exp_in = int(float(tok.get("expires_in") or 0))
    doc = {
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token"),
        "expires_at": time.time() + exp_in if exp_in else None,
        "linked_at": now_ist().isoformat(timespec="seconds"),
        "status": "Linked",
    }
    msg = ""
    try:
        cd = await ZEBU.client_details_async(doc["access_token"], acc.get("zebu_uid"), acc.get("zebu_actid") or acc.get("zebu_uid"))
        doc["cliname"] = cd.get("cliname") if isinstance(cd, dict) else None
    except ZebuError as e:
        msg = f"<p>Note: could not verify client {acc.get('zebu_uid')}: {e}. Check the Zebu UID on the account.</p>"

    await DB.put_token_async(d["account"], doc)
    await DB._delete_link_nonce(n)

    # Attach WebSocket stream immediately
    await CONN_MGR.register_account_stream(
        account_id=acc["id"],
        uid=acc.get("zebu_uid", ""),
        actid=acc.get("zebu_actid") or acc.get("zebu_uid", ""),
        token=doc["access_token"],
        on_fill=lambda msg, a=acc: poll_account_async(a, now_ist()),
    )

    # Trigger immediate sync
    asyncio.create_task(poll_all_async("linked"))

    return HTMLResponse(
        f"<div style='font-family:sans-serif;padding:40px'><h2>✅ {acc['name']} linked to Zebu</h2>{msg}"
        f"<p>Trades will appear in the dashboard within seconds. You can close this tab.</p>"
        f"<p><a href='{CFG.PUBLIC_URL}/'>Back to dashboard</a></p></div>"
    )


@app.get("/api/rules")
async def get_rules(_=Depends(current_user)):
    all_rules = await DB._all_rules()
    return {r["trader"]: {**engine.DEFAULT_RULES, **r["rules"]} for r in all_rules}


@app.put("/api/rules/{tid}")
async def put_rules(tid: str, request: Request, user=Depends(admin_only)):
    d = await request.json()
    await DB._put_rule(tid, {"trader": tid, "rules": d["rules"]})
    await DB._put_rule_log(secrets.token_hex(6), {
        "trader": tid,
        "at": now_ist().strftime("%Y-%m-%d %H:%M"),
        "by": user["user"],
        "change": d.get("change", "Rules updated"),
    })
    return {"ok": True}


@app.get("/api/rulelog")
async def get_rulelog(_=Depends(current_user)):
    logs = await DB._all_rule_logs()
    return sorted(logs, key=lambda x: x["at"])


@app.get("/api/trades")
async def get_trades(request: Request, user=Depends(current_user)):
    t0 = time.perf_counter()
    frm = request.query_params.get("from", "")
    all_trades = await DB._all_trades(from_date=frm if frm else None)

    # Account-level isolation
    if user["role"] == "trader":
        u = await DB.get_user_async(user["user"])
        tid = u.get("trader_id") if u else None
        if tid:
            all_trades = [t for t in all_trades if t.get("trader") == tid]
    elif user["role"] == "viewer":
        u = await DB.get_user_async(user["user"])
        allowed = u.get("assigned_accounts") if u else None
        if allowed:
            all_trades = [t for t in all_trades if t.get("account") in allowed]

    METRICS.record_api_latency((time.perf_counter() - t0) * 1000)
    return all_trades


@app.post("/api/trades/import")
async def import_trades(request: Request, _=Depends(admin_only)):
    rows = await request.json()
    items = []
    for r in rows:
        r["origin"] = r.get("origin", "import")
        r["id"] = r.get("id") or "I-" + secrets.token_hex(5)
        # Ensure account & trader exist to satisfy foreign key constraints
        aid = str(r.get("account") or "AC-01").strip()
        tid = str(r.get("trader") or "TR-001").strip()
        r["account"] = aid
        r["trader"] = tid
        if not await DB.get_account_async(aid):
            if not await DB._get_trader(tid):
                await DB._put_trader(tid, {"id": tid, "name": f"Trader {tid}", "status": "Testing"})
            await DB.put_account_async(aid, {"id": aid, "name": f"Account {aid}", "broker": "Manual", "trader": tid})
        elif not await DB._get_trader(tid):
            await DB._put_trader(tid, {"id": tid, "name": f"Trader {tid}", "status": "Testing"})
        items.append((r["id"], r))
    await DB._put_many_trades(items)
    return {"ok": True, "n": len(items)}


@app.delete("/api/trades/{tid}")
async def delete_trade(tid: str, _=Depends(admin_only)):
    t = await DB._get_trade(tid)
    if t and t.get("origin") == "zebu":
        raise HTTPException(400, "Trades from Zebu cannot be deleted (they come back on the next sync)")
    await DB._delete_trade(tid)
    return {"ok": True}


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
async def sync_now(_=Depends(admin_only)):
    return await poll_all_async("manual")


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
    poll_stat = (await DB._get_status("poll")) or {}
    return {
        "poll": poll_stat,
        "zebu_configured": bool(CFG.ZEBU_CLIENT_ID and CFG.ZEBU_SECRET_KEY),
        "redirect_url": CFG.PUBLIC_URL + "/auth/zebu/callback",
        "poll_seconds": CFG.POLL_SECONDS,
        "market": f"{CFG.MARKET_OPEN}–{CFG.MARKET_CLOSE} IST",
        "max_concurrent_accounts": CFG.MAX_CONCURRENT_ACCOUNTS,
        "active_streams": len([a for a, s in CONN_MGR.accounts.items() if s.status == "REAL-TIME"]),
    }


def get_frontend_dir() -> Path:
    # 1. Docker / production container layout: /app/static
    if (ROOT_DIR / "static").exists():
        return ROOT_DIR / "static"
    # 2. Local separated development layout: ../frontend
    if (ROOT_DIR.parent / "frontend").exists():
        return ROOT_DIR.parent / "frontend"
    return ROOT_DIR / "static"


FRONTEND_DIR = get_frontend_dir()


@app.get("/")
def index():
    html_file = FRONTEND_DIR / "index.html"
    if not html_file.exists():
        raise HTTPException(404, "Frontend file index.html not found.")
    return FileResponse(html_file)


# Mount static assets if present
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
