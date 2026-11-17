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
import httpx
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
        if bs.status == "Linked" and bs.access_token_enc:
            return decrypt_token(bs.access_token_enc)
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
    proxy_url = acc.get("proxy_url")

    try:
        fills = await ZEBU.trade_book_async(tok, uid, actid, proxy=proxy_url)
        positions = await ZEBU.position_book_async(tok, uid, actid, proxy=proxy_url)
    except SessionExpired:
        log.warning(f"Zebu session refresh notice for account {acc['id']}")
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

    async def _sync_one(acc_dict):
        async def _sync_single(a=acc_dict):
            return await poll_account_async(a, now)
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


@app.get("/api/test-proxy")
async def test_proxy(proxy: str = ""):
    p = proxy or "http://u084BWjx_2:HctOYpF0z5@79.127.168.43:50100"
    t0 = time.time()
    try:
        async with httpx.AsyncClient(proxy=p, timeout=5.0) as cl:
            r = await cl.get("https://api.ipify.org?format=json")
            return {"ok": True, "ip": r.json().get("ip"), "duration": round(time.time() - t0, 2)}
    except Exception as e:
        return {"ok": False, "error": str(e), "type": type(e).__name__, "duration": round(time.time() - t0, 2)}


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
    doc = {k: d.get(k) for k in ("name", "broker", "trader", "zebu_uid", "zebu_actid", "zebu_client_id", "zebu_secret_key", "proxy_url")}
    doc["id"] = aid
    doc["zebu_uid"] = (doc.get("zebu_uid") or "").strip().upper()
    doc["zebu_actid"] = (doc.get("zebu_actid") or doc["zebu_uid"]).strip().upper()
    doc["zebu_client_id"] = (doc.get("zebu_client_id") or doc["zebu_uid"]).strip()
    if doc.get("zebu_secret_key"):
        doc["zebu_secret_key"] = doc["zebu_secret_key"].strip()
    if doc.get("proxy_url"):
        doc["proxy_url"] = doc["proxy_url"].strip()
    await DB.put_account_async(aid, doc)
    return doc


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
    exp_in = 86400 * 3650  # Keep linked permanently (10 years) until disconnected
    exchange_error = None

    if is_code or len(token_str) < 50:
        try:
            client_id = acc.get("zebu_client_id") or uid or CFG.ZEBU_CLIENT_ID
            secret_key = acc.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY
            proxy_url = acc.get("proxy_url")
            exchanged = await ZEBU.exchange_code_async(token_str, uid=uid, client_id=client_id, secret_key=secret_key, proxy=proxy_url)
            tok = (exchanged.get("access_token") or exchanged.get("susertoken")) if isinstance(exchanged, dict) else None
            if tok:
                final_token = tok
                log.info(f"Successfully exchanged code for access token for account {aid} ({uid})")
            else:
                exchange_error = (exchanged or {}).get("emsg", "No access token received from Zebu")
        except Exception as e:
            exchange_error = str(e)
            log.warning(f"Failed to exchange code: {e}")

    # Validate the token with Zebu before marking as Linked!
    cliname = None
    try:
        cd = await ZEBU.client_details_async(final_token, uid, acc.get("zebu_actid") or uid, proxy=acc.get("proxy_url"))
        if isinstance(cd, dict):
            if cd.get("stat") == "Not_Ok":
                raise ZebuError(cd.get("emsg", "Session validation failed"))
            cliname = cd.get("cliname")
            returned_actid = (cd.get("actid") or cd.get("uid") or "").strip().upper()
            if returned_actid and uid and returned_actid != uid:
                raise ZebuError(f"Session belongs to client '{returned_actid}', but this account is configured for '{uid}'. Please log in as '{uid}'.")
    except Exception as e:
        log.warning(f"Token validation failed for {aid}: {e}")
        if is_code:
            raise HTTPException(
                400,
                f"Zebu authentication failed: {exchange_error or e}. Check that you logged into Zebu as '{uid}' and that the authorization code was not already used."
            )
        raise HTTPException(
            400,
            f"Invalid Zebu token for {uid}: {e}. Zebu rejected this token."
        )

    doc = {
        "access_token": final_token,
        "expires_at": time.time() + exp_in,
        "linked_at": now_ist().isoformat(timespec="seconds"),
        "status": "Linked",
        "cliname": cliname,
    }
    await DB.put_token_async(aid, doc)

    # Attach WebSocket stream immediately
    await CONN_MGR.register_account_stream(
        account_id=acc["id"],
        uid=uid,
        actid=acc.get("zebu_actid") or uid,
        token=final_token,
        on_fill=lambda msg, a=acc: poll_account_async(a, now_ist()),
    )
    asyncio.create_task(poll_all_async("manual_token"))
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
        nonce_doc = {
            "account": account,
            "client_id": clean_cid,
            "secret_key": secret_key,
            "proxy_url": proxy_url,
            "uid": acc.get("zebu_uid") or "",
            "exp": time.time() + 900,
            "frontend_url": frontend_target,
        }
        LINK_NONCE_MAP[n] = nonce_doc
        await DB._put_link_nonce(n, nonce_doc)

    d = LINK_NONCE_MAP.get(n) or await DB._get_link_nonce(n)
    if not d or d.get("exp", 0) < time.time():
        return HTMLResponse("<h3>Link expired. Go back to the dashboard and click Connect Zebu again.</h3>", 400)

    cb_url = "https://tts-backend-ezm3.onrender.com/auth/zebu/callback"
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
        # Fallback 1: check most recently created active link nonce in DB
        from app.models import LinkNonce
        async with AsyncSessionLocal() as session:
            stmt = select(LinkNonce).where(LinkNonce.exp > datetime.now(timezone.utc)).order_by(LinkNonce.created_at.desc())
            latest_nonce = (await session.execute(stmt)).scalars().first()
            if latest_nonce:
                exp_ts = latest_nonce.exp.timestamp() if latest_nonce.exp else time.time() + 900
                d = LINK_NONCE_MAP.get(latest_nonce.nonce) or {"account": latest_nonce.account_id, "exp": exp_ts}
                n = latest_nonce.nonce

    # Fallback 2: if d is still missing, auto-detect the configured Zebu account
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
            async with httpx.AsyncClient(timeout=3, proxy=proxy_url) as cl:
                res = await cl.get("https://api.ipify.org?format=json")
                server_ip_str = res.json().get("ip", "")
        except Exception:
            pass

        ip_hint = f"""<div style='background:#1e293b;border:1px solid #f59e0b;border-radius:8px;padding:12px;margin:16px auto;max-width:420px;text-align:left;'>
            <div style='color:#f59e0b;font-weight:600;font-size:13px;'>⚠️ Zebu IP Whitelist Notice</div>
            <div style='color:#cbd5e1;font-size:12px;margin-top:4px;'>Only the primary Developer App Key (<code>Z58805_U</code>) needs this IP mapped. Client accounts (like <code>{uid}</code>) do NOT need an API key and must NOT map the IP.</div>
            <div style='margin-top:8px;color:#94a3b8;font-size:12px;'>Server Outbound IP: <code style='background:#0f172a;color:#38bdf8;padding:2px 6px;border-radius:4px;'>{server_ip_str}</code></div>
        </div>""" if ("INVALID_IP" in str(e) and server_ip_str) else ""

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
        <div style='margin-top:16px;background:#0f172a;border-radius:8px;padding:14px;text-align:left;font-size:12px;color:#94a3b8;'>
            <b>Tip for Multiple Accounts:</b> If you were already signed into another Zebu account in this browser, Zebu re-uses that session.<br><br>
            Please open an <b>Incognito / Private Window</b>, log into Zebu as <b>{uid}</b>, and authorize.
        </div>
        <p style='margin-top:20px;'><a href='{target_url}' style='color:#38bdf8;text-decoration:none;font-weight:600;'>← Return to Dashboard</a></p>
    </div>
</body>
</html>""", 400
        )

    access_token = tok["access_token"]
    cliname = None
    mismatch_error = None
    try:
        cd = await ZEBU.client_details_async(access_token, uid, acc.get("zebu_actid") or uid, proxy=proxy_url)
        if isinstance(cd, dict):
            if cd.get("stat") == "Not_Ok":
                mismatch_error = cd.get("emsg", "Validation rejected by Zebu")
            else:
                cliname = cd.get("cliname")
                returned_actid = (cd.get("actid") or cd.get("uid") or "").strip().upper()
                if returned_actid and uid and returned_actid != uid:
                    mismatch_error = f"Zebu token belongs to client '{returned_actid}', but this account is configured for '{uid}'"
    except Exception as ex:
        mismatch_error = str(ex)

    if mismatch_error:
        log.warning(f"Zebu callback mismatch or validation error for {acc['id']} ({uid}): {mismatch_error}")
        return HTMLResponse(
            f"""<!DOCTYPE html>
<html>
<head><title>Zebu Account Mismatch</title></head>
<body style="font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0f172a;color:#f8fafc;">
    <div style="text-align:center;padding:30px;background:#1e293b;border-radius:12px;border:1px solid #ef4444;max-width:480px;">
        <h2 style="color:#ef4444;margin-top:0;">⚠️ Account Mismatch Detected</h2>
        <p style="color:#cbd5e1;font-size:14px;">
            The Zebu session in your browser does not match the configured client ID for <b>{acc.get('name', 'this account')}</b>.
        </p>
        <p style="color:#f87171;font-size:13px;font-family:monospace;background:#0f172a;padding:8px;border-radius:6px;">
            {mismatch_error}
        </p>
        <div style="background:#0f172a;border-radius:8px;padding:14px;margin:16px 0;text-align:left;font-size:13px;color:#94a3b8;">
            <b>Why did this happen?</b><br>
            Your browser was still logged into another Zebu account from a previous login.
            <div style="margin-top:8px;color:#cbd5e1;"><b>To connect {acc.get('name')} ({uid}):</b></div>
            <ol style="margin:6px 0 0 18px;padding:0;line-height:1.6;">
                <li>Open a new <b>Incognito / Private Window</b> in your browser.</li>
                <li>Go to the dashboard and click <b>Link Zebu</b> for <b>{acc.get('name')}</b>.</li>
                <li>Log in using <b>{uid}</b>'s User ID and Password, then click Authorize.</li>
            </ol>
        </div>
        <p><a href="{target_url}" style="color:#38bdf8;text-decoration:none;font-weight:600;">← Return to Dashboard</a></p>
    </div>
</body>
</html>""", 400
        )

    exp_in = 86400 * 3650  # Keep linked permanently (10 years) until disconnected by user

    doc = {
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token"),
        "expires_at": time.time() + exp_in,
        "linked_at": now_ist().isoformat(timespec="seconds"),
        "status": "Linked",
        "cliname": cliname,
    }
    await DB.put_token_async(d["account"], doc)
    await DB._delete_link_nonce(n)
    LINK_NONCE_MAP.pop(n, None)

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
        f"""<!DOCTYPE html>
<html>
<head>
    <title>Zebu Account Linked</title>
</head>
<body style="font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0f172a;color:#f8fafc;">
    <div style="text-align:center;padding:24px;background:#1e293b;border-radius:12px;border:1px solid #334155;max-width:440px;">
        <h2 style="color:#22c55e;margin-top:0;">✅ {acc['name']} Linked!</h2>
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
    try:
        rows = await request.json()
        items = []
        for r in rows:
            r["origin"] = "import"
            r["id"] = r.get("id") or "I-" + secrets.token_hex(5)
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
            if "qty" not in r or r["qty"] is None:
                r["qty"] = float(r.get("lots") or 1) * int(r.get("lotSize") or 1)
            items.append((r["id"], r))
        await DB._put_many_trades(items)
        return {"ok": True, "n": len(items)}
    except Exception as e:
        log.exception(f"Import trades error: {e}")
        raise HTTPException(500, f"Import failed: {str(e)}")


@app.delete("/api/trades/{tid}")
async def delete_trade(tid: str, _=Depends(admin_only)):
    await DB._delete_trade(tid)
    return {"ok": True}


@app.post("/api/trades/delete-bulk")
async def delete_trades_bulk(request: Request, _=Depends(admin_only)):
    d = await request.json()
    ids = d.get("ids", [])
    count = 0
    for tid in ids:
        await DB._delete_trade(tid)
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


@app.get("/api/server-ip")
async def server_ip():
    try:
        async with httpx.AsyncClient(timeout=4) as cl:
            r = await cl.get("https://api.ipify.org?format=json")
            return r.json()
    except Exception as e:
        return {"error": str(e)}


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
async def index(request: Request, code: str = "", state: str = ""):
    if code:
        return await zebu_callback(request, code=code, state=state)
    html_file = FRONTEND_DIR / "index.html"
    if html_file.exists():
        return FileResponse(html_file)
    return RedirectResponse(CFG.FRONTEND_URL, status_code=302)


# Mount static assets if present
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
