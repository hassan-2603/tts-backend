"""Validation Suite for TTS Financial Data Consistency — Correction Phase (Priorities 1-5).

Covers:
1. Single-account trader manual sync.
2. Multi-account trader concurrent manual sync with account-wise results.
3. One account failing while sibling account succeeds (isolated error handling).
4. Manual forced refresh after 15:30 IST EOD finalization with versioned reconciliation audit note.
5. Partially closed contracts: FIFO execution ledger preserved, open MTM = exact urmtom (no double counting),
   broker PositionBook rpnl/urmtom exposed separately on /api/accounts.
6. Fully closed contracts & duplicate synchronization idempotency.
7. >180-day historical trades: bootstrap sets has_older_trades=True while GET /api/trades returns full history + pagination.
8. Account-level isolation on POST /api/sync (HTTP 403 for unauthorized accounts).
"""
import asyncio
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List
import pytest
from fastapi.testclient import TestClient

from app.database import set_engine_url, init_models
from app.security import hash_pw
from app.zebu import SessionExpired, ZebuError

test_db_path = tempfile.mktemp(suffix="_correction_phase.db")
set_engine_url(f"sqlite+aiosqlite:///{test_db_path}")

import app.main as main_mod  # noqa: E402
from app.main import app, DB, ZEBU, finalize_eod_async  # noqa: E402
from app.scheduler import now_ist  # noqa: E402


class MockBrokerMulti:
    def __init__(self):
        self.trade_books: Dict[str, List[dict]] = {}
        self.position_books: Dict[str, List[dict]] = {}
        self.order_books: Dict[str, List[dict]] = {}
        self.errors: Dict[str, Exception] = {}
        self.calls: Dict[str, int] = {}

    async def trade_book_async(self, token: str, uid: str, actid: str, proxy=None) -> List[dict]:
        u = uid.upper()
        self.calls[f"tb:{u}"] = self.calls.get(f"tb:{u}", 0) + 1
        if u in self.errors:
            raise self.errors[u]
        return list(self.trade_books.get(u, []))

    async def position_book_async(self, token: str, uid: str, actid: str, proxy=None) -> List[dict]:
        u = uid.upper()
        self.calls[f"pb:{u}"] = self.calls.get(f"pb:{u}", 0) + 1
        if u in self.errors:
            raise self.errors[u]
        return list(self.position_books.get(u, []))

    async def order_book_async(self, token: str, uid: str, proxy=None) -> List[dict]:
        u = uid.upper()
        if u in self.errors:
            raise self.errors[u]
        return list(self.order_books.get(u, []))


@pytest.fixture(scope="module")
def ctx():
    asyncio.run(init_models())
    mock = MockBrokerMulti()
    orig_tb, orig_pb, orig_ob = ZEBU.trade_book_async, ZEBU.position_book_async, ZEBU.order_book_async
    ZEBU.trade_book_async = mock.trade_book_async
    ZEBU.position_book_async = mock.position_book_async
    ZEBU.order_book_async = mock.order_book_async

    async def _seed():
        await DB.put_user_async("admin", {"user": "admin", "role": "admin", "pw": hash_pw("admin123")})
        await DB.put_user_async("trader_single", {"user": "trader_single", "role": "trader", "trader_id": "TR-SINGLE", "pw": hash_pw("tr123")})

        await DB._put_trader("TR-MULTI", {"id": "TR-MULTI", "name": "Multi Account Trader", "status": "Live"})
        await DB._put_trader("TR-SINGLE", {"id": "TR-SINGLE", "name": "Single Account Trader", "status": "Live"})

        await DB.put_account_async("ACC-M1", {"id": "ACC-M1", "name": "Multi Acc 1", "broker": "Zebu", "trader": "TR-MULTI", "zebu_uid": "UID_M1", "zebu_actid": "UID_M1"})
        await DB.put_account_async("ACC-M2", {"id": "ACC-M2", "name": "Multi Acc 2", "broker": "Zebu", "trader": "TR-MULTI", "zebu_uid": "UID_M2", "zebu_actid": "UID_M2"})
        await DB.put_account_async("ACC-S1", {"id": "ACC-S1", "name": "Single Acc 1", "broker": "Zebu", "trader": "TR-SINGLE", "zebu_uid": "UID_S1", "zebu_actid": "UID_S1"})

        await DB.put_token_async("ACC-M1", {"access_token": "TOK_M1", "status": "Linked"})
        await DB.put_token_async("ACC-M2", {"access_token": "TOK_M2", "status": "Linked"})
        await DB.put_token_async("ACC-S1", {"access_token": "TOK_S1", "status": "Linked"})

        # Seed a historical trade older than 180 days (220 days ago) for Priority 3 testing
        old_day = (now_ist().date() - timedelta(days=220)).isoformat()
        await DB._put_trade("T-OLD-220", {
            "id": "T-OLD-220",
            "date": old_day,
            "time": "10:15",
            "exitDate": old_day,
            "exitTime": "11:00",
            "trader": "TR-SINGLE",
            "account": "ACC-S1",
            "symbol": "NIFTY",
            "tsym": "NIFTY25APR26C24000",
            "side": "BUY",
            "qty": 75,
            "lots": 1,
            "lotSize": 75,
            "entry": 100.0,
            "exit": 140.0,
            "pnl": 3000.0,
            "realized_pnl": 3000.0,
            "net_pnl": 2950.0,
            "charges": 50.0,
            "open": False,
            "is_finalized": True,
            "origin": "zebu",
        })

    asyncio.run(_seed())
    client = TestClient(app)

    def _hdr(u, p):
        r = client.post("/api/login", json={"user": u, "password": p})
        assert r.status_code == 200
        return {"Authorization": f"Bearer {r.json()['token']}"}

    yield {
        "client": client,
        "mock": mock,
        "admin": _hdr("admin", "admin123"),
        "trader_single": _hdr("trader_single", "tr123"),
    }
    ZEBU.trade_book_async, ZEBU.position_book_async, ZEBU.order_book_async = orig_tb, orig_pb, orig_ob


def test_p1_multi_account_sync_and_partial_failure_isolation(ctx):
    """Priority 1: Syncing TR-MULTI syncs BOTH ACC-M1 and ACC-M2 concurrently;
    if ACC-M2 fails with broker error, ACC-M1 still succeeds cleanly."""
    mock, client, admin = ctx["mock"], ctx["client"], ctx["admin"]
    today_fmt = now_ist().strftime("%d-%m-%Y")

    mock.trade_books["UID_M1"] = [
        {"norenordno": "O1", "flid": "F1", "fltm": f"09:30:00 {today_fmt}", "tsym": "NIFTY23NOV26C25500", "exch": "NFO", "trantype": "B", "prd": "M", "ls": "75", "flqty": "75", "flprc": "100.00"},
        {"norenordno": "O2", "flid": "F2", "fltm": f"10:00:00 {today_fmt}", "tsym": "NIFTY23NOV26C25500", "exch": "NFO", "trantype": "S", "prd": "M", "ls": "75", "flqty": "75", "flprc": "120.00"},
    ]
    mock.position_books["UID_M1"] = [
        {"stat": "Ok", "exch": "NFO", "tsym": "NIFTY23NOV26C25500", "token": "55001", "prd": "M", "netqty": "0", "lp": "120.00", "rpnl": "1500.00", "urmtom": "0.00"}
    ]
    mock.trade_books["UID_M2"] = [
        {"norenordno": "O3", "flid": "F3", "fltm": f"09:45:00 {today_fmt}", "tsym": "NIFTY23NOV26P25000", "exch": "NFO", "trantype": "S", "prd": "M", "ls": "75", "flqty": "75", "flprc": "80.00"},
    ]
    mock.position_books["UID_M2"] = [
        {"stat": "Ok", "exch": "NFO", "tsym": "NIFTY23NOV26P25000", "token": "55002", "prd": "M", "netqty": "-75", "lp": "60.00", "rpnl": "0.00", "urmtom": "1500.00"}
    ]

    # 1. Both accounts succeed concurrently
    r = client.post("/api/sync?account=TR-MULTI&force=1", headers=admin)
    assert r.status_code == 200
    d = r.json()
    assert d["accounts_synced"] == 2
    assert d["succeeded"] == 2
    assert d["failed"] == 0
    assert "ACC-M1" in d["results"] and "ACC-M2" in d["results"]
    assert d["results"]["ACC-M1"]["ok"] is True
    assert d["results"]["ACC-M2"]["ok"] is True

    # 2. One account fails while sibling succeeds
    mock.errors["UID_M2"] = ZebuError("Gateway timeout on M2")
    try:
        r2 = client.post("/api/sync?account=TR-MULTI&force=1", headers=admin)
        assert r2.status_code == 200
        d2 = r2.json()
        assert d2["accounts_synced"] == 2
        assert d2["succeeded"] == 1
        assert d2["failed"] == 1
        assert d2["results"]["ACC-M1"]["ok"] is True
        assert d2["results"]["ACC-M2"]["ok"] is False
    finally:
        mock.errors.pop("UID_M2", None)

    # 3. Account-level authorization: trader_single cannot sync TR-MULTI (HTTP 403)
    r_forbid = client.post("/api/sync?account=TR-MULTI&force=1", headers=ctx["trader_single"])
    assert r_forbid.status_code == 403


def test_p2_partially_closed_fifo_preserved_and_broker_pnl_separated(ctx):
    """Priority 2: Partially closed contract keeps genuine FIFO closed-trade P&L intact,
    sets open leg MTM strictly to PositionBook urmtom (no double counting), and exposes
    broker_rpnl / broker_urmtom / broker_total_pnl separately on /api/accounts."""
    mock, client, admin = ctx["mock"], ctx["client"], ctx["admin"]
    today_fmt = now_ist().strftime("%d-%m-%Y")

    # Buy 75 @ 100, Buy 75 @ 140 (avg 120), Sell 75 @ 130
    # FIFO closed P&L = (130 - 100) * 75 = +2250.00
    # Zebu weighted-average rpnl = (130 - 120) * 75 = +750.00
    # Open 75 @ LTP 150 -> Zebu urmtom = (150 - 120) * 75 = +2250.00
    mock.trade_books["UID_S1"] = [
        {"norenordno": "S1", "flid": "SF1", "fltm": f"09:20:00 {today_fmt}", "tsym": "NIFTY23NOV26C25900", "exch": "NFO", "trantype": "B", "prd": "M", "ls": "75", "flqty": "75", "flprc": "100.00"},
        {"norenordno": "S2", "flid": "SF2", "fltm": f"09:30:00 {today_fmt}", "tsym": "NIFTY23NOV26C25900", "exch": "NFO", "trantype": "B", "prd": "M", "ls": "75", "flqty": "75", "flprc": "140.00"},
        {"norenordno": "S3", "flid": "SF3", "fltm": f"10:00:00 {today_fmt}", "tsym": "NIFTY23NOV26C25900", "exch": "NFO", "trantype": "S", "prd": "M", "ls": "75", "flqty": "75", "flprc": "130.00"},
    ]
    mock.position_books["UID_S1"] = [
        {"stat": "Ok", "exch": "NFO", "tsym": "NIFTY23NOV26C25900", "token": "55900", "prd": "M", "netqty": "75", "lp": "150.00", "rpnl": "750.00", "urmtom": "2250.00"}
    ]

    r = client.post("/api/sync?account=ACC-S1&force=1", headers=admin)
    assert r.status_code == 200

    # Verify individual trades: FIFO closed leg = +2250 (NOT mutated to 750!), open leg = +2250 (exact urmtom!)
    tr_list = client.get("/api/trades?account=ACC-S1", headers=admin).json()
    today_str = now_ist().date().isoformat()
    today_s1 = [t for t in tr_list if t["date"] == today_str]
    closed_legs = [t for t in today_s1 if not t["open"]]
    open_legs = [t for t in today_s1 if t["open"]]
    assert len(closed_legs) == 1
    assert len(open_legs) == 1
    assert closed_legs[0]["realized_pnl"] == 2250.0
    assert open_legs[0]["unrealized_pnl"] == 2250.0

    # Verify broker-reported rpnl & urmtom are exposed separately on /api/accounts
    accs = client.get("/api/accounts", headers=admin).json()
    s1 = next(a for a in accs if a["id"] == "ACC-S1")
    assert s1["broker_rpnl"] == 750.0
    assert s1["broker_urmtom"] == 2250.0
    assert s1["broker_total_pnl"] == 3000.0


def test_p3_remove_180_day_limit_with_bootstrap_flag_and_full_history(ctx):
    """Priority 3: /api/bootstrap?from=<TODAY-180> stays fast and sets has_older_trades=True,
    while GET /api/trades (full history) returns trades older than 180 days + supports pagination."""
    client, admin = ctx["client"], ctx["admin"]
    cutoff_180 = (now_ist().date() - timedelta(days=180)).isoformat()

    boot = client.get(f"/api/bootstrap?from={cutoff_180}", headers=admin).json()
    assert boot["has_older_trades"] is True
    assert not any(t["id"] == "T-OLD-220" for t in boot["trades"])

    # Full history fetch includes the 220-day-old trade
    all_tr = client.get("/api/trades", headers=admin).json()
    assert any(t["id"] == "T-OLD-220" for t in all_tr)

    # Pagination works cleanly
    page1 = client.get("/api/trades?limit=2&offset=0", headers=admin).json()
    assert len(page1) <= 2


def test_p4_eod_finalization_and_post_eod_versioned_forced_reconciliation(ctx):
    """Priority 4: EOD finalization locks today's report, and a subsequent manual forced sync
    records a versioned reconciliation audit entry ([v2 @ ...]) without creating duplicates."""
    mock, client, admin = ctx["mock"], ctx["client"], ctx["admin"]
    today_str = now_ist().date().isoformat()

    # Finalize EOD
    eod_dt = now_ist().replace(hour=15, minute=45, second=0, microsecond=0)
    asyncio.run(finalize_eod_async(now=eod_dt, target_date=today_str))

    # Late broker settlement price update (LTP moves from 150 -> 160, urmtom 2250 -> 3000)
    mock.position_books["UID_S1"][0]["lp"] = "160.00"
    mock.position_books["UID_S1"][0]["urmtom"] = "3000.00"

    # Manual forced sync after EOD
    r = client.post("/api/sync?account=ACC-S1&force=1", headers=admin)
    assert r.status_code == 200

    # Verify single DailyAccountReport row with versioned correction note
    reps = asyncio.run(DB.get_daily_reports_async(account_id="ACC-S1"))
    today_reps = [x for x in reps if x["account"] == "ACC-S1" and x["date"] == today_str]
    assert len(today_reps) == 1
    assert "[v2 @" in (today_reps[0].get("reconciliation_notes") or "")
    assert today_reps[0]["unrealized_pnl"] == 3000.0
