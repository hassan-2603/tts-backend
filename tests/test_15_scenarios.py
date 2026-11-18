"""Comprehensive Production Validation Suite: 15 Mandatory Test Scenarios.

Tests:
- TEST 1: Connect multiple broker accounts and verify that each account displays its own live data.
- TEST 2: During market hours, verify that live positions, executions, and P&L update automatically.
- TEST 3: Verify that switching accounts displays the correct account information without mixing data.
- TEST 4: Close a trade and verify that its actual realized P&L is recorded correctly.
- TEST 5: Partially close a position and verify that the remaining quantity stays active.
- TEST 6: Leave a position open overnight and verify that it continues the next trading day without duplication.
- TEST 7: End the trading session and verify final reconciliation and daily finalization.
- TEST 8: Simulate midnight processing and verify that closed trades are archived while open positions remain active.
- TEST 9: Verify that the Daily P&L Calendar matches the underlying finalized records.
- TEST 10: Restart the backend and verify that connected accounts, historical records, rules, and red flags persist.
- TEST 11: Disconnect an account manually and verify that synchronization stops for that account.
- TEST 12: Expire an authentication token and verify that the application detects the failure correctly.
- TEST 13: Break a configured trading rule and verify that the correct red flag and explanation appear.
- TEST 14: Run synchronization and finalization multiple times and verify that no duplicate trades or historical records are created.
- TEST 15: Verify that historical P&L remains unchanged after subsequent trading sessions.
"""
import asyncio
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import pytest
from fastapi.testclient import TestClient

from app.database import set_engine_url, init_models
from app.security import hash_pw
from app.zebu import SessionExpired, ZebuError
import app.main as main_mod
from app.main import (
    app,
    DB,
    ZEBU,
    poll_account_async,
    poll_all_async,
    finalize_eod_async,
    midnight_archive_async,
    reconcile_daily_calendar_async,
    evaluate_and_record_violations_async,
    link_state,
)

test_db_path = tempfile.mktemp(suffix="_15_scenarios.db")
set_engine_url(f"sqlite+aiosqlite:///{test_db_path}")


class SimulatedBrokerState:
    """In-memory multi-account Zebu broker simulator returning realistic Noren API responses."""

    def __init__(self):
        self.trade_books: Dict[str, List[dict]] = {}
        self.position_books: Dict[str, List[dict]] = {}
        self.order_books: Dict[str, List[dict]] = {}
        self.expired_uids: set = set()
        self.offline_uids: set = set()
        self.spot_price: float = 51800.0
        self.sync_call_counts: Dict[str, int] = {}

    async def trade_book_async(self, token: str, uid: str, actid: str, proxy=None) -> List[dict]:
        u = uid.upper()
        self.sync_call_counts[u] = self.sync_call_counts.get(u, 0) + 1
        if u in self.expired_uids or token == "EXPIRED_TOK":
            raise SessionExpired("Session Expired : Invalid Session Key")
        if u in self.offline_uids:
            raise ZebuError("TradeBook network error: Broker gateway timeout")
        return list(self.trade_books.get(u, []))

    async def position_book_async(self, token: str, uid: str, actid: str, proxy=None) -> List[dict]:
        u = uid.upper()
        if u in self.expired_uids or token == "EXPIRED_TOK":
            raise SessionExpired("Session Expired : Invalid Session Key")
        if u in self.offline_uids:
            raise ZebuError("PositionBook network error: Broker gateway timeout")
        return list(self.position_books.get(u, []))

    async def order_book_async(self, token: str, uid: str, proxy=None) -> List[dict]:
        u = uid.upper()
        if u in self.expired_uids or token == "EXPIRED_TOK":
            raise SessionExpired("Session Expired : Invalid Session Key")
        if u in self.offline_uids:
            raise ZebuError("OrderBook network error: Broker gateway timeout")
        return list(self.order_books.get(u, []))

    async def index_list_async(self, token: str, uid: str, exch: str = "NSE", proxy=None) -> List[dict]:
        return [{"idxname": "NIFTY BANK", "token": "26009"}, {"idxname": "NIFTY 50", "token": "26000"}]

    async def tp_series_async(self, token: str, uid: str, exch: str, sym_token, st: int, et: int, intrv: str = "1", proxy=None) -> List[dict]:
        dt_str = datetime.fromtimestamp(st).strftime("%d-%m-%Y 09:25:00")
        return [{"stat": "Ok", "time": dt_str, "into": str(self.spot_price), "intc": str(self.spot_price)}]


BROKER = SimulatedBrokerState()


@pytest.fixture(scope="module", autouse=True)
def setup_environment(monkeypatch_module=None):
    asyncio.run(init_models())
    DB.put("users", "admin_val", {"user": "admin_val", "role": "admin", "pw": hash_pw("valpass123")})

    # Hook simulated broker into ZEBU singleton
    ZEBU.trade_book_async = BROKER.trade_book_async
    ZEBU.position_book_async = BROKER.position_book_async
    ZEBU.order_book_async = BROKER.order_book_async
    ZEBU.index_list_async = BROKER.index_list_async
    ZEBU.tp_series_async = BROKER.tp_series_async


def _get_auth_headers(client: TestClient) -> dict:
    resp = client.post("/api/login", json={"user": "admin_val", "password": "valpass123"})
    assert resp.status_code == 200
    return {"Authorization": f"Bearer {resp.json()['token']}"}


@pytest.mark.asyncio
async def test_01_and_03_multi_account_connection_and_isolation():
    """TEST 1 & TEST 3: Connect multiple broker accounts and verify each displays its own live data & switching isolates data."""
    client = TestClient(app)
    headers = _get_auth_headers(client)

    # Create Account 1 (UID101) and Account 2 (UID202)
    client.put("/api/traders/TR-A", headers=headers, json={"name": "Trader Alpha", "style": "Manual", "holding": "Intraday", "testCapital": 300000}).raise_for_status()
    client.put("/api/traders/TR-B", headers=headers, json={"name": "Trader Beta", "style": "Algo", "holding": "Positional", "testCapital": 500000}).raise_for_status()

    client.put("/api/accounts/ACC-1", headers=headers, json={"name": "Alpha Zebu", "broker": "Zebu", "trader": "TR-A", "zebu_uid": "UID101"}).raise_for_status()
    client.put("/api/accounts/ACC-2", headers=headers, json={"name": "Beta Zebu", "broker": "Zebu", "trader": "TR-B", "zebu_uid": "UID202"}).raise_for_status()

    await DB.put_token_async("ACC-1", {"access_token": "TOK_101", "status": "Linked", "cliname": "Alpha Client"})
    await DB.put_token_async("ACC-2", {"access_token": "TOK_202", "status": "Linked", "cliname": "Beta Client"})

    # Setup distinct broker fills on Day 1 (2026-11-17)
    day1_str = "17-11-2026"
    BROKER.trade_books["UID101"] = [
        {
            "norenordno": "ORD101_E1",
            "flid": "FL101_1",
            "tsym": "BANKNIFTY26NOV26C51700",
            "exch": "NFO",
            "trantype": "B",
            "prd": "I",
            "flqty": "30",
            "flprc": "300.00",
            "ls": "30",
            "fltm": f"{day1_str} 09:30:00",
            "ordersource": "WEB",
        }
    ]
    BROKER.position_books["UID101"] = [
        {"tsym": "BANKNIFTY26NOV26C51700", "exch": "NFO", "prd": "I", "netqty": "30", "lp": "320.00", "rpnl": "0.00", "urmtom": "600.00"}
    ]

    BROKER.trade_books["UID202"] = [
        {
            "norenordno": "ORD202_E1",
            "flid": "FL202_1",
            "tsym": "NIFTY26NOV26C24000",
            "exch": "NFO",
            "trantype": "S",
            "prd": "M",
            "flqty": "50",
            "flprc": "150.00",
            "ls": "25",
            "fltm": f"{day1_str} 09:45:00",
            "ordersource": "API",
        }
    ]
    BROKER.position_books["UID202"] = [
        {"tsym": "NIFTY26NOV26C24000", "exch": "NFO", "prd": "M", "netqty": "-50", "lp": "140.00", "rpnl": "0.00", "urmtom": "500.00"}
    ]

    day1_now = datetime(2026, 11, 17, 10, 0, 0)
    acc1 = await DB.get_account_async("ACC-1")
    acc2 = await DB.get_account_async("ACC-2")
    await poll_account_async(acc1, day1_now)
    await poll_account_async(acc2, day1_now)

    # Verify strict isolation via API
    t_acc1 = client.get("/api/trades?account=ACC-1", headers=headers).json()
    t_acc2 = client.get("/api/trades?account=ACC-2", headers=headers).json()

    assert len(t_acc1) == 1
    assert t_acc1[0]["account"] == "ACC-1"
    assert t_acc1[0]["symbol"] == "BANKNIFTY"
    assert t_acc1[0]["open"] is True
    assert t_acc1[0]["unrealized_pnl"] == 600.0

    assert len(t_acc2) == 1
    assert t_acc2[0]["account"] == "ACC-2"
    assert t_acc2[0]["symbol"] == "NIFTY"
    assert t_acc2[0]["side"] == "SELL"
    assert t_acc2[0]["open"] is True
    assert t_acc2[0]["unrealized_pnl"] == 500.0


@pytest.mark.asyncio
async def test_02_and_04_live_updates_and_full_trade_closure():
    """TEST 2 & TEST 4: Live positions/executions/P&L update during market hours, and closing a trade records exact realized P&L."""
    day1_str = "17-11-2026"
    # Update LTP first (market hours live tick)
    BROKER.position_books["UID101"] = [
        {"tsym": "BANKNIFTY26NOV26C51700", "exch": "NFO", "prd": "I", "netqty": "30", "lp": "335.00", "rpnl": "0.00", "urmtom": "1050.00"}
    ]
    acc1 = await DB.get_account_async("ACC-1")
    await poll_account_async(acc1, datetime(2026, 11, 17, 10, 15, 0))

    trades = [t for t in await DB._all_trades() if t["account"] == "ACC-1"]
    assert len(trades) == 1
    assert trades[0]["open"] is True
    assert trades[0]["exit"] == 335.0
    assert trades[0]["unrealized_pnl"] == 1050.0  # (335 - 300) * 30

    # Now close the trade on ACC-1 at 350.00
    BROKER.trade_books["UID101"].append({
        "norenordno": "ORD101_X1",
        "flid": "FL101_2",
        "tsym": "BANKNIFTY26NOV26C51700",
        "exch": "NFO",
        "trantype": "S",
        "prd": "I",
        "flqty": "30",
        "flprc": "350.00",
        "ls": "30",
        "fltm": f"{day1_str} 11:00:00",
        "ordersource": "WEB",
    })
    BROKER.position_books["UID101"] = [
        {"tsym": "BANKNIFTY26NOV26C51700", "exch": "NFO", "prd": "I", "netqty": "0", "lp": "350.00", "rpnl": "1500.00", "urmtom": "0.00"}
    ]

    await poll_account_async(acc1, datetime(2026, 11, 17, 11, 5, 0))
    trades = [t for t in await DB._all_trades() if t["account"] == "ACC-1"]
    assert len(trades) == 1
    t = trades[0]
    assert t["open"] is False
    assert t["entry"] == 300.0
    assert t["exit"] == 350.0
    assert t["realized_pnl"] == 1500.0  # (350 - 300) * 30
    assert t["pnl"] == 1500.0
    assert t["charges"] > 0
    assert t["net_pnl"] == round(1500.0 - t["charges"], 2)
    assert t["exitDate"] == "2026-11-17"


@pytest.mark.asyncio
async def test_05_partial_position_closure():
    """TEST 5: Partially close a position and verify that closed quantity is realized while remaining quantity stays active."""
    day1_str = "17-11-2026"
    # ACC-2 sold 50 qty (2 lots of 25) NIFTY26NOV26C24000 @ 150.00.
    # Now buy back 1 lot (25 qty) @ 120.00 (Realized profit = (150 - 120) * 25 = +750.00), leaving 25 qty open @ LTP 130.00 (Unrealized profit = (150 - 130) * 25 = +500.00).
    BROKER.trade_books["UID202"].append({
        "norenordno": "ORD202_X1",
        "flid": "FL202_2",
        "tsym": "NIFTY26NOV26C24000",
        "exch": "NFO",
        "trantype": "B",
        "prd": "M",
        "flqty": "25",
        "flprc": "120.00",
        "ls": "25",
        "fltm": f"{day1_str} 14:00:00",
        "ordersource": "API",
    })
    BROKER.position_books["UID202"] = [
        {"tsym": "NIFTY26NOV26C24000", "exch": "NFO", "prd": "M", "netqty": "-25", "lp": "130.00", "rpnl": "750.00", "urmtom": "500.00"}
    ]

    acc2 = await DB.get_account_async("ACC-2")
    await poll_account_async(acc2, datetime(2026, 11, 17, 14, 5, 0))

    trades_acc2 = [t for t in await DB._all_trades() if t["account"] == "ACC-2"]
    assert len(trades_acc2) == 2

    closed_leg = next(t for t in trades_acc2 if not t["open"])
    open_leg = next(t for t in trades_acc2 if t["open"])

    assert closed_leg["qty"] == 25.0
    assert closed_leg["lots"] == 1.0
    assert closed_leg["entry"] == 150.0
    assert closed_leg["exit"] == 120.0
    assert closed_leg["realized_pnl"] == 750.0
    assert closed_leg["exitDate"] == "2026-11-17"

    assert open_leg["qty"] == 25.0
    assert open_leg["lots"] == 1.0
    assert open_leg["entry"] == 150.0
    assert open_leg["exit"] == 130.0
    assert open_leg["unrealized_pnl"] == 500.0
    assert open_leg["open"] is True
    assert open_leg["date"] == "2026-11-17"
    assert open_leg["parent_trade_id"] == closed_leg["parent_trade_id"]


@pytest.mark.asyncio
async def test_07_and_14_eod_finalization_and_idempotency():
    """TEST 7 & TEST 14: End trading session (15:30 IST), verify reconciliation & finalization, and verify idempotency on repeated runs."""
    eod_now = datetime(2026, 11, 17, 15, 35, 0)
    res1 = await finalize_eod_async(now=eod_now, target_date="2026-11-17")
    assert res1["finalized_accounts"] >= 2
    assert res1["pending_accounts"] == 0

    # Run synchronization and finalization multiple times (TEST 14)
    acc1 = await DB.get_account_async("ACC-1")
    acc2 = await DB.get_account_async("ACC-2")
    await poll_account_async(acc1, eod_now)
    await poll_account_async(acc2, eod_now)
    res2 = await finalize_eod_async(now=eod_now, target_date="2026-11-17")
    res3 = await finalize_eod_async(now=eod_now, target_date="2026-11-17")

    # Verify no duplicate trades or duplicate daily reports
    all_trades = await DB._all_trades()
    acc1_trades = [t for t in all_trades if t["account"] == "ACC-1"]
    acc2_trades = [t for t in all_trades if t["account"] == "ACC-2"]
    assert len(acc1_trades) == 1
    assert acc1_trades[0]["is_finalized"] is True

    assert len(acc2_trades) == 2
    closed_acc2 = next(t for t in acc2_trades if not t["open"])
    open_acc2 = next(t for t in acc2_trades if t["open"])
    assert closed_acc2["is_finalized"] is True
    assert open_acc2["is_finalized"] is False  # Still open overnight!

    rep_acc1 = await DB.get_daily_reports_async(account_id="ACC-1")
    rep_acc2 = await DB.get_daily_reports_async(account_id="ACC-2")
    assert len(rep_acc1) == 1
    assert rep_acc1[0]["realized_pnl"] == 1500.0
    assert rep_acc1[0]["closed_trades"] == 1
    assert rep_acc1[0]["status"] == "FINALIZED"

    assert len(rep_acc2) == 1
    assert rep_acc2[0]["realized_pnl"] == 750.0
    assert rep_acc2[0]["closed_trades"] == 1
    assert rep_acc2[0]["open_trades"] == 1
    assert rep_acc2[0]["partially_closed_trades"] == 1


@pytest.mark.asyncio
async def test_08_06_09_15_midnight_archiving_overnight_continuity_and_calendar_immutability():
    """TEST 8, TEST 6, TEST 9, TEST 15:
    - Simulate midnight processing at 00:00 IST on 2026-11-18.
    - Closed trades from 2026-11-17 are archived; overnight open position on ACC-2 remains active.
    - Next day (2026-11-18) synchronization updates overnight position without duplication.
    - Daily P&L Calendar reconciles 100% with finalized records, and 2026-11-17 historical P&L remains unchanged.
    """
    midnight_now = datetime(2026, 11, 18, 0, 1, 0)
    arch_res = await midnight_archive_async(now=midnight_now)
    assert arch_res["archived_trades"] == 2  # 1 closed on ACC-1 + 1 partially closed leg on ACC-2
    assert arch_res["active_overnight_positions"] == 1  # 1 open overnight position on ACC-2

    all_trades = await DB._all_trades()
    for t in all_trades:
        if not t["open"]:
            assert t["is_archived"] is True
        else:
            assert t["is_archived"] is False

    # Next trading day (2026-11-18 09:20 IST): Broker TradeBook is empty for the new day initially,
    # while PositionBook shows the carried-forward short 25 qty on ACC-2 at a new LTP of 110.00!
    BROKER.trade_books["UID101"] = []
    BROKER.position_books["UID101"] = []

    BROKER.trade_books["UID202"] = []
    BROKER.position_books["UID202"] = [
        {"tsym": "NIFTY26NOV26C24000", "exch": "NFO", "prd": "M", "netqty": "-25", "lp": "110.00", "rpnl": "0.00", "urmtom": "1000.00"}
    ]

    day2_morning = datetime(2026, 11, 18, 9, 20, 0)
    acc1 = await DB.get_account_async("ACC-1")
    acc2 = await DB.get_account_async("ACC-2")
    await poll_account_async(acc1, day2_morning)
    await poll_account_async(acc2, day2_morning)

    # Verify TEST 6: Overnight position continues seamlessly without duplication, preserving original entry date (2026-11-17) and entry price (150.0)
    acc2_trades = [t for t in await DB._all_trades() if t["account"] == "ACC-2"]
    open_positions = [t for t in acc2_trades if t["open"]]
    assert len(open_positions) == 1
    op = open_positions[0]
    assert op["id"] == "Z-ACC-2-ORD202_E1"
    assert op["date"] == "2026-11-17"  # Original entry date preserved!
    assert op["entry"] == 150.0  # Original entry price preserved!
    assert op["qty"] == 25.0
    assert op["exit"] == 110.0  # Updated to Day 2 LTP!
    assert op["unrealized_pnl"] == 1000.0  # (150 - 110) * 25

    # Now close the remaining 25 qty on Day 2 (18-11-2026 10:30:00) @ 100.00 (Realized profit on Day 2 = (150 - 100) * 25 = +1250.00)
    BROKER.trade_books["UID202"] = [
        {
            "norenordno": "ORD202_X2",
            "flid": "FL202_3",
            "tsym": "NIFTY26NOV26C24000",
            "exch": "NFO",
            "trantype": "B",
            "prd": "M",
            "flqty": "25",
            "flprc": "100.00",
            "ls": "25",
            "fltm": "18-11-2026 10:30:00",
            "ordersource": "API",
        }
    ]
    BROKER.position_books["UID202"] = [
        {"tsym": "NIFTY26NOV26C24000", "exch": "NFO", "prd": "M", "netqty": "0", "lp": "100.00", "rpnl": "1250.00", "urmtom": "0.00"}
    ]
    await poll_account_async(acc2, datetime(2026, 11, 18, 10, 35, 0))
    await finalize_eod_async(now=datetime(2026, 11, 18, 15, 35, 0), target_date="2026-11-18")

    # Verify TEST 9 & TEST 15:
    # Day 1 (2026-11-17) P&L for ACC-2 MUST still be +750.00 (unchanged!), and Day 2 (2026-11-18) P&L for ACC-2 MUST be +1250.00!
    rep_acc2 = await DB.get_daily_reports_async(account_id="ACC-2")
    by_date = {r["date"]: r for r in rep_acc2}
    assert by_date["2026-11-17"]["realized_pnl"] == 750.0
    assert by_date["2026-11-17"]["status"] == "ARCHIVED"
    assert by_date["2026-11-18"]["realized_pnl"] == 1250.0
    assert by_date["2026-11-18"]["status"] == "FINALIZED"

    # Reconcile calendar check (Req 6.10, 6.14)
    recon = await reconcile_daily_calendar_async()
    assert recon["ok"] is True
    assert len(recon["mismatches"]) == 0


@pytest.mark.asyncio
async def test_13_rule_violation_and_explanation():
    """TEST 13: Break a configured trading rule and verify that the correct red flag and explanation appear."""
    client = TestClient(app)
    headers = _get_auth_headers(client)

    # Configure specific rule for ACC-1: BANKNIFTY only, max 2 lots per trade, entry from 09:20 to 14:30
    custom_rules = {
        "instrument": "BANKNIFTY",
        "expiry": "Monthly",
        "strikeMode": "ANY",
        "strikeDist": 1,
        "side": "BUY",
        "product": "MIS",
        "timeFrom": "09:20",
        "timeTo": "14:30",
        "maxTradesDay": 4,
        "maxLots": 2,
        "slPerLot": 2000,
        "dailyLoss": 10000,
        "hedge": False,
        "source": "Manual",
    }
    client.put("/api/rules/ACC-1", headers=headers, json={"rules": custom_rules, "change": "Set maxLots=2 for ACC-1"}).raise_for_status()

    # Place a trade on ACC-1 that breaks maxLots (took 4 lots > max 2 lots) and timeTo (14:50 > 14:30)
    BROKER.trade_books["UID101"] = [
        {
            "norenordno": "ORD101_V1",
            "flid": "FL101_V1",
            "tsym": "BANKNIFTY26NOV26C51800",
            "exch": "NFO",
            "trantype": "B",
            "prd": "I",
            "flqty": "120",  # 4 lots of 30
            "flprc": "250.00",
            "ls": "30",
            "fltm": "18-11-2026 14:50:00",
            "ordersource": "WEB",
        }
    ]
    BROKER.position_books["UID101"] = [
        {"tsym": "BANKNIFTY26NOV26C51800", "exch": "NFO", "prd": "I", "netqty": "120", "lp": "255.00", "rpnl": "0.00", "urmtom": "600.00"}
    ]

    acc1 = await DB.get_account_async("ACC-1")
    await poll_account_async(acc1, datetime(2026, 11, 18, 14, 52, 0))

    violations = client.get("/api/violations?account=ACC-1", headers=headers).json()
    v_rules = {v["rule"]: v for v in violations}
    assert "Max 2 lots / trade" in v_rules
    assert "Took 4 lots" in v_rules["Max 2 lots / trade"]["did"]
    assert "Entry till 14:30" in v_rules
    assert "14:50" in v_rules["Entry till 14:30"]["did"]

    # Ensure ACC-2 violations are NOT mixed into ACC-1
    assert all(v["account"] == "ACC-1" for v in violations)


@pytest.mark.asyncio
async def test_12_token_expiration_and_stale_data_detection():
    """TEST 12: Expire an authentication token and verify that the application detects the failure correctly without marking as Linked."""
    client = TestClient(app)
    headers = _get_auth_headers(client)

    BROKER.expired_uids.add("UID101")
    acc1 = await DB.get_account_async("ACC-1")

    with pytest.raises(SessionExpired):
        await poll_account_async(acc1, datetime(2026, 11, 18, 15, 0, 0))

    l_state = await link_state(acc1)
    assert l_state == "Login expired"

    accs_resp = client.get("/api/accounts", headers=headers).json()
    acc1_info = next(a for a in accs_resp if a["id"] == "ACC-1")
    assert acc1_info["link"] == "Login expired"
    assert acc1_info["is_stale"] is True
    assert "expired" in (acc1_info.get("sync_error") or "").lower()

    BROKER.expired_uids.discard("UID101")


@pytest.mark.asyncio
async def test_11_manual_account_disconnect():
    """TEST 11: Disconnect an account manually and verify that synchronization stops for that account."""
    client = TestClient(app)
    headers = _get_auth_headers(client)

    client.post("/api/accounts/ACC-1/unlink", headers=headers).raise_for_status()

    accs_resp = client.get("/api/accounts", headers=headers).json()
    acc1_info = next(a for a in accs_resp if a["id"] == "ACC-1")
    assert acc1_info["link"] == "Not linked"

    calls_before = BROKER.sync_call_counts.get("UID101", 0)
    acc1 = await DB.get_account_async("ACC-1")
    res = await poll_account_async(acc1, datetime(2026, 11, 18, 15, 5, 0))
    assert res == "not_linked"
    assert BROKER.sync_call_counts.get("UID101", 0) == calls_before


@pytest.mark.asyncio
async def test_10_persistence_across_restart():
    """TEST 10: Simulate backend restart and verify connected accounts, historical records, rules, and red flags persist."""
    # Re-initialize database models and Store instance (simulating server restart on same DB)
    await init_models()
    fresh_store = main_mod.Store()

    acc2 = await fresh_store.get_account_async("ACC-2")
    assert acc2 is not None
    assert await link_state(acc2) == "Linked"

    rules_acc1 = await fresh_store._get_rule("ACC-1")
    assert rules_acc1 is not None
    assert rules_acc1["rules"]["maxLots"] == 2

    reports_acc2 = await fresh_store.get_daily_reports_async(account_id="ACC-2")
    assert len(reports_acc2) == 2

    violations_acc1 = await fresh_store.get_rule_violations_async(account_id="ACC-1")
    assert len(violations_acc1) >= 2
