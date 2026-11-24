"""Comprehensive tests for F&O instrument expiry-date retrieval, storage, and edge cases.

Covers:
1. Weekly and monthly options via Zebu GetSecurityInfo (`exd` field)
2. Multiple expiries with the exact same underlying, strike, and CE/PE distinguished strictly by (exch, token)
3. Equities / instruments without expiry (empty expiry -> UI displays N/A; never invented)
4. Expired contracts persisted in DB (`trades.expiry` & `instruments.expiry_date`) after GetSecurityInfo stops returning them
5. Missing tokens, unmatched instruments, API failures, and duplicate executions (`flid` deduplication)
6. Strict non-inference guarantee: expiry is never guessed from calendar assumptions
"""
import asyncio
import os
import tempfile
from datetime import datetime
import pytest
from fastapi.testclient import TestClient

_TMP_DB = os.path.join(tempfile.gettempdir(), f"tts_expiry_test_{os.getpid()}.db")
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_TMP_DB}"

from app.database import init_models, set_engine_url
from app.engine import build_trades, normalize_expiry_date, parse_instrument
from app.main import app, DB, ZEBU, _security_info_cache, poll_account_async, IST
from app.security import hash_pw
from app.zebu import ZebuError


def test_01_normalize_expiry_and_no_calendar_inference():
    # Official Zebu GetSecurityInfo `exd` formats
    assert normalize_expiry_date("26-NOV-2026") == "2026-11-26"
    assert normalize_expiry_date("19-NOV-2026") == "2026-11-19"
    assert normalize_expiry_date("2026-12-31") == "2026-12-31"
    assert normalize_expiry_date("03-12-2026") == "2026-12-03"
    assert normalize_expiry_date("26-NOV-26") == "2026-11-26"

    # Equities / empty / invalid values must return None (never invent a date)
    assert normalize_expiry_date("") is None
    assert normalize_expiry_date(None) is None
    assert normalize_expiry_date("0") is None
    assert normalize_expiry_date("N/A") is None
    assert normalize_expiry_date("INVALID") is None

    # parse_instrument must NEVER infer expiry from calendar assumptions
    opt_monthly = parse_instrument("BANKNIFTY26NOV51700CE")
    assert opt_monthly["underlying"] == "BANKNIFTY"
    assert opt_monthly["strike"] == 51700.0
    assert opt_monthly["opt"] == "CE"
    assert opt_monthly["expiry"] is None

    fut = parse_instrument("NIFTY26NOVFUT")
    assert fut["type"] == "FUT"
    assert fut["expiry"] is None

    eq = parse_instrument("RELIANCE-EQ")
    assert eq["type"] == "EQ"
    assert eq["expiry"] is None


def test_02_multiple_expiries_same_strike_and_duplicate_flid():
    """Two NIFTY 24500 CE contracts with different tokens & expiries (weekly vs monthly)
    must remain completely isolated by (exch, token) and duplicate flid executions must be deduped."""
    acc = {"id": "AC-EXP", "trader": "TR-EXP"}
    now = datetime(2026, 11, 18, 14, 0)
    cfg = {"brokerage_per_order": 20, "stt_sell_pct": 0.1, "txn_pct": 0.035, "stamp_buy_pct": 0.003}
    pmap = {"I": "MIS", "M": "NRML"}

    sec_info = {
        ("NFO", "55001"): {"exch": "NFO", "token": "55001", "exd": "2026-11-19", "tsym": "NIFTY19NOV26C24500"},
        ("NFO", "55002"): {"exch": "NFO", "token": "55002", "exd": "2026-11-26", "tsym": "NIFTY26NOV26C24500"},
        ("NSE", "2885"): {"exch": "NSE", "token": "2885", "exd": None, "tsym": "RELIANCE-EQ"},
    }

    fills = [
        # Weekly NIFTY 24500 CE (token 55001) + duplicate flid execution
        {"norenordno": "O1", "flid": "FL1", "exch": "NFO", "token": "55001", "tsym": "NIFTY19NOV26C24500", "trantype": "B", "flqty": "50", "flprc": "100", "ls": "25", "prd": "I", "fltm": "18-11-2026 09:30:00"},
        {"norenordno": "O1", "flid": "FL1", "exch": "NFO", "token": "55001", "tsym": "NIFTY19NOV26C24500", "trantype": "B", "flqty": "50", "flprc": "100", "ls": "25", "prd": "I", "fltm": "18-11-2026 09:30:00"},
        {"norenordno": "O2", "flid": "FL2", "exch": "NFO", "token": "55001", "tsym": "NIFTY19NOV26C24500", "trantype": "S", "flqty": "50", "flprc": "120", "ls": "25", "prd": "I", "fltm": "18-11-2026 10:00:00"},
        # Monthly NIFTY 24500 CE (token 55002) - same underlying, strike, CE!
        {"norenordno": "O3", "flid": "FL3", "exch": "NFO", "token": "55002", "tsym": "NIFTY26NOV26C24500", "trantype": "B", "flqty": "25", "flprc": "200", "ls": "25", "prd": "I", "fltm": "18-11-2026 09:35:00"},
        {"norenordno": "O4", "flid": "FL4", "exch": "NFO", "token": "55002", "tsym": "NIFTY26NOV26C24500", "trantype": "S", "flqty": "25", "flprc": "240", "ls": "25", "prd": "I", "fltm": "18-11-2026 10:15:00"},
        # Equity cash (NSE, token 2885) - no expiry
        {"norenordno": "O5", "flid": "FL5", "exch": "NSE", "token": "2885", "tsym": "RELIANCE-EQ", "trantype": "B", "flqty": "10", "flprc": "2500", "ls": "1", "prd": "I", "fltm": "18-11-2026 11:00:00"},
        {"norenordno": "O6", "flid": "FL6", "exch": "NSE", "token": "2885", "tsym": "RELIANCE-EQ", "trantype": "S", "flqty": "10", "flprc": "2550", "ls": "1", "prd": "I", "fltm": "18-11-2026 11:30:00"},
        # Missing token fill - must not crash and must not invent expiry
        {"norenordno": "O7", "flid": "FL7", "exch": "NFO", "token": "", "tsym": "BANKNIFTY26NOV51700CE", "trantype": "B", "flqty": "15", "flprc": "300", "ls": "15", "prd": "I", "fltm": "18-11-2026 12:00:00"},
        {"norenordno": "O8", "flid": "FL8", "exch": "NFO", "token": "", "tsym": "BANKNIFTY26NOV51700CE", "trantype": "S", "flqty": "15", "flprc": "310", "ls": "15", "prd": "I", "fltm": "18-11-2026 12:30:00"},
    ]

    trades = build_trades(acc, fills, cfg, pmap, lambda o: False, now, security_info=sec_info)
    by_oid = {t["entry_order_id"]: t for t in trades}

    # Weekly option: qty=50 (deduped!), exact weekly expiry
    assert by_oid["O1"]["qty"] == 50.0
    assert by_oid["O1"]["expiry"] == "2026-11-19"
    assert by_oid["O1"]["expiryType"] == "Weekly"
    assert by_oid["O1"]["token"] == "55001"
    assert by_oid["O1"]["realized_pnl"] == 1000.0

    # Monthly option: isolated by token 55002, exact monthly expiry
    assert by_oid["O3"]["qty"] == 25.0
    assert by_oid["O3"]["expiry"] == "2026-11-26"
    assert by_oid["O3"]["expiryType"] == "Monthly"
    assert by_oid["O3"]["token"] == "55002"
    assert by_oid["O3"]["realized_pnl"] == 1000.0

    # Equity: empty expiry (UI displays N/A)
    assert by_oid["O5"]["expiry"] == ""
    assert by_oid["O5"]["expiryType"] == "Equity"
    assert by_oid["O5"]["token"] == "2885"

    # Missing token: empty expiry (never inferred from calendar)
    assert by_oid["O7"]["expiry"] == ""
    assert by_oid["O7"]["token"] is None


def test_03_poll_integration_getsecurityinfo_persistence_expired_and_api_failure():
    """End-to-end poll test verifying:
    - Live GetSecurityInfo retrieval & DB persistence in `instruments` and `trades`
    - Expired contract remains accurate after clearing memory cache and simulating GetSecurityInfo Not_Ok / API failure
    - Unmatched instrument / API error gracefully results in empty expiry (N/A) without failing sync
    """
    set_engine_url(f"sqlite+aiosqlite:///{_TMP_DB}")
    asyncio.run(init_models())
    _security_info_cache.clear()

    sec_mode = {"fail_expired": False}
    sec_calls = []

    async def mock_trade_book(token, uid, actid, proxy=None, auth_mode=None):
        return [
            {"norenordno": "W1", "flid": "F1", "exch": "NFO", "token": "60001", "tsym": "NIFTY19NOV26C24500", "trantype": "B", "flqty": "25", "flprc": "100", "ls": "25", "prd": "I", "fltm": "18-11-2026 09:30:00"},
            {"norenordno": "W2", "flid": "F2", "exch": "NFO", "token": "60001", "tsym": "NIFTY19NOV26C24500", "trantype": "S", "flqty": "25", "flprc": "110", "ls": "25", "prd": "I", "fltm": "18-11-2026 09:45:00"},
            {"norenordno": "U1", "flid": "F3", "exch": "NFO", "token": "99999", "tsym": "UNKNOWN26NOV100CE", "trantype": "B", "flqty": "10", "flprc": "50", "ls": "10", "prd": "I", "fltm": "18-11-2026 10:00:00"},
            {"norenordno": "U2", "flid": "F4", "exch": "NFO", "token": "99999", "tsym": "UNKNOWN26NOV100CE", "trantype": "S", "flqty": "10", "flprc": "55", "ls": "10", "prd": "I", "fltm": "18-11-2026 10:15:00"},
        ]

    async def mock_pos_book(token, uid, actid, proxy=None, auth_mode=None):
        return []

    async def mock_order_book(token, uid, proxy=None, auth_mode=None):
        return []

    async def mock_security_info(token, uid, exch, sym_token, proxy=None, auth_mode=None):
        sec_calls.append((exch, str(sym_token)))
        if str(sym_token) == "60001":
            if sec_mode["fail_expired"]:
                return {"stat": "Not_Ok", "emsg": "No Data found for token"}
            return {
                "stat": "Ok",
                "exch": "NFO",
                "token": "60001",
                "tsym": "NIFTY19NOV26C24500",
                "symname": "NIFTY",
                "instname": "OPTIDX",
                "strprc": "24500.00",
                "optt": "CE",
                "exd": "19-NOV-2026",
                "ls": "25",
            }
        # Simulate API failure for unmatched token 99999
        raise ZebuError("GetSecurityInfo: No Data found")

    orig = (ZEBU.trade_book_async, ZEBU.position_book_async, ZEBU.order_book_async, ZEBU.security_info_async)
    ZEBU.trade_book_async = mock_trade_book
    ZEBU.position_book_async = mock_pos_book
    ZEBU.order_book_async = mock_order_book
    ZEBU.security_info_async = mock_security_info

    try:
        async def _run():
            try:
                await DB._put_trader("TR-E1", {"id": "TR-E1", "name": "Expiry Test Trader", "status": "Live"})
                await DB.put_account_async("ACC-E1", {"id": "ACC-E1", "name": "Expiry Test Acc", "broker": "Zebu", "trader": "TR-E1", "zebu_uid": "UIDE1", "zebu_actid": "UIDE1"})
                await DB.put_token_async("ACC-E1", {"access_token": "TOKE1", "status": "Linked"})
                acc = await DB.get_account_async("ACC-E1")

                now = datetime(2026, 11, 18, 11, 0, tzinfo=IST)
                # First poll: fetches GetSecurityInfo for 60001 (succeeds) and 99999 (fails gracefully)
                await poll_account_async(acc, now=now, force=True)

                trades = [t for t in await DB._all_trades() if t.get("account") == "ACC-E1"]
                by_oid = {t["entry_order_id"]: t for t in trades}
                assert by_oid["W1"]["expiry"] == "2026-11-19"
                assert by_oid["W1"]["token"] == "60001"
                assert by_oid["U1"]["expiry"] == ""

                # Second poll: simulate contract expired & server restart (clear in-memory cache + broker returns Not_Ok)
                _security_info_cache.clear()
                sec_mode["fail_expired"] = True
                sec_calls.clear()
                await poll_account_async(acc, now=now, force=True)

                # 60001 must be resolved from persisted DB `instruments` table without even calling GetSecurityInfo again!
                assert ("NFO", "60001") not in sec_calls
                trades2 = [t for t in await DB._all_trades() if t.get("account") == "ACC-E1"]
                by_oid2 = {t["entry_order_id"]: t for t in trades2}
                assert by_oid2["W1"]["expiry"] == "2026-11-19"
                assert by_oid2["U1"]["expiry"] == ""
            finally:
                # Clean up test account so subsequent test modules in the same pytest session stay isolated
                from sqlalchemy import delete as sa_delete
                from app.database import AsyncSessionLocal
                from app.models import Account, Trader, BrokerSession, Trade, Fill, Instrument
                async with AsyncSessionLocal() as session:
                    await session.execute(sa_delete(BrokerSession).where(BrokerSession.account_id == "ACC-E1"))
                    await session.execute(sa_delete(Trade).where(Trade.account_id == "ACC-E1"))
                    await session.execute(sa_delete(Fill).where(Fill.account_id == "ACC-E1"))
                    await session.execute(sa_delete(Account).where(Account.id == "ACC-E1"))
                    await session.execute(sa_delete(Trader).where(Trader.id == "TR-E1"))
                    await session.execute(sa_delete(Instrument).where(Instrument.token == "60001"))
                    await session.commit()

        asyncio.run(_run())
    finally:
        ZEBU.trade_book_async, ZEBU.position_book_async, ZEBU.order_book_async, ZEBU.security_info_async = orig
        _security_info_cache.clear()
        if os.path.exists(_TMP_DB):
            try:
                os.remove(_TMP_DB)
            except OSError:
                pass
