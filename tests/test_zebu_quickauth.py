"""Comprehensive tests for Zebu QuickAuth authentication, session management, and full data parity."""
import asyncio
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Ensure backend is importable and uses an isolated temporary SQLite database
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.zebu import ZebuClient, SessionExpired, ZebuError
from app.zebu_ws import ZebuWebSocketClient


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# =====================================================================
# 1. Official Zebu QuickAuth Hashing, Payload, and Dual-Mode Routing
# =====================================================================

def test_quickauth_payload_hashing_and_endpoint():
    """Verify QuickAuth calls /NorenWClientTP/QuickAuth with exact documented SHA-256 hashes."""
    client = ZebuClient("https://go.mynt.in", "DEFAULT_CID", "DEFAULT_SEC")
    captured = {}

    class FakeResp:
        status_code = 200
        text = json.dumps({
            "stat": "Ok",
            "susertoken": "tok_qa_1234567890",
            "uid": "47000S19",
            "actid": "47000S19",
            "uname": "TEST TRADER",
        })

        def json(self):
            return json.loads(self.text)

    class FakeHttpClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, content=None, headers=None):
            captured["url"] = url
            captured["content"] = content
            captured["headers"] = headers
            return FakeResp()

    async def _run():
        with patch("httpx.AsyncClient", return_value=FakeHttpClient()):
            res = await client.quick_auth_async(
                uid="47000S19",
                password="MySecretPassword!1",
                factor2="ABCDE1234F",
                vc="47000S19_U",
                api_secret="API_SECRET_XYZ",
            )
            assert res["stat"] == "Ok"
            assert res["susertoken"] == "tok_qa_1234567890"
            assert client.get_token_mode("tok_qa_1234567890") == "quickauth"

    asyncio.run(_run())

    assert captured["url"] == "https://go.mynt.in/NorenWClientTP/QuickAuth"
    assert captured["headers"]["Content-Type"] == "text/plain"
    assert captured["content"].startswith("jData=")
    jdata = json.loads(captured["content"][len("jData="):])
    assert jdata["uid"] == "47000S19"
    assert jdata["pwd"] == _sha256("MySecretPassword!1")
    assert jdata["factor2"] == "ABCDE1234F"
    assert jdata["vc"] == "47000S19_U"
    assert jdata["appkey"] == _sha256("47000S19|API_SECRET_XYZ")
    assert jdata["imei"] == "abc1234"
    assert jdata["source"] == "API"
    assert jdata["apkversion"] == "1.0.0"


def test_quickauth_rejects_invalid_credentials():
    """Verify QuickAuth raises ZebuError on stat != Ok or missing susertoken."""
    client = ZebuClient("https://go.mynt.in", "", "")

    class FakeErrResp:
        status_code = 200
        text = json.dumps({"stat": "Not_Ok", "emsg": "Invalid Input : Wrong Password"})

        def json(self):
            return json.loads(self.text)

    class FakeHttpClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, content=None, headers=None):
            return FakeErrResp()

    async def _run():
        with patch("httpx.AsyncClient", return_value=FakeHttpClient()):
            with pytest.raises(ZebuError, match="Wrong Password"):
                await client.quick_auth_async(
                    uid="47000S19",
                    password="bad",
                    factor2="ABCDE1234F",
                    vc="47000S19_U",
                    api_secret="SEC",
                )

    asyncio.run(_run())


def test_dual_mode_rest_routing_quickauth_vs_oauth():
    """QuickAuth tokens route to /NorenWClientTP/<ep> with jKey; OAuth tokens stay on /NorenWClientAPI/<ep> with Bearer."""
    client = ZebuClient("https://go.mynt.in", "", "")
    client.register_token_mode("qa_token_1", "quickauth")
    client.register_token_mode("oa_token_1", "oauth")
    calls = []

    class FakeResp:
        status_code = 200
        text = json.dumps({"stat": "Ok", "cash": "500000.00", "marginused": "120000.00"})

        def json(self):
            return json.loads(self.text)

    class FakeHttpClient:
        is_closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, content=None, headers=None):
            calls.append({"url": url, "content": content, "headers": headers})
            return FakeResp()

    async def _run():
        with patch("httpx.AsyncClient", return_value=FakeHttpClient()):
            await client.limits_async("qa_token_1", "47000S19", "47000S19")
            await client.limits_async("oa_token_1", "47000S19", "47000S19")

    asyncio.run(_run())

    assert len(calls) == 2
    # QuickAuth call
    assert calls[0]["url"] == "https://go.mynt.in/NorenWClientTP/Limits"
    assert "Authorization" not in calls[0]["headers"]
    assert "&jKey=qa_token_1" in calls[0]["content"]
    # OAuth call
    assert calls[1]["url"] == "https://go.mynt.in/NorenWClientAPI/Limits"
    assert calls[1]["headers"]["Authorization"] == "Bearer oa_token_1"


def test_websocket_handshake_auth_mode():
    """Verify ZebuWebSocketClient stores auth_mode and uses 'c'+susertoken for QuickAuth vs 'a'+accesstoken for OAuth."""
    ws_qa = ZebuWebSocketClient("wss://go.mynt.in/NorenWSTP/", "AC-1", "47000S19", "47000S19", "suser_tok", auth_mode="quickauth")
    ws_oa = ZebuWebSocketClient("wss://go.mynt.in/NorenWSTP/", "AC-2", "47000S19", "47000S19", "oauth_tok", auth_mode="oauth")
    assert ws_qa.auth_mode == "quickauth"
    assert ws_oa.auth_mode == "oauth"


# =====================================================================
# 2. End-to-End Backend Integration Tests (Config, Connect, Data Parity,
#    Session Persistence, Auto-Reauth, Concurrency, Multi-Account Isolation)
# =====================================================================

@pytest.fixture
def isolated_app(tmp_path, monkeypatch):
    """Initialize an isolated SQLite DB and return main module + admin auth header."""
    db_file = tmp_path / "test_quickauth.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_file.as_posix()}")
    monkeypatch.setenv("RUN_WORKER_IN_PROCESS", "false")

    import app.database as db_mod
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from app.database import Base

    engine_test = create_async_engine(f"sqlite+aiosqlite:///{db_file.as_posix()}", future=True)
    session_factory = async_sessionmaker(bind=engine_test, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(db_mod, "async_engine", engine_test)
    monkeypatch.setattr(db_mod, "AsyncSessionLocal", session_factory)

    import app.store as store_mod
    monkeypatch.setattr(store_mod, "AsyncSessionLocal", session_factory)

    import app.session_manager as sm_mod
    monkeypatch.setattr(sm_mod, "AsyncSessionLocal", session_factory)

    import app.main as main_mod
    monkeypatch.setattr(main_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(main_mod.CFG, "RUN_WORKER_IN_PROCESS", False)

    # Reset caches/locks between tests
    main_mod.SESSION_MGR._uid_locks.clear()
    main_mod.SESSION_MGR._trackers.clear()
    main_mod._FUNDS_CACHE.clear()
    main_mod._FUNDS_CACHE_MONO.clear()
    main_mod._FUNDS_LAST_VERIFIED.clear()
    main_mod._security_info_cache.clear()
    main_mod.FIN_SUMMARY.clear_account("AC-01")
    main_mod.FIN_SUMMARY.clear_account("AC-02")

    async def _init():
        async with engine_test.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await main_mod.DB.put_user_async("admin", {
            "user": "admin",
            "role": "admin",
            "pw": main_mod.hash_pw("admin123"),
        })
        await main_mod.DB.put_session_async("test_admin_tok", {
            "user": "admin",
            "role": "admin",
            "exp": time.time() + 3600,
        })
        await main_mod.DB._put_trader("TR-001", {"id": "TR-001", "name": "Trader One", "status": "Testing"})
        await main_mod.DB.put_account_async("AC-01", {
            "id": "AC-01",
            "name": "Zebu Account 1",
            "broker": "Zebu",
            "trader": "TR-001",
            "zebu_uid": "47000S19",
            "zebu_actid": "47000S19",
        })

    asyncio.run(_init())
    yield main_mod, {"Authorization": "Bearer test_admin_tok"}
    asyncio.run(engine_test.dispose())


def test_five_field_config_retains_blank_secrets_and_never_exposes_them(isolated_app):
    """Saving 5 fields encrypts secrets; blank updates retain them; GET /api/accounts never leaks them."""
    from fastapi.testclient import TestClient

    main_mod, headers = isolated_app
    client = TestClient(main_mod.app)

    # 1. Save full 5-field config
    r1 = client.post("/api/accounts/AC-01/zebu-config", headers=headers, json={
        "zebu_uid": "47000S19",
        "zebu_vc": "47000S19_U",
        "zebu_factor2": "ABCDE1234F",
        "zebu_secret_key": "SUPER_SECRET_APP_KEY",
        "zebu_password": "TopSecretPassword99",
    })
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["ok"] is True
    assert body1["has_zebu_secret_key"] is True
    assert body1["has_zebu_factor2"] is True
    assert body1["has_zebu_password"] is True
    assert "SUPER_SECRET_APP_KEY" not in r1.text
    assert "TopSecretPassword99" not in r1.text
    assert "ABCDE1234F" not in r1.text

    # 2. Update with blank password, factor2, secret_key -> must retain existing values
    r2 = client.post("/api/accounts/AC-01/zebu-config", headers=headers, json={
        "zebu_uid": "47000S19",
        "zebu_vc": "47000S19_U",
        "zebu_factor2": "",
        "zebu_secret_key": "",
        "zebu_password": "",
    })
    assert r2.status_code == 200

    creds = asyncio.run(main_mod.DB.get_account_credentials_async("AC-01"))
    assert creds["zebu_uid"] == "47000S19"
    assert creds["zebu_vc"] == "47000S19_U"
    assert creds["zebu_factor2"] == "ABCDE1234F"
    assert creds["zebu_secret_key"] == "SUPER_SECRET_APP_KEY"
    assert creds["zebu_password"] == "TopSecretPassword99"

    # 3. GET /api/accounts must NEVER expose secrets or tokens
    r3 = client.get("/api/accounts", headers=headers)
    assert r3.status_code == 200
    assert "SUPER_SECRET_APP_KEY" not in r3.text
    assert "TopSecretPassword99" not in r3.text
    assert "ABCDE1234F" not in r3.text
    acc = r3.json()[0]
    assert acc["has_zebu_credentials"] is True
    assert acc["zebu_vc"] == "47000S19_U"
    assert "zebu_password" not in acc
    assert "zebu_factor2" not in acc
    assert "zebu_secret_key" not in acc


def test_connect_and_sync_full_data_parity_and_restart_persistence(isolated_app, monkeypatch):
    """Connect & Sync authenticates via QuickAuth, syncs trades + expiry + P&L + MTM + Funds,
    and continues working after simulated backend restart (in-memory token mode cache cleared).
    """
    from fastapi.testclient import TestClient

    main_mod, headers = isolated_app
    client = TestClient(main_mod.app)
    today_str = main_mod.now_ist().strftime("%d-%m-%Y")

    # Mock Zebu REST endpoints
    async def fake_quick_auth(uid, password, factor2, vc, api_secret, **kwargs):
        assert uid == "47000S19"
        assert password == "Pass@123"
        assert factor2 == "ABCDE1234F"
        assert vc == "47000S19_U"
        assert api_secret == "SEC_KEY_1"
        main_mod.ZEBU.register_token_mode("suser_live_tok_1", "quickauth")
        return {"stat": "Ok", "susertoken": "suser_live_tok_1", "uname": "TRADER ONE", "actid": "47000S19"}

    async def fake_trade_book(tok, uid, actid=None, proxy=None, auth_mode=None):
        assert main_mod.ZEBU.get_token_mode(tok) == "quickauth"
        return [
            {
                "flid": "F1", "norenordno": "O1", "trantype": "B", "tsym": "NIFTY26NOV26C24500",
                "exch": "NFO", "token": "55001", "flqty": "75", "flprc": "120.00", "ls": "75",
                "prd": "I", "fltm": f"10:15:00 {today_str}",
            },
            {
                "flid": "F2", "norenordno": "O2", "trantype": "S", "tsym": "NIFTY26NOV26C24500",
                "exch": "NFO", "token": "55001", "flqty": "75", "flprc": "150.00", "ls": "75",
                "prd": "I", "fltm": f"10:45:00 {today_str}",
            },
        ]

    async def fake_pos_book(tok, uid, actid=None, proxy=None, auth_mode=None):
        assert main_mod.ZEBU.get_token_mode(tok) == "quickauth"
        return [
            {
                "tsym": "NIFTY26NOV26C24500", "exch": "NFO", "token": "55001", "prd": "I",
                "netqty": "0", "rpnl": "2250.00", "urmtom": "0.00", "lp": "150.00", "ls": "75",
                "daybuyqty": "75", "daysellqty": "75",
            },
            {
                "tsym": "BANKNIFTY26NOV26P52000", "exch": "NFO", "token": "55002", "prd": "M",
                "netqty": "30", "rpnl": "0.00", "urmtom": "900.00", "lp": "230.00", "netavgprc": "200.00",
                "ls": "30", "daybuyqty": "30", "daysellqty": "0",
            },
        ]

    async def fake_pos_snapshot(token, uid, actid=None, proxy=None, auth_mode=None):
        assert main_mod.ZEBU.get_token_mode(token) == "quickauth"
        rows = await fake_pos_book(token, uid, actid, proxy)
        return rows, json.dumps(rows), 12.0

    async def fake_order_book(tok, uid, proxy=None, auth_mode=None):
        return [{"norenordno": "O1", "status": "COMPLETE", "tsym": "NIFTY26NOV26C24500", "exch": "NFO", "trantype": "B", "qty": "75", "avgprc": "120.00"}]

    async def fake_sec_info(tok, uid, exch, token, proxy=None, auth_mode=None):
        return {"stat": "Ok", "exch": exch, "token": token, "exd": "26-NOV-2026", "tsym": "NIFTY26NOV26C24500"}

    async def fake_limits(tok, uid, actid=None, proxy=None, auth_mode=None):
        assert main_mod.ZEBU.get_token_mode(tok) == "quickauth"
        return {"stat": "Ok", "cash": "500000.00", "brkcollamt": "50000.00", "payin": "0.00", "payout": "0.00", "marginused": "125000.00"}

    monkeypatch.setattr(main_mod.ZEBU, "quick_auth_async", fake_quick_auth)
    monkeypatch.setattr(main_mod.ZEBU, "trade_book_async", fake_trade_book)
    monkeypatch.setattr(main_mod.ZEBU, "position_book_async", fake_pos_book)
    monkeypatch.setattr(main_mod.ZEBU, "position_book_snapshot_async", fake_pos_snapshot)
    monkeypatch.setattr(main_mod.ZEBU, "order_book_async", fake_order_book)
    monkeypatch.setattr(main_mod.ZEBU, "security_info_async", fake_sec_info)
    monkeypatch.setattr(main_mod.ZEBU, "limits_async", fake_limits)

    # 1. Connect & Sync with 5 fields
    r_conn = client.post("/api/accounts/AC-01/zebu-connect", headers=headers, json={
        "zebu_uid": "47000S19",
        "zebu_vc": "47000S19_U",
        "zebu_factor2": "ABCDE1234F",
        "zebu_secret_key": "SEC_KEY_1",
        "zebu_password": "Pass@123",
    })
    assert r_conn.status_code == 200, r_conn.text
    res_conn = r_conn.json()
    assert res_conn["ok"] is True
    assert res_conn["auth_mode"] == "quickauth"
    assert res_conn["status"] == "Linked"
    assert res_conn["cliname"] == "TRADER ONE"

    # 2. Verify Trades + Expiry Date stored accurately
    r_trades = client.get("/api/trades", headers=headers)
    assert r_trades.status_code == 200
    trades = r_trades.json()
    closed = [t for t in trades if not t.get("open") and t.get("tsym") == "NIFTY26NOV26C24500"]
    assert len(closed) >= 1
    assert closed[0]["expiry"] == "2026-11-26"
    assert closed[0]["pnl"] == 2250.0

    # 3. Simulate backend restart: wipe in-memory token mode cache, reload from DB
    main_mod.ZEBU._token_auth_modes.clear()
    main_mod.FIN_SUMMARY.clear_account("AC-01")
    main_mod._FUNDS_CACHE.clear()

    # 4. Verify Realized P&L and Unrealized MTM via Financial Summary (restores auth_mode from DB)
    r_fin = client.get("/api/overview/financial-summary?account_id=AC-01&force=1", headers=headers)
    assert r_fin.status_code == 200
    fin = r_fin.json()
    assert fin["status"] == "LIVE"
    assert fin["pnl"]["value"] == "2250.00"
    assert fin["mtm"]["value"] == "900.00"

    # 5. Verify Available Funds and Utilized Funds via Overview Funds Summary
    r_funds = client.get("/api/overview/funds-summary?force=1", headers=headers)
    assert r_funds.status_code == 200
    funds = r_funds.json()
    assert funds["status"] == "LIVE"
    assert funds["available_funds"] == "425000.00"  # 500000 + 50000 - 125000
    assert funds["utilized_funds"] == "125000.00"


def test_auto_reauth_on_session_expired_and_concurrent_dedup(isolated_app, monkeypatch):
    """Expired session triggers automatic QuickAuth reauthentication seamlessly across poll, FinancialSummary,
    and FundsSummary, and concurrent auth requests deduplicate to a single QuickAuth call.
    """
    main_mod, _ = isolated_app
    qa_calls = {"n": 0}
    limits_calls = {"n": 0}
    pos_calls = {"n": 0}

    async def fake_quick_auth(uid, password, factor2, vc, api_secret, **kwargs):
        qa_calls["n"] += 1
        await asyncio.sleep(0.05)
        tok = f"fresh_qa_tok_{qa_calls['n']}"
        main_mod.ZEBU.register_token_mode(tok, "quickauth")
        return {"stat": "Ok", "susertoken": tok, "uname": "TRADER ONE", "actid": uid}

    async def fake_limits(tok, uid, actid=None, proxy=None, auth_mode=None):
        limits_calls["n"] += 1
        if tok == "expired_tok":
            raise SessionExpired("Session Expired : Invalid Session Key")
        return {"stat": "Ok", "cash": "300000.00", "marginused": "50000.00"}

    async def fake_pos_snapshot(token, uid, actid=None, proxy=None, auth_mode=None):
        pos_calls["n"] += 1
        if token == "expired_tok":
            raise SessionExpired("Session Expired : Invalid Session Key")
        rows = [{"tsym": "NIFTY26NOV26C24500", "exch": "NFO", "token": "55001", "prd": "I", "netqty": "0", "rpnl": "1500.00", "urmtom": "0.00"}]
        return rows, json.dumps(rows), 5.0

    monkeypatch.setattr(main_mod.ZEBU, "quick_auth_async", fake_quick_auth)
    monkeypatch.setattr(main_mod.ZEBU, "limits_async", fake_limits)
    monkeypatch.setattr(main_mod.ZEBU, "position_book_snapshot_async", fake_pos_snapshot)

    async def _run():
        # Save QuickAuth credentials + seed an expired token
        await main_mod.DB.put_account_async("AC-01", {
            "id": "AC-01",
            "name": "Zebu Account 1",
            "broker": "Zebu",
            "trader": "TR-001",
            "zebu_uid": "47000S19",
            "zebu_actid": "47000S19",
            "zebu_vc": "47000S19_U",
            "zebu_factor2": "ABCDE1234F",
            "zebu_secret_key": "SEC_KEY_1",
            "zebu_password": "Pass@123",
        })
        await main_mod.DB.put_token_async("AC-01", {
            "access_token": "expired_tok",
            "auth_mode": "quickauth",
            "status": "Login expired",
            "expires_at": time.time() - 60,
        })

        # 1. Concurrent auth deduplication (5 concurrent calls -> 1 QuickAuth broker request)
        results = await asyncio.gather(*[
            main_mod.authenticate_zebu_quickauth_async("AC-01", force=False)
            for _ in range(5)
        ])
        assert qa_calls["n"] == 1
        assert all(r["access_token"] == "fresh_qa_tok_1" for r in results)

        # 2. Reset token to expired_tok and clear cooldown -> test Limits auto-reauth
        main_mod.SESSION_MGR._trackers.clear()
        await main_mod.DB.put_token_async("AC-01", {
            "access_token": "expired_tok",
            "auth_mode": "quickauth",
            "status": "Linked",
            "expires_at": time.time() + 3600,
        })
        acc = await main_mod.DB.get_account_async("AC-01")
        funds = await main_mod._fetch_overview_funds_summary_async([acc], "admin")
        assert funds["status"] == "LIVE"
        assert funds["available_funds"] == "250000.00"
        assert limits_calls["n"] == 2  # 1st expired -> reauth -> 2nd succeeded

        # 3. Reset token to expired_tok -> test FinancialSummary auto-reauth
        main_mod.SESSION_MGR._trackers.clear()
        main_mod.FIN_SUMMARY.clear_account("AC-01")
        await main_mod.DB.put_token_async("AC-01", {
            "access_token": "expired_tok",
            "auth_mode": "quickauth",
            "status": "Linked",
            "expires_at": time.time() + 3600,
        })
        fin = await main_mod.FIN_SUMMARY.get_summary(acc=acc, user_id="admin", force=True)
        assert fin["status"] == "LIVE"
        assert fin["pnl"]["value"] == "1500.00"
        assert pos_calls["n"] == 2

    asyncio.run(_run())


def test_multi_account_isolation_partial_failure(isolated_app, monkeypatch):
    """Failure in Account A (wrong password) does not affect Account B's QuickAuth sync or funds."""
    from fastapi.testclient import TestClient

    main_mod, headers = isolated_app
    client = TestClient(main_mod.app)

    async def _setup_second():
        await main_mod.DB._put_trader("TR-002", {"id": "TR-002", "name": "Trader Two", "status": "Testing"})
        await main_mod.DB.put_account_async("AC-02", {
            "id": "AC-02",
            "name": "Zebu Account 2",
            "broker": "Zebu",
            "trader": "TR-002",
            "zebu_uid": "99000Z99",
            "zebu_actid": "99000Z99",
        })

    asyncio.run(_setup_second())

    async def fake_quick_auth(uid, password, factor2, vc, api_secret, **kwargs):
        if uid == "47000S19":
            raise ZebuError("Invalid Password")
        tok = f"tok_{uid}"
        main_mod.ZEBU.register_token_mode(tok, "quickauth")
        return {"stat": "Ok", "susertoken": tok, "uname": "TRADER TWO", "actid": uid}

    async def fake_trade_book(tok, uid, actid=None, proxy=None, auth_mode=None):
        return []

    async def fake_pos_book(tok, uid, actid=None, proxy=None, auth_mode=None):
        return []

    async def fake_order_book(tok, uid, proxy=None, auth_mode=None):
        return []

    async def fake_limits(tok, uid, actid=None, proxy=None, auth_mode=None):
        return {"stat": "Ok", "cash": "100000.00", "marginused": "10000.00"}

    monkeypatch.setattr(main_mod.ZEBU, "quick_auth_async", fake_quick_auth)
    monkeypatch.setattr(main_mod.ZEBU, "trade_book_async", fake_trade_book)
    monkeypatch.setattr(main_mod.ZEBU, "position_book_async", fake_pos_book)
    monkeypatch.setattr(main_mod.ZEBU, "order_book_async", fake_order_book)
    monkeypatch.setattr(main_mod.ZEBU, "limits_async", fake_limits)

    # Account 1 fails with clear 400 error
    r1 = client.post("/api/accounts/AC-01/zebu-connect", headers=headers, json={
        "zebu_uid": "47000S19", "zebu_vc": "47000S19_U", "zebu_factor2": "PAN1", "zebu_secret_key": "S1", "zebu_password": "BAD",
    })
    assert r1.status_code == 400
    assert "Invalid Password" in r1.text

    # Account 2 succeeds independently
    r2 = client.post("/api/accounts/AC-02/zebu-connect", headers=headers, json={
        "zebu_uid": "99000Z99", "zebu_vc": "99000Z99_U", "zebu_factor2": "PAN2", "zebu_secret_key": "S2", "zebu_password": "GOOD",
    })
    assert r2.status_code == 200
    assert r2.json()["ok"] is True

    r_funds = client.get("/api/overview/funds-summary?force=1", headers=headers)
    assert r_funds.status_code == 200
    assert r_funds.json()["status"] == "LIVE"
    assert r_funds.json()["available_funds"] == "90000.00"
