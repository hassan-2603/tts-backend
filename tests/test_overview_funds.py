"""Verification Suite: Overview Total Available Funds & Total Utilized Funds (Zebu Limits API).

Tests:
1. Official Zebu Limits response fields (cash, brkcollamt, payin, payout, marginused) exact Decimal formula.
2. Multi-account aggregation across multiple linked broker accounts.
3. Duplicate / shared broker account deduplication (same uid+actid referenced by 2 traders counted ONCE).
4. Partial API failure (1 account succeeds, 1 fails -> PARTIAL status, sums succeeded account, reports failure).
5. Full API failure & expired session (never silently displays zero -> UNAVAILABLE or STALE with prior verified total).
6. Invalid / missing numeric fields in Limits response rejected cleanly (never coerced to zero).
7. Strict per-user authorization (viewer/trader only aggregates authorized accounts).
"""
import asyncio
import tempfile
from typing import Dict
import pytest
from fastapi.testclient import TestClient

from app.database import set_engine_url, init_models
from app.security import hash_pw
from app.zebu import SessionExpired, ZebuError

test_db_path = tempfile.mktemp(suffix="_overview_funds.db")
set_engine_url(f"sqlite+aiosqlite:///{test_db_path}")

import app.main as main_mod  # noqa: E402
from app.main import app, DB, ZEBU, _compute_account_funds_from_limits  # noqa: E402


class MockLimitsBroker:
    def __init__(self):
        self.limits: Dict[str, dict] = {}
        self.errors: Dict[str, Exception] = {}
        self.call_counts: Dict[str, int] = {}

    async def limits_async(self, token: str, uid: str, actid: str, proxy=None) -> dict:
        u = uid.upper()
        self.call_counts[u] = self.call_counts.get(u, 0) + 1
        if u in self.errors:
            raise self.errors[u]
        return dict(self.limits[u])


@pytest.fixture(scope="module")
def env():
    asyncio.run(init_models())
    mock = MockLimitsBroker()
    orig_limits = ZEBU.limits_async
    ZEBU.limits_async = mock.limits_async

    async def _seed():
        await DB.put_user_async("admin", {"user": "admin", "role": "admin", "pw": hash_pw("admin123")})
        await DB.put_user_async("viewer1", {"user": "viewer1", "role": "viewer", "assigned_accounts": ["ACC-1"], "pw": hash_pw("vw123")})

        await DB._put_trader("TR-1", {"id": "TR-1", "name": "Trader 1", "status": "Live"})
        await DB._put_trader("TR-2", {"id": "TR-2", "name": "Trader 2", "status": "Live"})

        # ACC-1 and ACC-1-DUP share the exact same Zebu UID+ACTID (UID_1) -> must be fetched & counted ONCE!
        await DB.put_account_async("ACC-1", {"id": "ACC-1", "name": "Zebu Account 1", "broker": "Zebu", "trader": "TR-1", "zebu_uid": "UID_1", "zebu_actid": "UID_1"})
        await DB.put_account_async("ACC-1-DUP", {"id": "ACC-1-DUP", "name": "Zebu Account 1 Shared", "broker": "Zebu", "trader": "TR-2", "zebu_uid": "UID_1", "zebu_actid": "UID_1"})
        await DB.put_account_async("ACC-2", {"id": "ACC-2", "name": "Zebu Account 2", "broker": "Zebu", "trader": "TR-2", "zebu_uid": "UID_2", "zebu_actid": "UID_2"})

        await DB.put_token_async("ACC-1", {"access_token": "TOK_1", "status": "Linked"})
        await DB.put_token_async("ACC-1-DUP", {"access_token": "TOK_1", "status": "Linked"})
        await DB.put_token_async("ACC-2", {"access_token": "TOK_2", "status": "Linked"})

    asyncio.run(_seed())
    client = TestClient(app)

    def _hdr(u, p):
        r = client.post("/api/login", json={"user": u, "password": p})
        assert r.status_code == 200
        return {"Authorization": f"Bearer {r.json()['token']}"}

    yield {"client": client, "mock": mock, "admin": _hdr("admin", "admin123"), "viewer1": _hdr("viewer1", "vw123")}
    ZEBU.limits_async = orig_limits


def test_01_unit_limits_formula_and_validation():
    # Valid full response: cash=500000 + brkcollamt=100000 + payin=50000 - payout=25000 - marginused=175000.50
    # Available = 449999.50, Utilized = 175000.50
    res = _compute_account_funds_from_limits({
        "stat": "Ok", "cash": "500000.00", "brkcollamt": "100000.00",
        "payin": "50000.00", "payout": "25000.00", "marginused": "175000.50",
    })
    assert res["ok"] is True
    assert str(res["available_funds"]) == "449999.50"
    assert str(res["utilized_funds"]) == "175000.50"

    # Missing cash -> rejected (never silently zero)
    bad = _compute_account_funds_from_limits({"stat": "Ok", "marginused": "100.00"})
    assert bad["ok"] is False

    # Not_Ok -> rejected
    bad2 = _compute_account_funds_from_limits({"stat": "Not_Ok", "emsg": "Session Expired"})
    assert bad2["ok"] is False


def test_02_multi_account_aggregation_and_duplicate_deduplication(env):
    mock, client, admin, viewer1 = env["mock"], env["client"], env["admin"], env["viewer1"]
    main_mod._FUNDS_CACHE.clear()
    main_mod._FUNDS_LAST_VERIFIED.clear()
    mock.errors.clear()
    mock.call_counts.clear()

    # UID_1: avail = 400000 + 50000 - 120000.25 = 329999.75; util = 120000.25
    mock.limits["UID_1"] = {"stat": "Ok", "cash": "400000.00", "brkcollamt": "50000.00", "marginused": "120000.25"}
    # UID_2: avail = 250000 + 0 - 80000.75 = 169999.25; util = 80000.75
    mock.limits["UID_2"] = {"stat": "Ok", "cash": "250000.00", "marginused": "80000.75"}

    r = client.get("/api/overview/funds-summary?force=1", headers=admin)
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "LIVE"
    assert d["accounts_linked"] == 2          # ACC-1 and ACC-1-DUP deduplicated to 1!
    assert d["accounts_succeeded"] == 2
    assert mock.call_counts["UID_1"] == 1     # Fetched EXACTLY ONCE!
    assert mock.call_counts["UID_2"] == 1
    # Total Available = 329999.75 + 169999.25 = 499999.00
    assert d["available_funds"] == "499999.00"
    # Total Utilized  = 120000.25 + 80000.75  = 200001.00
    assert d["utilized_funds"] == "200001.00"

    # Viewer1 (assigned ONLY ACC-1) sees ONLY UID_1's funds
    rv = client.get("/api/overview/funds-summary?force=1", headers=viewer1)
    assert rv.status_code == 200
    dv = rv.json()
    assert dv["accounts_linked"] == 1
    assert dv["available_funds"] == "329999.75"
    assert dv["utilized_funds"] == "120000.25"


def test_03_partial_failure_and_full_failure_stale_behavior(env):
    mock, client, admin = env["mock"], env["client"], env["admin"]

    # Partial failure: UID_2 fails with SessionExpired, UID_1 succeeds
    mock.errors["UID_2"] = SessionExpired("Token expired")
    try:
        r = client.get("/api/overview/funds-summary?force=1", headers=admin)
        assert r.status_code == 200
        d = r.json()
        assert d["status"] == "PARTIAL"
        assert d["accounts_succeeded"] == 1
        assert d["accounts_failed"] == 1
        assert d["available_funds"] == "329999.75"
        assert d["utilized_funds"] == "120000.25"
        assert "failed" in d["error"].lower()
    finally:
        mock.errors.pop("UID_2", None)

    # Full failure when prior verified total exists -> STALE (retains 499999.00 / 200001.00, never zero!)
    mock.errors["UID_1"] = ZebuError("Timeout")
    mock.errors["UID_2"] = ZebuError("Timeout")
    try:
        r2 = client.get("/api/overview/funds-summary?force=1", headers=admin)
        d2 = r2.json()
        assert d2["status"] == "STALE"
        assert d2["available_funds"] == "499999.00"
        assert d2["utilized_funds"] == "200001.00"

        # Full failure with NO prior verified total -> UNAVAILABLE with null values (never fake zero!)
        main_mod._FUNDS_LAST_VERIFIED.clear()
        r3 = client.get("/api/overview/funds-summary?force=1", headers=admin)
        d3 = r3.json()
        assert d3["status"] == "UNAVAILABLE"
        assert d3["available_funds"] is None
        assert d3["utilized_funds"] is None
    finally:
        mock.errors.clear()
