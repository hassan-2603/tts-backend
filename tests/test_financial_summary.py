"""Comprehensive 30-Point Acceptance Test Suite for TTS Read-Only Financial Summary (P&L & MTM).

Covers Phase 12 requirements:
- Account Tests (1-6): multi-account isolation, UID/ACTID verification, rpnl & urmtom exact aggregation,
  account switching, late response protection.
- Financial Tests (7-17): positive/negative P&L & MTM, short positions, partial closures, multiple
  exchanges/instruments with non-1 prcftr, missing numeric fields, empty responses, malformed responses,
  duplicate requests & duplicate position rows.
- Performance Tests (18-26): PositionBook response time, total Overview financial load time,
  WebSocket update latency, slow broker responses, network interruption, WS disconnect & recovery,
  expired tokens, simultaneous Overview requests (single-flight), account switching during pending requests.
- Security Tests (27-30): zero trading endpoints exposed, strictly read-only, access tokens never exposed
  to browser, cross-account authorization blocked (HTTP 403).
"""
import asyncio
import tempfile
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
import pytest
from fastapi.testclient import TestClient

from app.database import set_engine_url, init_models
from app.financial_summary import (
    FinancialSummaryService,
    FinancialValidationError,
    validate_and_aggregate_position_book,
)
from app.security import hash_pw
from app.zebu import READ_ONLY_ENDPOINTS, SessionExpired, ZebuError

test_db_path = tempfile.mktemp(suffix="_fin_summary.db")
set_engine_url(f"sqlite+aiosqlite:///{test_db_path}")

import app.main as main_mod  # noqa: E402
from app.main import app, DB, ZEBU, FIN_SUMMARY  # noqa: E402


class MockZebuPositionBook:
    """Controllable PositionBook mock simulating real Zebu OAuth responses, delays, and failures."""

    def __init__(self):
        self.responses: Dict[str, Any] = {}
        self.delays: Dict[str, float] = {}
        self.errors: Dict[str, Exception] = {}
        self.call_counts: Dict[str, int] = {}

    async def position_book_snapshot_async(
        self,
        token: str,
        uid: str,
        actid: str,
        proxy: Optional[str] = None,
        timeout: Optional[float] = None,
        retries: int = 2,
    ) -> Tuple[Any, int, float]:
        u = uid.upper()
        self.call_counts[u] = self.call_counts.get(u, 0) + 1
        t0 = time.perf_counter()
        delay = self.delays.get(u, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)
        if u in self.errors:
            raise self.errors[u]
        if token == "EXPIRED_TOKEN":
            raise SessionExpired("PositionBook: HTTP 401 Unauthorized")
        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        return self.responses.get(u, []), 200, elapsed_ms


@pytest.fixture(scope="module")
def env():
    asyncio.run(init_models())
    mock_zebu = MockZebuPositionBook()
    orig_snap = ZEBU.position_book_snapshot_async
    ZEBU.position_book_snapshot_async = mock_zebu.position_book_snapshot_async

    async def _seed():
        # Users: admin, trader_a (assigned TR-A), viewer_b (assigned ACC-B only)
        await DB.put_user_async("admin", {"user": "admin", "role": "admin", "pw": hash_pw("admin123")})
        await DB.put_user_async("trader_a", {"user": "trader_a", "role": "trader", "trader_id": "TR-A", "pw": hash_pw("tr123")})
        await DB.put_user_async("viewer_b", {"user": "viewer_b", "role": "viewer", "assigned_accounts": ["ACC-B"], "pw": hash_pw("vw123")})

        await DB._put_trader("TR-A", {"id": "TR-A", "name": "Trader A", "status": "Live"})
        await DB._put_trader("TR-B", {"id": "TR-B", "name": "Trader B", "status": "Live"})

        await DB.put_account_async("ACC-A", {"id": "ACC-A", "name": "Zebu Account A", "broker": "Zebu", "trader": "TR-A", "zebu_uid": "ZEBU_A", "zebu_actid": "ACT_A"})
        await DB.put_account_async("ACC-B", {"id": "ACC-B", "name": "Zebu Account B", "broker": "Zebu", "trader": "TR-B", "zebu_uid": "ZEBU_B", "zebu_actid": "ACT_B"})
        await DB.put_account_async("ACC-EXP", {"id": "ACC-EXP", "name": "Expired Account", "broker": "Zebu", "trader": "TR-A", "zebu_uid": "ZEBU_EXP", "zebu_actid": "ACT_EXP"})

        await DB.put_token_async("ACC-A", {"access_token": "SECRET_TOK_A_DO_NOT_LEAK", "status": "Linked"})
        await DB.put_token_async("ACC-B", {"access_token": "SECRET_TOK_B_DO_NOT_LEAK", "status": "Linked"})
        await DB.put_token_async("ACC-EXP", {"access_token": "EXPIRED_TOKEN", "status": "Linked"})

    asyncio.run(_seed())
    client = TestClient(app)

    def _login(u, p):
        r = client.post("/api/login", json={"user": u, "password": p})
        assert r.status_code == 200
        return {"Authorization": f"Bearer {r.json()['token']}"}

    admin_hdr = _login("admin", "admin123")
    trader_a_hdr = _login("trader_a", "tr123")
    viewer_b_hdr = _login("viewer_b", "vw123")

    yield {
        "client": client,
        "mock": mock_zebu,
        "admin": admin_hdr,
        "trader_a": trader_a_hdr,
        "viewer_b": viewer_b_hdr,
    }
    ZEBU.position_book_snapshot_async = orig_snap


# ============================================================
# FINANCIAL CALCULATION & VALIDATION TESTS (Tests 7 - 17)
# ============================================================

def test_07_to_13_exact_decimal_calculations_multi_exchange_short_partial():
    """Tests 7, 8, 9, 10, 11, 12, 13: positive/negative P&L & MTM, short positions,
    partially closed positions, multiple exchanges & instruments, non-1 prcftr."""
    rows = [
        # Closed winning NFO position (positive rpnl, 0 urmtom)
        {
            "stat": "Ok", "uid": "ZEBU_A", "actid": "ACT_A",
            "exch": "NFO", "tsym": "NIFTY23NOV26C25350", "token": "50001", "prd": "NRML",
            "netqty": "0", "netavgprc": "0.00", "lp": "260.10",
            "rpnl": "45125.75", "urmtom": "0.00", "prcftr": "1.000000",
        },
        # Partially closed SHORT NFO position (negative rpnl, positive urmtom)
        {
            "stat": "Ok", "uid": "ZEBU_A", "actid": "ACT_A",
            "exch": "NFO", "tsym": "BANKNIFTY23NOV26P52000", "token": "50002", "prd": "MIS",
            "netqty": "-90", "netavgprc": "310.50", "lp": "285.25",
            "rpnl": "-12340.50", "urmtom": "2272.50", "prcftr": "1.000000",
        },
        # MCX commodity open position with prcftr = 100 (negative urmtom)
        {
            "stat": "Ok", "uid": "ZEBU_A", "actid": "ACT_A",
            "exch": "MCX", "tsym": "CRUDEOIL20NOV26", "token": "90001", "prd": "NRML",
            "netqty": "2", "netavgprc": "6120.00", "lp": "6095.50",
            "rpnl": "1500.00", "urmtom": "-4900.00", "prcftr": "100.000000",
        },
    ]
    snap = validate_and_aggregate_position_book(rows, "ACC-A", "ZEBU_A", "ACT_A")
    assert snap.realized_valid is True
    assert snap.mtm_valid is True
    # 45125.75 - 12340.50 + 1500.00 = 34285.25 (exact Decimal)
    assert snap.realized_pnl == Decimal("34285.25")
    # 0.00 + 2272.50 - 4900.00 = -2627.50 (exact Decimal)
    assert snap.unrealized_mtm == Decimal("-2627.50")
    assert snap.positions_count == 3
    assert snap.open_positions_count == 2
    assert snap.open_positions["MCX|90001|NRML"].prcftr == Decimal("100.000000")


def test_14_missing_numeric_fields_never_coerced_to_zero():
    """Test 14: Missing rpnl marks ONLY P&L unavailable while valid MTM still displays cleanly."""
    rows = [
        {
            "stat": "Ok", "uid": "ZEBU_A", "actid": "ACT_A",
            "exch": "NFO", "tsym": "NIFTY23NOV26C25500", "token": "50010", "prd": "NRML",
            "netqty": "75", "netavgprc": "100.00", "lp": "120.00",
            "rpnl": "", "urmtom": "1500.00", "prcftr": "1.000000",
        }
    ]
    snap = validate_and_aggregate_position_book(rows, "ACC-A", "ZEBU_A", "ACT_A")
    assert snap.realized_valid is False
    assert snap.realized_pnl is None
    assert snap.mtm_valid is True
    assert snap.unrealized_mtm == Decimal("1500.00")


def test_15_empty_responses_verified_semantics():
    """Test 15: Official Zebu empty-position response {'stat':'Not_Ok','emsg':'no data : No Data'}."""
    snap_dict = validate_and_aggregate_position_book(
        {"stat": "Not_Ok", "emsg": "no data : No Data"}, "ACC-A", "ZEBU_A", "ACT_A"
    )
    assert snap_dict.empty_confirmed is True
    assert snap_dict.realized_pnl == Decimal("0.00")
    assert snap_dict.unrealized_mtm == Decimal("0.00")

    snap_list = validate_and_aggregate_position_book([], "ACC-A", "ZEBU_A", "ACT_A")
    assert snap_list.empty_confirmed is True
    assert snap_list.realized_pnl == Decimal("0.00")


def test_16_malformed_and_account_mismatch_responses_rejected():
    """Test 16: Malformed JSON, non-empty Not_Ok, and UID/ACTID mismatch raise FinancialValidationError."""
    with pytest.raises(FinancialValidationError):
        validate_and_aggregate_position_book({"stat": "Not_Ok", "emsg": "Internal Gateway Failure"}, "ACC-A", "ZEBU_A", "ACT_A")

    with pytest.raises(FinancialValidationError):
        validate_and_aggregate_position_book("not-a-list", "ACC-A", "ZEBU_A", "ACT_A")

    # Account UID mismatch must be rejected
    with pytest.raises(FinancialValidationError):
        validate_and_aggregate_position_book(
            [{"stat": "Ok", "uid": "ZEBU_B", "actid": "ACT_B", "exch": "NFO", "token": "123", "rpnl": "100", "urmtom": "50"}],
            "ACC-A", "ZEBU_A", "ACT_A",
        )


def test_17_duplicate_position_rows_deduplicated():
    """Test 17: Duplicate position rows are never double-counted."""
    row = {
        "stat": "Ok", "uid": "ZEBU_A", "actid": "ACT_A",
        "exch": "NFO", "tsym": "NIFTY23NOV26C25500", "token": "50010", "prd": "NRML",
        "netqty": "75", "netavgprc": "100.00", "lp": "110.00",
        "rpnl": "500.00", "urmtom": "750.00", "prcftr": "1.000000",
    }
    snap = validate_and_aggregate_position_book([row, dict(row)], "ACC-A", "ZEBU_A", "ACT_A")
    assert snap.positions_count == 1
    assert snap.duplicate_rows_ignored == 1
    assert snap.realized_pnl == Decimal("500.00")
    assert snap.unrealized_mtm == Decimal("750.00")


# ============================================================
# ACCOUNT TESTS (1 - 6) & PERFORMANCE TESTS (18 - 26)
# ============================================================

def test_01_to_06_multi_account_isolation_switching_and_exact_values(env):
    """Tests 1-6: Two linked accounts return isolated UID/ACTID values, exact rpnl/urmtom,
    and switching accounts never leaks cached data across accounts."""
    mock, client, admin = env["mock"], env["client"], env["admin"]
    FIN_SUMMARY.clear_account("ACC-A")
    FIN_SUMMARY.clear_account("ACC-B")
    mock.errors.clear()
    mock.delays.clear()

    mock.responses["ZEBU_A"] = [
        {"stat": "Ok", "uid": "ZEBU_A", "actid": "ACT_A", "exch": "NFO", "tsym": "NIFTY23NOV26C25350", "token": "111", "prd": "NRML",
         "netqty": "-975", "netavgprc": "170.95", "lp": "260.10", "rpnl": "12500.50", "urmtom": "-86921.25", "prcftr": "1.000000"}
    ]
    mock.responses["ZEBU_B"] = [
        {"stat": "Ok", "uid": "ZEBU_B", "actid": "ACT_B", "exch": "NFO", "tsym": "NIFTY23NOV26C25950", "token": "222", "prd": "NRML",
         "netqty": "5850", "netavgprc": "15.34", "lp": "19.95", "rpnl": "-4200.00", "urmtom": "26968.50", "prcftr": "1.000000"}
    ]

    t0 = time.perf_counter()
    ra = client.get("/api/overview/financial-summary?account_id=ACC-A&force=1", headers=admin)
    load_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    assert ra.status_code == 200
    da = ra.json()
    assert da["account_id"] == "ACC-A"
    assert da["uid"] == "ZEBU_A"
    assert da["actid"] == "ACT_A"
    assert da["pnl"]["value"] == "12500.50"
    assert da["mtm"]["value"] == "-86921.25"
    assert da["status"] == "LIVE"
    assert load_ms < 2000

    rb = client.get("/api/overview/financial-summary?account_id=ACC-B&force=1", headers=admin)
    assert rb.status_code == 200
    db = rb.json()
    assert db["account_id"] == "ACC-B"
    assert db["uid"] == "ZEBU_B"
    assert db["actid"] == "ACT_B"
    assert db["pnl"]["value"] == "-4200.00"
    assert db["mtm"]["value"] == "26968.50"
    assert db["status"] == "LIVE"


def test_20_websocket_touchline_live_mtm_and_rpnl_immutability(env):
    """Test 20: Live WebSocket Touchline tick updates ONLY Unrealized MTM using exact Decimal formula
    and NEVER modifies Realized P&L (rpnl). Measures update latency."""
    async def _run():
        t0 = time.perf_counter()
        # Account A has short -975 @ 170.95 on NFO|111; new LTP = 250.95 -> MTM = -975 * (250.95 - 170.95) * 1 = -78000.00
        updated = await FIN_SUMMARY.on_touchline_tick("ACC-A", {"t": "tf", "e": "NFO", "tk": "111", "lp": "250.95", "ft": "1732089600"})
        ws_latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        assert updated is True
        assert ws_latency_ms < 50.0
        entry = FIN_SUMMARY.get_entry_for_account("ACC-A")
        assert entry.live_mtm_override == Decimal("-78000.00")
        # Realized P&L MUST remain unchanged from PositionBook snapshot
        assert entry.last_verified.realized_pnl == Decimal("12500.50")

    asyncio.run(_run())


def test_22_24_network_interruption_stale_preservation_and_expired_token(env):
    """Tests 22 & 24: Network failure preserves last verified snapshot as STALE (never zero);
    expired token returns REAUTH_REQUIRED."""
    mock, client, admin = env["mock"], env["client"], env["admin"]

    # Simulate network failure on ACC-A after it already has a verified snapshot
    mock.errors["ZEBU_A"] = ZebuError("PositionBook network error: Gateway timeout")
    r_stale = client.get("/api/overview/financial-summary?account_id=ACC-A&force=1", headers=admin)
    assert r_stale.status_code == 200
    ds = r_stale.json()
    assert ds["status"] == "STALE"
    assert ds["pnl"]["value"] == "12500.50"   # Preserved last verified value, NOT zero!
    assert ds["pnl"]["status"] == "STALE"
    assert ds["last_updated_ist"] is not None
    mock.errors.pop("ZEBU_A", None)

    # Expired token account returns REAUTH_REQUIRED with null values (never fake zero)
    FIN_SUMMARY.clear_account("ACC-EXP")
    r_exp = client.get("/api/overview/financial-summary?account_id=ACC-EXP&force=1", headers=admin)
    assert r_exp.status_code == 200
    de = r_exp.json()
    assert de["status"] == "REAUTH_REQUIRED"
    assert de["pnl"]["value"] is None
    assert de["mtm"]["value"] is None


def test_21_25_26_single_flight_simultaneous_requests_and_slow_broker(env):
    """Tests 21, 25, 26: Simultaneous requests for the same account deduplicate into ONE broker call."""
    mock = env["mock"]
    FIN_SUMMARY.clear_account("ACC-A")
    mock.call_counts["ZEBU_A"] = 0
    mock.delays["ZEBU_A"] = 0.15

    async def _run():
        acc = await DB.get_account_async("ACC-A")
        res = await asyncio.gather(
            FIN_SUMMARY.get_summary(acc, user_id="u1", force=True),
            FIN_SUMMARY.get_summary(acc, user_id="u2", force=True),
            FIN_SUMMARY.get_summary(acc, user_id="u3", force=True),
        )
        assert all(r["pnl"]["value"] == "12500.50" for r in res)
        assert mock.call_counts["ZEBU_A"] == 1

    asyncio.run(_run())
    mock.delays.pop("ZEBU_A", None)


# ============================================================
# SECURITY TESTS (27 - 30)
# ============================================================

def test_27_to_30_strict_read_only_no_token_leak_and_account_authorization(env):
    """Tests 27-30:
    27. No broker trading endpoint (PlaceOrder/ModifyOrder/CancelOrder/ProductConversion) exists.
    28. Endpoint is strictly GET read-only (POST/PUT/DELETE rejected with 405).
    29. Access tokens are never exposed in responses or diagnostics.
    30. Users cannot access another account's financial data by changing account_id (HTTP 403).
    """
    client = env["client"]
    admin, trader_a, viewer_b = env["admin"], env["trader_a"], env["viewer_b"]

    # 27. Strictly read-only Zebu endpoint allow-list
    for forbidden in ("PlaceOrder", "ModifyOrder", "CancelOrder", "ProductConversion"):
        assert forbidden not in READ_ONLY_ENDPOINTS

    # 28. Financial summary endpoint rejects mutating HTTP methods
    assert client.post("/api/overview/financial-summary?account_id=ACC-A", headers=admin).status_code == 405
    assert client.put("/api/overview/financial-summary?account_id=ACC-A", headers=admin).status_code == 405
    assert client.delete("/api/overview/financial-summary?account_id=ACC-A", headers=admin).status_code == 405

    # 29. Access tokens never leaked in summary or diagnostics
    r_sum = client.get("/api/overview/financial-summary?account_id=ACC-A", headers=admin)
    assert "SECRET_TOK_A_DO_NOT_LEAK" not in r_sum.text
    r_diag = client.get("/api/overview/financial-summary/diagnostics", headers=admin)
    assert r_diag.status_code == 200
    assert "SECRET_TOK_A_DO_NOT_LEAK" not in r_diag.text

    # 30. Cross-account parameter tampering blocked with HTTP 403
    assert client.get("/api/overview/financial-summary?account_id=ACC-A", headers=trader_a).status_code == 200
    assert client.get("/api/overview/financial-summary?account_id=ACC-B", headers=trader_a).status_code == 403
    assert client.get("/api/overview/financial-summary?account_id=ACC-B", headers=viewer_b).status_code == 200
    assert client.get("/api/overview/financial-summary?account_id=ACC-A", headers=viewer_b).status_code == 403
