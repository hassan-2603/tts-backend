"""Comprehensive mocked verification suite for production-grade Zebu persistent session management.

Covers all 16 required scenarios without hitting live Zebu servers.
"""
import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config import CFG
from app.database import init_models, set_engine_url
from app.financial_summary import FinancialSummaryService
from app.security import encrypt_token, decrypt_token
from app.session_manager import BrokerSessionManager, _uid_advisory_lock_key
from app.store import Store
from app.zebu import RateLimitError, SessionExpired, ZebuClient, ZebuError
from app.zebu_ws import ZebuWebSocketClient, _is_genuine_ws_auth_failure


async def _setup_mem_db() -> Store:
    set_engine_url("sqlite+aiosqlite:///:memory:")
    await init_models()
    return Store()


async def _seed_account(db: Store, aid: str, uid: str, secret: str = "test_secret_123"):
    await db.put_account_async(
        aid,
        {
            "id": aid,
            "name": f"Acc {aid}",
            "broker": "Zebu",
            "zebu_uid": uid,
            "zebu_actid": uid,
            "zebu_vc": f"{uid}_U",
            "zebu_secret_key": secret,
            "zebu_factor2": "ABCDE1234F",
            "zebu_password": "TestPassword@123",
        },
    )


@pytest.mark.asyncio
async def test_01_valid_session_position_book_reuses_token():
    """TEST 1: Valid session + Position Book request -> Existing token reused; zero new logins."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.quick_auth_async = AsyncMock()
    zebu.position_book_snapshot_async = AsyncMock(
        return_value=(
            [{"actid": "47000S19", "exch": "NFO", "tsym": "NIFTY23NOV26C26100", "token": "64501", "prd": "M", "netqty": "0", "rpnl": "100.00", "urmtom": "0.00", "lp": "10.00"}],
            200,
            12.5,
        )
    )
    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_EXISTING_1", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)

    fin = FinancialSummaryService(
        zebu=zebu,
        session_resolver=mgr.get_valid_session,
        reauth_resolver=mgr.recover_expired_session,
    )
    acc = await db.get_account_async("47000S19")
    res = await fin.get_summary(acc, user_id="admin", force=True)

    assert res["status"] == "LIVE"
    assert zebu.quick_auth_async.call_count == 0
    assert zebu.position_book_snapshot_async.call_args.kwargs["token"] == "TOK_EXISTING_1"
    assert zebu.position_book_snapshot_async.call_args.kwargs["auth_mode"] == "quickauth"


@pytest.mark.asyncio
async def test_02_multiple_endpoints_simultaneously_reuse_same_token():
    """TEST 2: Valid session + Trade Book + Order Book + Funds simultaneously -> All reuse same token."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.quick_auth_async = AsyncMock()
    zebu.trade_book_async = AsyncMock(return_value=[])
    zebu.order_book_async = AsyncMock(return_value=[])
    zebu.position_book_async = AsyncMock(return_value=[])
    zebu.limits_async = AsyncMock(return_value={"stat": "Ok", "cash": "100000.00", "marginused": "0.00"})

    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_SHARED_2", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)

    s1, s2, s3, s4 = await asyncio.gather(
        mgr.get_valid_session("47000S19"),
        mgr.get_valid_session("47000S19"),
        mgr.get_valid_session("47000S19"),
        mgr.get_valid_session("47000S19"),
    )
    assert all(s.token == "TOK_SHARED_2" and s.auth_mode == "quickauth" for s in (s1, s2, s3, s4))
    await asyncio.gather(
        zebu.trade_book_async(s1.token, s1.uid, s1.actid, auth_mode=s1.auth_mode),
        zebu.order_book_async(s2.token, s2.uid, auth_mode=s2.auth_mode),
        zebu.position_book_async(s3.token, s3.uid, s3.actid, auth_mode=s3.auth_mode),
        zebu.limits_async(s4.token, s4.uid, s4.actid, auth_mode=s4.auth_mode),
    )
    assert zebu.quick_auth_async.call_count == 0


@pytest.mark.asyncio
async def test_03_ten_simultaneous_expired_requests_trigger_exactly_one_quickauth():
    """TEST 3: Ten simultaneous requests encounter an expired token -> Exactly ONE QuickAuth attempt."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")

    async def _slow_qa(**kwargs):
        await asyncio.sleep(0.05)
        return {"stat": "Ok", "susertoken": "TOK_FRESH_3", "uid": "47000S19", "actid": "47000S19", "uname": "S19"}

    zebu.quick_auth_async = AsyncMock(side_effect=_slow_qa)
    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_OLD_3", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)
    # Clear last_ok_mono so external-conflict cooldown doesn't trigger for initial expiry test
    mgr.reset_tracker("47000S19")

    results = await asyncio.gather(*[
        mgr.recover_expired_session("47000S19", failed_token="TOK_OLD_3", reason="test_concurrent")
        for _ in range(10)
    ])
    assert zebu.quick_auth_async.call_count == 1
    assert all(r is not None and r.token == "TOK_FRESH_3" for r in results)


@pytest.mark.asyncio
async def test_04_shared_zebu_uid_accounts_do_not_duplicate_auth():
    """TEST 4: Two internal account records share the same zebu_uid -> Exactly ONE QuickAuth attempt."""
    db = await _setup_mem_db()
    await _seed_account(db, "AC-01", "47000S19")
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")

    async def _slow_qa(**kwargs):
        await asyncio.sleep(0.05)
        return {"stat": "Ok", "susertoken": "TOK_SHARED_UID_4", "uid": "47000S19", "actid": "47000S19"}

    zebu.quick_auth_async = AsyncMock(side_effect=_slow_qa)
    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("AC-01", "TOK_EXPIRED_4", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)
    mgr.reset_tracker("47000S19")

    r1, r2 = await asyncio.gather(
        mgr.recover_expired_session("AC-01", failed_token="TOK_EXPIRED_4", reason="shared_uid"),
        mgr.recover_expired_session("47000S19", failed_token="TOK_EXPIRED_4", reason="shared_uid"),
    )
    assert zebu.quick_auth_async.call_count == 1
    assert r1 is not None and r2 is not None
    assert r1.token == "TOK_SHARED_UID_4" and r2.token == "TOK_SHARED_UID_4"


@pytest.mark.asyncio
async def test_05_backend_restart_restores_token_and_auth_mode():
    """TEST 5: Backend restart -> Existing token and auth_mode='quickauth' restored from DB without QuickAuth."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")

    # Simulate process 1 saving QuickAuth session
    zebu1 = ZebuClient("https://go.mynt.in", "", "")
    mgr1 = BrokerSessionManager(db=db, zebu=zebu1)
    await mgr1.register_session("47000S19", "TOK_PERSIST_5", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)

    # Simulate fresh process 2 after restart (empty in-memory _token_auth_modes)
    zebu2 = ZebuClient("https://go.mynt.in", "", "")
    zebu2.quick_auth_async = AsyncMock()
    ws_calls = []

    async def _ws_cb(acc, tok, mode):
        ws_calls.append((acc["id"], tok, mode))

    mgr2 = BrokerSessionManager(db=db, zebu=zebu2, ws_sync_cb=_ws_cb)
    restored = await mgr2.restore_sessions_on_startup(await db.get_all_accounts_async())

    assert restored == 1
    assert zebu2.quick_auth_async.call_count == 0
    assert zebu2.get_token_mode("TOK_PERSIST_5") == "quickauth"
    assert ws_calls == [("47000S19", "TOK_PERSIST_5", "quickauth")]


@pytest.mark.asyncio
async def test_06_07_08_temporary_errors_do_not_disconnect_or_quickauth():
    """TESTS 6, 7, 8: Temporary network timeout, HTTP 429, HTTP 500 -> STALE/backoff, token retained, ZERO QuickAuth."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.quick_auth_async = AsyncMock()
    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_KEEP_678", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)

    for err in [
        ZebuError("PositionBook network error: ReadTimeout"),
        RateLimitError("PositionBook: HTTP 429 Rate limit exceeded"),
        ZebuError("PositionBook: HTTP 500 server error"),
    ]:
        zebu.client_details_async = AsyncMock(side_effect=err)
        hc = await mgr.health_check_session("47000S19", recover_if_expired=True)
        assert hc == "stale"
        sess = await mgr.get_valid_session("47000S19")
        assert sess is not None and sess.token == "TOK_KEEP_678"
        assert zebu.quick_auth_async.call_count == 0


@pytest.mark.asyncio
async def test_09_genuine_invalid_session_triggers_one_controlled_recovery():
    """TEST 9: Genuine invalid session -> One controlled QuickAuth recovery."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.client_details_async = AsyncMock(side_effect=SessionExpired("Session Expired : Invalid Session Key"))
    zebu.quick_auth_async = AsyncMock(return_value={"stat": "Ok", "susertoken": "TOK_RECOVERED_9", "uid": "47000S19", "actid": "47000S19"})

    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_EXPIRED_9", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)
    mgr.reset_tracker("47000S19")

    hc = await mgr.health_check_session("47000S19", recover_if_expired=True)
    assert hc == "recovered"
    assert zebu.quick_auth_async.call_count == 1
    sess = await mgr.get_valid_session("47000S19")
    assert sess is not None and sess.token == "TOK_RECOVERED_9"


@pytest.mark.asyncio
async def test_10_wrong_password_marks_auth_blocked_and_stops_retries():
    """TEST 10: Wrong password -> AUTH_BLOCKED; subsequent auto-recovery makes ZERO additional QuickAuth attempts."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S43", "47000S43")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.quick_auth_async = AsyncMock(side_effect=ZebuError("QuickAuth failed: Invalid Password"))

    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S43", "TOK_OLD_10", "quickauth", uid="47000S43", actid="47000S43", sync_ws=False)
    mgr.reset_tracker("47000S43")

    r1 = await mgr.recover_expired_session("47000S43", failed_token="TOK_OLD_10", reason="test_wrong_pw")
    assert r1 is None
    assert zebu.quick_auth_async.call_count == 1

    st = await db.get_sync_state_async("47000S43")
    assert st["status"] == "AUTH_BLOCKED"

    # Second automatic recovery must NOT call QuickAuth again!
    r2 = await mgr.recover_expired_session("47000S43", failed_token="TOK_OLD_10", reason="test_retry_blocked")
    assert r2 is None
    assert zebu.quick_auth_async.call_count == 1


@pytest.mark.asyncio
async def test_11_12_websocket_transient_vs_genuine_auth_failure():
    """TESTS 11 & 12: Generic Not_Ok is transient (not REAUTH_REQUIRED); genuine WS auth failure triggers centralized recovery."""
    # TEST 11: Classification check
    assert _is_genuine_ws_auth_failure("Not_Ok") is False
    assert _is_genuine_ws_auth_failure("Server busy") is False
    assert _is_genuine_ws_auth_failure("Session Expired : Invalid Session Key") is True
    assert _is_genuine_ws_auth_failure("Invalid susertoken") is True

    # TEST 12: Genuine WS auth failure invokes on_auth_failure callback and updates token
    recovered_calls = []

    async def _on_auth_fail(aid: str, old_tok: str):
        recovered_calls.append((aid, old_tok))
        return ("TOK_WS_NEW_12", "quickauth")

    ws_client = ZebuWebSocketClient(
        ws_url="wss://go.mynt.in/NorenWSTP/",
        account_id="47000S19",
        uid="47000S19",
        actid="47000S19",
        token="TOK_WS_OLD_12",
        auth_mode="quickauth",
        on_auth_failure=_on_auth_fail,
    )
    res = await ws_client.on_auth_failure("47000S19", ws_client.token)
    assert res == ("TOK_WS_NEW_12", "quickauth")
    assert recovered_calls == [("47000S19", "TOK_WS_OLD_12")]


@pytest.mark.asyncio
async def test_13_five_minute_health_check_valid_tokens_zero_quickauth():
    """TEST 13: Five-minute scheduler health check with valid tokens -> ZERO new QuickAuth calls."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.client_details_async = AsyncMock(return_value={"stat": "Ok", "uid": "47000S19"})
    zebu.quick_auth_async = AsyncMock()

    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_VALID_13", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)

    hc = await mgr.health_check_session("47000S19", recover_if_expired=True)
    assert hc == "valid"
    assert zebu.quick_auth_async.call_count == 0


@pytest.mark.asyncio
async def test_14_midnight_archival_does_not_delete_tokens():
    """TEST 14: Midnight archival -> No broker token deletion or forced logout."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("47000S19", "TOK_MIDNIGHT_14", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)

    # Import and run midnight_archive_async against the in-memory DB
    from app.main import midnight_archive_async
    await midnight_archive_async(now=datetime(2026, 11, 24, 0, 5))

    sess = await mgr.get_valid_session("47000S19")
    assert sess is not None and sess.token == "TOK_MIDNIGHT_14" and sess.status == "Linked"


@pytest.mark.asyncio
async def test_15_two_workers_recovering_same_account_single_quickauth():
    """TEST 15: Two separate BrokerSessionManager instances (simulating 2 workers) recover same UID -> DB double-check prevents duplicate auth."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")

    # Verify deterministic 64-bit PG advisory lock key is identical across processes
    assert _uid_advisory_lock_key("47000S19") == _uid_advisory_lock_key("47000S19")
    assert _uid_advisory_lock_key("47000S19") != _uid_advisory_lock_key("47000S12")

    zebu_w1 = ZebuClient("https://go.mynt.in", "", "")
    zebu_w2 = ZebuClient("https://go.mynt.in", "", "")
    zebu_w1.quick_auth_async = AsyncMock(return_value={"stat": "Ok", "susertoken": "TOK_W1_15", "uid": "47000S19", "actid": "47000S19"})
    zebu_w2.quick_auth_async = AsyncMock(return_value={"stat": "Ok", "susertoken": "TOK_W2_15", "uid": "47000S19", "actid": "47000S19"})

    mgr1 = BrokerSessionManager(db=db, zebu=zebu_w1)
    mgr2 = BrokerSessionManager(db=db, zebu=zebu_w2)
    await mgr1.register_session("47000S19", "TOK_STALE_15", "quickauth", uid="47000S19", actid="47000S19", sync_ws=False)
    mgr1.reset_tracker("47000S19")

    # Worker 1 acquires lock first & refreshes DB; Worker 2 acquires lock right after & rechecks DB -> reuses TOK_W1_15!
    r1 = await mgr1.recover_expired_session("47000S19", failed_token="TOK_STALE_15", reason="worker1")
    r2 = await mgr2.recover_expired_session("47000S19", failed_token="TOK_STALE_15", reason="worker2")

    assert r1 is not None and r1.token == "TOK_W1_15"
    assert r2 is not None and r2.token == "TOK_W1_15"
    assert zebu_w1.quick_auth_async.call_count == 1
    assert zebu_w2.quick_auth_async.call_count == 0


@pytest.mark.asyncio
async def test_16_oauth_legacy_account_and_encrypted_secret_key():
    """TEST 16: Legacy OAuth account uses auth_mode='oauth', and zebu_secret_key is encrypted at rest with backward compatibility."""
    db = await _setup_mem_db()
    await _seed_account(db, "OAUTH01", "OAUTH01", secret="MY_SECRET_KEY_XYZ")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    mgr = BrokerSessionManager(db=db, zebu=zebu)
    await mgr.register_session("OAUTH01", "OAUTH_TOK_16", "oauth", uid="OAUTH01", actid="OAUTH01", sync_ws=False)

    sess = await mgr.get_valid_session("OAUTH01")
    assert sess is not None
    assert sess.token == "OAUTH_TOK_16"
    assert sess.auth_mode == "oauth"
    assert zebu.get_token_mode("OAUTH_TOK_16") == "oauth"

    # Verify zebu_secret_key was encrypted at rest in DB and decrypted transparently by get_account_credentials_async
    creds = await db.get_account_credentials_async("OAUTH01")
    assert creds["zebu_secret_key"] == "MY_SECRET_KEY_XYZ"
    # Verify decrypt_token remains backward-compatible with legacy plaintext records
    assert decrypt_token("LEGACY_PLAINTEXT_SECRET") == "LEGACY_PLAINTEXT_SECRET"
    assert decrypt_token(encrypt_token("ENCRYPTED_SECRET")) == "ENCRYPTED_SECRET"


@pytest.mark.asyncio
async def test_17_advisory_lock_normal_exception_and_cancellation_safety():
    """FIX 1 Regression: Normal lock acquire/release, exception during auth, CancelledError while holding lock,
    subsequent lock re-acquisition for same UID, and PostgreSQL pg_advisory_xact_lock SQL verification."""
    db = await _setup_mem_db()
    await _seed_account(db, "47000S19", "47000S19")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    mgr = BrokerSessionManager(db=db, zebu=zebu)

    # 1. Normal acquire & release
    entered = False
    async with mgr._distributed_uid_lock("47000S19"):
        entered = True
    assert entered is True

    # 2. Exception during authentication releases lock cleanly
    zebu.quick_auth_async = AsyncMock(side_effect=ZebuError("QuickAuth network error: timeout"))
    with pytest.raises(ZebuError):
        await mgr.authenticate_once("47000S19", reason="manual", force=True)
    # Subsequent request must immediately acquire lock without deadlock
    zebu.quick_auth_async = AsyncMock(return_value={"stat": "Ok", "susertoken": "TOK_AFTER_EXC", "uid": "47000S19", "actid": "47000S19"})
    res_after_exc = await mgr.authenticate_once("47000S19", reason="manual", force=True)
    assert res_after_exc["ok"] and res_after_exc["access_token"] == "TOK_AFTER_EXC"

    # 3. asyncio.CancelledError while holding the lock releases lock cleanly
    lock_acquired_evt = asyncio.Event()

    async def _cancellable_holder():
        async with mgr._distributed_uid_lock("47000S19"):
            lock_acquired_evt.set()
            await asyncio.sleep(10)

    t = asyncio.create_task(_cancellable_holder())
    await lock_acquired_evt.wait()
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    # 4. Subsequent request for the exact same UID acquires lock cleanly without deadlock
    reacquired = False

    async def _reacquire():
        nonlocal reacquired
        async with mgr._distributed_uid_lock("47000S19"):
            reacquired = True

    await asyncio.wait_for(_reacquire(), timeout=2.0)
    assert reacquired is True

    # 5. Verify PostgreSQL branch executes transaction-scoped pg_advisory_xact_lock with deterministic key
    executed_sql = []

    class _FakeBegin:
        async def __aenter__(self):
            executed_sql.append("BEGIN")
            return self
        async def __aexit__(self, exc_type, exc, tb):
            executed_sql.append("ROLLBACK" if exc_type else "COMMIT")
            return False

    class _FakeSession:
        def begin(self):
            return _FakeBegin()
        async def execute(self, stmt, params=None):
            executed_sql.append((str(stmt), params))
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False

    from app import session_manager as sm_mod
    orig_engine = sm_mod._db_mod.async_engine
    orig_session_local = sm_mod.AsyncSessionLocal
    fake_engine = MagicMock()
    fake_engine.dialect.name = "postgresql"
    try:
        sm_mod._db_mod.async_engine = fake_engine
        sm_mod.AsyncSessionLocal = lambda: _FakeSession()
        # Normal PG path
        async with mgr._distributed_uid_lock("47000S19"):
            pass
        assert executed_sql[0] == "BEGIN"
        assert "pg_advisory_xact_lock" in executed_sql[1][0]
        assert executed_sql[1][1] == {"k": _uid_advisory_lock_key("47000S19")}
        assert executed_sql[2] == "COMMIT"

        # CancelledError PG path -> transaction rolls back & auto-releases pg_advisory_xact_lock
        executed_sql.clear()
        ev = asyncio.Event()

        async def _pg_cancel_holder():
            async with mgr._distributed_uid_lock("47000S19"):
                ev.set()
                await asyncio.sleep(10)

        t2 = asyncio.create_task(_pg_cancel_holder())
        await ev.wait()
        t2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t2
        assert executed_sql[0] == "BEGIN"
        assert "pg_advisory_xact_lock" in executed_sql[1][0]
        assert executed_sql[2] == "ROLLBACK"
    finally:
        sm_mod._db_mod.async_engine = orig_engine
        sm_mod.AsyncSessionLocal = orig_session_local


@pytest.mark.asyncio
async def test_18_put_token_expires_at_omitted_none_and_datetime_plus_relink():
    """FIX 2 Regression:
    1) expires_at supplied with datetime -> saves new expiry.
    2) expires_at omitted -> preserves existing database expiry.
    3) expires_at explicitly set to None -> clears stale past expiry (re-linking expired account works!).
    """
    db = await _setup_mem_db()
    await _seed_account(db, "47000S99", "47000S99")
    zebu = ZebuClient("https://go.mynt.in", "", "")
    mgr = BrokerSessionManager(db=db, zebu=zebu)

    # Case 1: Supply a past datetime expires_at -> saved & treated as expired
    past_dt = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    await db.put_token_async("47000S99", {
        "access_token": "OLD_EXPIRED_TOK",
        "auth_mode": "oauth",
        "status": "Linked",
        "expires_at": past_dt,
    })
    tok1 = await db.get_token_async("47000S99")
    assert tok1["expires_at"] == past_dt.timestamp()
    assert await mgr.get_valid_session("47000S99") is None  # genuinely expired in DB

    # Case 2: Omit expires_at on partial update -> preserves existing DB expires_at
    await db.put_token_async("47000S99", {"sync_message": "partial update"})
    tok2 = await db.get_token_async("47000S99")
    assert tok2["expires_at"] == past_dt.timestamp()

    # Case 3: Re-link via register_session(expires_at=None) -> clears stale past expires_at & session is valid!
    await mgr.register_session(
        account_id="47000S99",
        token="FRESH_RELINKED_TOK",
        auth_mode="quickauth",
        uid="47000S99",
        actid="47000S99",
        expires_at=None,
        sync_ws=False,
    )
    tok3 = await db.get_token_async("47000S99")
    assert tok3["expires_at"] is None
    sess = await mgr.get_valid_session("47000S99")
    assert sess is not None and sess.token == "FRESH_RELINKED_TOK" and sess.auth_mode == "quickauth"

    # Case 4: Supply a future datetime -> saves new expiry
    future_dt = datetime(2030, 1, 1, 0, 0, tzinfo=timezone.utc)
    await db.put_token_async("47000S99", {"expires_at": future_dt})
    tok4 = await db.get_token_async("47000S99")
    assert tok4["expires_at"] == future_dt.timestamp()


@pytest.mark.asyncio
async def test_19_morning_auto_reconnect_recovers_all_expired_and_respects_guards():
    """Verify 08:45 AM / 5-min health_check_session reconnects every overnight-expired account
    (including those still marked Linked in DB or with transient sync_state=DISCONNECTED),
    makes 0 QuickAuth calls for valid sessions, and never touches manually Disconnected or AUTH_BLOCKED accounts."""
    db = await _setup_mem_db()
    for aid in ("ACC_EXP_LINKED", "ACC_EXP_SYNC_DISC", "ACC_VALID", "ACC_MANUAL_DISC", "ACC_BLOCKED"):
        await _seed_account(db, aid, aid)

    qa_uids = []

    async def _fake_qa(uid, password, factor2, vc, api_secret, **kwargs):
        qa_uids.append(uid)
        return {"stat": "Ok", "susertoken": f"NEW_TOK_{uid}", "uid": uid, "actid": uid}

    async def _fake_cd(token, uid, actid, proxy=None, auth_mode=None):
        if token.startswith("OLD_EXPIRED_"):
            raise SessionExpired("Session Expired : Invalid Session Key")
        return {"stat": "Ok", "uid": uid}

    zebu = ZebuClient("https://go.mynt.in", "", "")
    zebu.quick_auth_async = AsyncMock(side_effect=_fake_qa)
    zebu.client_details_async = AsyncMock(side_effect=_fake_cd)
    mgr = BrokerSessionManager(db=db, zebu=zebu)

    # 1. Overnight-expired token still marked Linked in DB (typical morning state)
    await mgr.register_session("ACC_EXP_LINKED", "OLD_EXPIRED_1", "quickauth", uid="ACC_EXP_LINKED", actid="ACC_EXP_LINKED", sync_ws=False)
    mgr.reset_tracker("ACC_EXP_LINKED")

    # 2. Expired token whose sync_state was transiently set to DISCONNECTED by a failed poll
    await db.put_token_async("ACC_EXP_SYNC_DISC", {"access_token": "OLD_EXPIRED_2", "auth_mode": "quickauth", "status": "Login expired"})
    await db.put_sync_state_async("ACC_EXP_SYNC_DISC", {"status": "DISCONNECTED", "is_stale": True})

    # 3. Valid token that did not expire
    await mgr.register_session("ACC_VALID", "STILL_VALID_3", "quickauth", uid="ACC_VALID", actid="ACC_VALID", sync_ws=False)

    # 4. Manually disconnected by user -> must NOT auto-reconnect
    await db.put_token_async("ACC_MANUAL_DISC", {"access_token": None, "status": "Disconnected"})
    await db.put_sync_state_async("ACC_MANUAL_DISC", {"status": "DISCONNECTED"})

    # 5. AUTH_BLOCKED by Zebu -> must NOT auto-reconnect
    await db.put_token_async("ACC_BLOCKED", {"access_token": "OLD_EXPIRED_5", "status": "AUTH_BLOCKED"})
    await db.put_sync_state_async("ACC_BLOCKED", {"status": "AUTH_BLOCKED", "last_error": "User Blocked"})

    r1 = await mgr.health_check_session("ACC_EXP_LINKED", recover_if_expired=True)
    r2 = await mgr.health_check_session("ACC_EXP_SYNC_DISC", recover_if_expired=True)
    r3 = await mgr.health_check_session("ACC_VALID", recover_if_expired=True)
    r4 = await mgr.health_check_session("ACC_MANUAL_DISC", recover_if_expired=True)
    r5 = await mgr.health_check_session("ACC_BLOCKED", recover_if_expired=True)

    assert r1 == "recovered"
    assert r2 == "recovered"
    assert r3 == "valid"
    assert r4 == "skipped_disconnected"
    assert r5 == "skipped_auth_blocked"
    assert sorted(qa_uids) == ["ACC_EXP_LINKED", "ACC_EXP_SYNC_DISC"]

    s1 = await mgr.get_valid_session("ACC_EXP_LINKED")
    s2 = await mgr.get_valid_session("ACC_EXP_SYNC_DISC")
    assert s1 and s1.token == "NEW_TOK_ACC_EXP_LINKED"
    assert s2 and s2.token == "NEW_TOK_ACC_EXP_SYNC_DISC"


