"""Tests for strict Admin vs Viewer RBAC, per-account viewer isolation, and password-change global logout."""
import asyncio
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


@pytest.fixture
def rbac_app(tmp_path, monkeypatch):
    db_file = tmp_path / "test_rbac.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_file.as_posix()}")
    monkeypatch.setenv("RUN_WORKER_IN_PROCESS", "false")

    import app.database as db_mod
    import app.store as store_mod
    import app.main as main_mod
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from app.database import Base

    engine_test = create_async_engine(f"sqlite+aiosqlite:///{db_file.as_posix()}", future=True)
    session_factory = async_sessionmaker(bind=engine_test, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(db_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(store_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(main_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(main_mod.CFG, "RUN_WORKER_IN_PROCESS", False)

    async def _init():
        async with engine_test.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await main_mod.DB.put_user_async("admin", {
            "user": "admin", "role": "admin", "pw": main_mod.hash_pw("adminpass"),
        })
        await main_mod.DB._put_trader("TR-1", {"id": "TR-1", "name": "Trader 1", "status": "Live"})
        await main_mod.DB._put_trader("TR-2", {"id": "TR-2", "name": "Trader 2", "status": "Testing"})
        await main_mod.DB.put_account_async("AC-1", {"id": "AC-1", "name": "Acc 1", "broker": "Zebu", "trader": "TR-1", "zebu_uid": "U1"})
        await main_mod.DB.put_account_async("AC-2", {"id": "AC-2", "name": "Acc 2", "broker": "Zebu", "trader": "TR-2", "zebu_uid": "U2"})
        await main_mod.DB._put_trade("T-1", {"id": "T-1", "account": "AC-1", "trader": "TR-1", "date": "2026-11-20", "time": "10:00", "symbol": "NIFTY", "pnl": 1000.0, "open": False})
        await main_mod.DB._put_trade("T-2", {"id": "T-2", "account": "AC-2", "trader": "TR-2", "date": "2026-11-20", "time": "10:05", "symbol": "BANKNIFTY", "pnl": -500.0, "open": False})
        await main_mod.DB._put_group("G-ALL", {"id": "G-ALL", "name": "All Accounts", "accounts": ["AC-1", "AC-2"]})
        await main_mod.DB._put_group("G-TWO", {"id": "G-TWO", "name": "Only Two", "accounts": ["AC-2"]})

    asyncio.run(_init())
    yield main_mod
    asyncio.run(engine_test.dispose())


def test_viewer_strict_account_isolation_read_only_and_password_global_logout(rbac_app):
    client = TestClient(rbac_app.app)

    # 1. Admin logs in
    r_admin = client.post("/api/login", json={"user": "admin", "password": "adminpass"})
    assert r_admin.status_code == 200
    admin_hdr = {"Authorization": f"Bearer {r_admin.json()['token']}"}

    # 2. Admin creates viewer1 assigned ONLY to AC-1, and viewer_empty with NO accounts
    r_c1 = client.put("/api/users/viewer1", headers=admin_hdr, json={
        "password": "v1password",
        "role": "viewer",
        "assigned_accounts": ["AC-1"],
    })
    assert r_c1.status_code == 200
    r_c2 = client.put("/api/users/viewer_empty", headers=admin_hdr, json={
        "password": "v2password",
        "role": "viewer",
        "assigned_accounts": [],
    })
    assert r_c2.status_code == 200

    # Verify GET /api/users returns assigned_accounts
    users_list = client.get("/api/users", headers=admin_hdr).json()
    v1_row = next(u for u in users_list if u["user"] == "viewer1")
    assert v1_row["assigned_accounts"] == ["AC-1"]

    # 3. viewer1 logs in on TWO separate devices
    v1_dev1 = client.post("/api/login", json={"user": "viewer1", "password": "v1password"}).json()["token"]
    v1_dev2 = client.post("/api/login", json={"user": "viewer1", "password": "v1password"}).json()["token"]
    v1_hdr1 = {"Authorization": f"Bearer {v1_dev1}"}
    v1_hdr2 = {"Authorization": f"Bearer {v1_dev2}"}

    # 4. viewer1 bootstrap sees ONLY AC-1, TR-1, T-1, and G-ALL filtered to [AC-1] (NOT G-TWO or AC-2!)
    boot = client.get("/api/bootstrap", headers=v1_hdr1).json()
    assert [a["id"] for a in boot["accounts"]] == ["AC-1"]
    assert [t["id"] for t in boot["traders"]] == ["TR-1"]
    assert [t["id"] for t in boot["trades"]] == ["T-1"]
    assert [g["id"] for g in boot["groups"]] == ["G-ALL"]
    assert boot["groups"][0]["accounts"] == ["AC-1"]

    # Direct access to unassigned AC-2 is 403 Forbidden
    assert client.get("/api/overview/financial-summary?account_id=AC-2", headers=v1_hdr1).status_code == 403
    assert client.get("/api/account-pnl/AC-2", headers=v1_hdr1).status_code == 403

    # 5. viewer1 is strictly prohibited (403) from making ANY edit
    assert client.put("/api/traders/TR-1", headers=v1_hdr1, json={"name": "Hacked"}).status_code == 403
    assert client.put("/api/accounts/AC-1", headers=v1_hdr1, json={"name": "Hacked"}).status_code == 403
    assert client.put("/api/rules/TR-1", headers=v1_hdr1, json={"rules": {}}).status_code == 403
    assert client.post("/api/trades/import", headers=v1_hdr1, json=[]).status_code == 403
    assert client.put("/api/groups/G-ALL", headers=v1_hdr1, json={"name": "Hacked", "accounts": []}).status_code == 403
    assert client.put("/api/settings", headers=v1_hdr1, json={}).status_code == 403
    assert client.put("/api/users/viewer1", headers=v1_hdr1, json={"role": "admin"}).status_code == 403
    assert client.post("/api/sync", headers=v1_hdr1).status_code == 403
    assert client.post("/api/accounts/AC-1/zebu-config", headers=v1_hdr1, json={}).status_code == 403

    # 6. viewer_empty fails closed (sees 0 accounts, 0 traders, 0 trades — NEVER all accounts)
    ve_tok = client.post("/api/login", json={"user": "viewer_empty", "password": "v2password"}).json()["token"]
    ve_boot = client.get("/api/bootstrap", headers={"Authorization": f"Bearer {ve_tok}"}).json()
    assert ve_boot["accounts"] == []
    assert ve_boot["traders"] == []
    assert ve_boot["trades"] == []
    assert ve_boot["groups"] == []

    # 7. Admin changes viewer1's password -> BOTH device sessions are IMMEDIATELY logged out (401)
    r_pw = client.put("/api/users/viewer1", headers=admin_hdr, json={
        "password": "v1newpassword",
        "role": "viewer",
        "assigned_accounts": ["AC-1"],
    })
    assert r_pw.status_code == 200
    assert r_pw.json()["sessions_revoked"] >= 2

    assert client.get("/api/me", headers=v1_hdr1).status_code == 401
    assert client.get("/api/me", headers=v1_hdr2).status_code == 401
    assert client.get("/api/accounts", headers=v1_hdr1).status_code == 401

    # Old password fails, new password succeeds
    assert client.post("/api/login", json={"user": "viewer1", "password": "v1password"}).status_code == 401
    assert client.post("/api/login", json={"user": "viewer1", "password": "v1newpassword"}).status_code == 200
