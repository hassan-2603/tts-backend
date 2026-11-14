"""Automated tests for Authentication, Authorization, Account-Level Isolation, and Security."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient
from app.database import set_engine_url, init_models
from app.security import hash_pw, check_pw, encrypt_token, decrypt_token
from app.store import Store
from app.main import app

test_db = tempfile.mktemp(suffix=".db")
set_engine_url(f"sqlite+aiosqlite:///{test_db}")


@pytest.fixture(scope="module", autouse=True)
def setup_test_db():
    import asyncio
    asyncio.run(init_models())
    store = Store()
    # Create test users
    store.put("users", "admin_test", {"user": "admin_test", "role": "admin", "pw": hash_pw("admin123")})
    store.put("users", "trader_user", {"user": "trader_user", "role": "trader", "pw": hash_pw("trader123"), "trader_id": "TR-TEST-1"})
    store.put("users", "viewer_user", {"user": "viewer_user", "role": "viewer", "pw": hash_pw("viewer123"), "assigned_accounts": ["AC-TEST-1"]})

    # Create traders and accounts
    store.put("traders", "TR-TEST-1", {"id": "TR-TEST-1", "name": "Trader One", "status": "Testing"})
    store.put("traders", "TR-TEST-2", {"id": "TR-TEST-2", "name": "Trader Two", "status": "Testing"})
    store.put("accounts", "AC-TEST-1", {"id": "AC-TEST-1", "name": "Account 1", "trader": "TR-TEST-1", "broker": "Zebu", "zebu_uid": "U1"})
    store.put("accounts", "AC-TEST-2", {"id": "AC-TEST-2", "name": "Account 2", "trader": "TR-TEST-2", "broker": "Zebu", "zebu_uid": "U2"})

    # Create trades
    store.put("trades", "TRD-1", {"id": "TRD-1", "trader": "TR-TEST-1", "account": "AC-TEST-1", "date": "2026-11-14", "time": "10:00", "symbol": "BANKNIFTY", "tsym": "BANKNIFTY24NOV51700CE", "strike": 51700, "opt": "CE", "side": "BUY", "lots": 1, "lotSize": 30, "qty": 30, "entry": 300, "exit": 350, "open": False})
    store.put("trades", "TRD-2", {"id": "TRD-2", "trader": "TR-TEST-2", "account": "AC-TEST-2", "date": "2026-11-14", "time": "10:30", "symbol": "NIFTY", "tsym": "NIFTY24NOV24000CE", "strike": 24000, "opt": "CE", "side": "BUY", "lots": 1, "lotSize": 25, "qty": 25, "entry": 150, "exit": 180, "open": False})


def test_password_hashing():
    pw = "SuperSecret123!"
    hashed = hash_pw(pw)
    assert "$" in hashed
    assert check_pw(pw, hashed) is True
    assert check_pw("wrong_password", hashed) is False


def test_token_encryption_at_rest():
    secret_token = "zebu_access_token_xyz_987654321"
    encrypted = encrypt_token(secret_token)
    assert encrypted != secret_token
    assert len(encrypted) > 20

    decrypted = decrypt_token(encrypted)
    assert decrypted == secret_token


def test_account_level_authorization():
    client = TestClient(app)

    # 1. Admin logs in -> Sees all accounts and trades
    resp = client.post("/api/login", json={"user": "admin_test", "password": "admin123"})
    assert resp.status_code == 200
    admin_token = resp.json()["token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    accs = client.get("/api/accounts", headers=admin_headers).json()
    assert len(accs) >= 2

    trades = client.get("/api/trades", headers=admin_headers).json()
    assert len(trades) >= 2

    # 2. Trader logs in -> Sees ONLY their accounts and trades
    resp = client.post("/api/login", json={"user": "trader_user", "password": "trader123"})
    assert resp.status_code == 200
    trader_token = resp.json()["token"]
    trader_headers = {"Authorization": f"Bearer {trader_token}"}

    accs = client.get("/api/accounts", headers=trader_headers).json()
    assert len(accs) == 1
    assert accs[0]["id"] == "AC-TEST-1"

    trades = client.get("/api/trades", headers=trader_headers).json()
    assert len(trades) == 1
    assert trades[0]["account"] == "AC-TEST-1"

    # 3. Viewer logs in -> Sees ONLY assigned accounts and cannot modify
    resp = client.post("/api/login", json={"user": "viewer_user", "password": "viewer123"})
    assert resp.status_code == 200
    viewer_token = resp.json()["token"]
    viewer_headers = {"Authorization": f"Bearer {viewer_token}"}

    accs = client.get("/api/accounts", headers=viewer_headers).json()
    assert len(accs) == 1
    assert accs[0]["id"] == "AC-TEST-1"

    # Write attempt must be 403 Forbidden
    put_resp = client.put("/api/traders/TR-TEST-99", headers=viewer_headers, json={"name": "Hacked"})
    assert put_resp.status_code == 403


def test_security_headers():
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert resp.headers.get("X-XSS-Protection") == "1; mode=block"
