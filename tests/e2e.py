"""End-to-end test: modernized server + mock Zebu (REST & WebSocket).

Run from tts-server folder:
python tests/e2e.py
"""
import os
import subprocess
import sys
import tempfile
import time
import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = tempfile.mktemp(suffix=".db")
db_url = f"sqlite+aiosqlite:///{db_path}"

env = {
    **os.environ,
    "ZEBU_BASE_URL": "http://127.0.0.1:9001",
    "ZEBU_WS_URL": "ws://127.0.0.1:9001/NorenWSTP/",
    "ZEBU_CLIENT_ID": "TESTCID",
    "ZEBU_SECRET_KEY": "TESTSECRET",
    "PUBLIC_URL": "http://127.0.0.1:8000",
    "DATABASE_URL": db_url,
    "DB_PATH": db_path,
    "DISABLE_SCHEDULER": "1",
    "ADMIN_USER": "admin",
    "ADMIN_PASSWORD": "pass123",
    "TELEGRAM_BOT_TOKEN": "TT",
    "TELEGRAM_API": "http://127.0.0.1:9001",
}

python_bin = sys.executable

procs = [
    subprocess.Popen(
        [python_bin, "-m", "uvicorn", "tests.mock_zebu:app", "--port", "9001", "--host", "127.0.0.1"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ),
    subprocess.Popen(
        [python_bin, "-m", "uvicorn", "app.main:app", "--port", "8000", "--host", "127.0.0.1"],
        cwd=ROOT,
        env=env,
        stdout=open(os.path.join(tempfile.gettempdir(), "server.log"), "w"),
        stderr=subprocess.STDOUT,
    ),
]

try:
    print("Waiting for mock Zebu and TTS server to initialize...")
    for _ in range(50):
        try:
            httpx.get("http://127.0.0.1:8000/api/health", timeout=1.0)
            httpx.get("http://127.0.0.1:9001/_calls", timeout=1.0)
            break
        except Exception:
            time.sleep(0.3)

    B = "http://127.0.0.1:8000/api"

    # Health check
    h_resp = httpx.get("http://127.0.0.1:8000/health").json()
    assert h_resp.get("status") == "ok", f"Health failed: {h_resp}"

    # Ready check
    r_resp = httpx.get("http://127.0.0.1:8000/ready").json()
    assert r_resp.get("status") == "ready", f"Ready check failed: {r_resp}"

    # Login
    tok = httpx.post(B + "/login", json={"user": "admin", "password": "pass123"}).json()["token"]
    H = {"Authorization": "Bearer " + tok}

    assert httpx.post(B + "/login", json={"user": "admin", "password": "wrong"}).status_code == 401

    # Add Trader
    httpx.put(
        B + "/traders/TR-001",
        headers=H,
        json={
            "name": "Trader A",
            "ra": "RA A",
            "style": "Manual",
            "holding": "Intraday",
            "segment": "Bank Nifty options",
            "testCapital": 300000,
            "status": "Testing",
            "start": "2026-11-01",
        },
    ).raise_for_status()

    # Add Account
    httpx.put(
        B + "/accounts/AC-01",
        headers=H,
        json={
            "name": "Account 1",
            "broker": "Zebu",
            "trader": "TR-001",
            "zebu_uid": "zp1001",
        },
    ).raise_for_status()

    # Settings
    s = httpx.get(B + "/settings", headers=H).json()
    s["alerts"]["telegram"] = "999"
    httpx.put(B + "/settings", headers=H, json=s).raise_for_status()

    # OAuth Account Link
    url = httpx.post(B + "/accounts/AC-01/link", headers=H).json()["url"]
    with httpx.Client(follow_redirects=True) as c:
        page = c.get(url).text
    assert "linked to Zebu" in page, f"Linking response: {page}"

    acc = httpx.get(B + "/accounts", headers=H).json()[0]
    print(f"Account status: link={acc['link']} cliname={acc['cliname']} ws_status={acc.get('ws_status')}")

    # Synchronize
    res = httpx.post(B + "/sync", headers=H, timeout=60).json()
    print("Sync result:", res)

    # Check trades
    trades = sorted(httpx.get(B + "/trades", headers=H).json(), key=lambda t: t["time"])
    for t in trades:
        print(
            f"  {t['time']} {t['tsym']} {t['side']} lots={t['lots']} entry={t['entry']} exit={t['exit']} "
            f"open={t['open']} spot={t['spot']} prod={t['product']} src={t['source']} exp={t['expiryType']} chg={t['charges']}"
        )

    assert len(trades) == 3 and all(t["spot"] == 51800 for t in trades)
    assert trades[0]["source"] == "Algo" and trades[1]["source"] == "Manual" and trades[2]["open"] and trades[2]["lots"] == 1

    # Verify broker endpoints
    calls = httpx.get("http://127.0.0.1:9001/_calls").json()
    print("Zebu endpoints called:", sorted(set(calls["calls"])))
    assert not {"PlaceOrder", "ModifyOrder", "CancelOrder"} & set(calls["calls"]), "Order placement endpoints must never be called!"

    # Telegram alerts
    print("Telegram alerts received:")
    for m in calls["telegram"]:
        print("  ---\n  " + m["text"].replace("\n", "\n  "))

    assert any("Strike" in m["text"] for m in calls["telegram"])
    assert any("Intraday only" in m["text"] for m in calls["telegram"])

    # Second sync must deduplicate and NOT send duplicate alerts
    n = len(calls["telegram"])
    httpx.post(B + "/sync", headers=H, timeout=60)
    assert len(httpx.get("http://127.0.0.1:9001/_calls").json()["telegram"]) == n, "Duplicate alerts were sent!"

    # Viewer role restrictions
    httpx.put(B + "/users/viewer1", headers=H, json={"password": "v", "role": "viewer"}).raise_for_status()
    vt = httpx.post(B + "/login", json={"user": "viewer1", "password": "v"}).json()["token"]
    assert httpx.put(B + "/traders/TR-009", headers={"Authorization": "Bearer " + vt}, json={}).status_code == 403

    # Metrics endpoint
    metrics = httpx.get(B + "/metrics", headers=H).json()
    print("System metrics:", metrics["uptime_seconds"], "s uptime, memory:", metrics["memory_mb"], "MB")

    print("\n>>> ALL BACKEND & E2E CHECKS PASSED SUCCESSFULLY <<<\n")

finally:
    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=2)
        except Exception:
            p.kill()
