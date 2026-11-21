"""Comprehensive Phase 12 tests for Resend Email, Meta WhatsApp Cloud API, 6-Sheet Excel Report,
PostgreSQL Transactional Outbox, Deduplication, Retries, Recovery, and Webhook Security.
"""
import asyncio
import base64
import hashlib
import hmac
import io
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pytest
from fastapi.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.excel_report import generate_daily_excel_bytes
from app.notifications import (
    NotificationEngine,
    normalize_phone_e164,
    parse_whatsapp_recipients,
    parse_email_recipients,
    verify_whatsapp_signature,
)


def test_phone_normalization_multiple_recipients_and_opt_out():
    assert normalize_phone_e164("9876543210") == "919876543210"
    assert normalize_phone_e164("+91 98765-43210") == "919876543210"
    assert normalize_phone_e164("+1 (415) 555-2671") == "14155552671"

    cfg = {
        "whatsapp": "9876543210, +91 9811122233",
        "whatsappNumbers": ["+919876543210", "+919900011122"],
        "optedOutWhatsApp": ["9811122233"],
    }
    recips = parse_whatsapp_recipients(cfg)
    assert recips == ["919876543210", "919900011122"]  # deduplicated + opted-out excluded
    assert parse_email_recipients({"email": "Client@Gmail.com, second@example.com"}) == ["client@gmail.com", "second@example.com"]


def test_excel_report_six_sheets_structure_and_accuracy():
    report_data = {
        "trading_date": "2026-11-22",
        "report_status": "PROVISIONAL",
        "generated_ist": "22 Nov 2026, 15:36:00 IST",
        "sync_note": "EOD finalization not yet completed for this trading date",
        "summary": {
            "realized_pnl": 12500.50,
            "unrealized_mtm": -1500.00,
            "overall_pnl": 11000.50,
            "available_funds": 425000.00,
            "utilized_funds": 125000.00,
            "total_trades": 2,
            "active_traders": 1,
            "high_flags": 1,
            "medium_flags": 0,
            "low_flags": 0,
            "total_flags": 1,
            "traders_with_flags": 1,
            "rule_follow_pct": 50.0,
        },
        "trader_performance": [
            {
                "trader_name": "Trader Alpha",
                "trader_id": "TR-1",
                "broker_account": "AC-1",
                "realized_pnl": 12500.50,
                "unrealized_mtm": -1500.00,
                "total_pnl": 11000.50,
                "trade_count": 2,
                "rule_follow_pct": 50.0,
            }
        ],
        "account_financials": [
            {
                "account_id": "AC-1",
                "account_name": "Zebu Main",
                "zebu_uid": "47000S23",
                "available_funds": 425000.00,
                "utilized_funds": 125000.00,
                "realized_pnl": 12500.50,
                "unrealized_mtm": -1500.00,
                "status": "LIVE",
            }
        ],
        "trade_details": [
            {
                "date": "2026-11-22", "time": "10:15", "exitDate": "2026-11-22", "exitTime": "10:45",
                "trader": "TR-1", "trader_name": "Trader Alpha", "account": "AC-1",
                "instrument_name": "NIFTY 24500 CE", "expiry": "2026-11-26",
                "side": "BUY", "lots": 2, "qty": 150, "entry": 100.0, "exit": 183.34,
                "pnl": 12500.50, "product": "MIS", "open": False,
            },
            {
                "date": "2026-11-22", "time": "14:50", "exitDate": None, "exitTime": "OPEN",
                "trader": "TR-1", "trader_name": "Trader Alpha", "account": "AC-1",
                "instrument_name": "RELIANCE", "expiry": "",
                "side": "BUY", "lots": 1, "qty": 10, "entry": 2500.0, "exit": 2350.0,
                "pnl": -1500.00, "product": "NRML", "open": True,
            },
        ],
        "red_flags": [
            {
                "trader": "TR-1", "trader_name": "Trader Alpha", "account": "AC-1",
                "rule": "Intraday only", "severity": "High", "instrument": "RELIANCE",
                "detection_time": "2026-11-22 14:50 IST", "observed_value": "NRML",
                "allowed_value": "MIS", "status": "ACTIVE",
            }
        ],
    }

    fname, xlsx_bytes = generate_daily_excel_bytes(report_data)
    assert fname == "TTS_Daily_Trading_Report_2026-11-22.xlsx"
    assert len(xlsx_bytes) > 2000

    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))
    assert wb.sheetnames == [
        "Executive Summary",
        "Trader Performance",
        "Account Financial Summary",
        "Trade Details",
        "Red Flag Details",
        "Risk Analysis",
    ]
    ws1 = wb["Executive Summary"]
    assert ws1["B6"].value == "PROVISIONAL"
    assert ws1["B7"].value == 12500.50
    assert ws1["B8"].value == -1500.00
    assert ws1["B9"].value == 11000.50
    assert ws1["B10"].value == 425000.00

    ws4 = wb["Trade Details"]
    assert ws4["G5"].value == "NIFTY 24500 CE"
    assert ws4["H5"].value == "2026-11-26"
    assert ws4["H6"].value == "N/A"  # equity without expiry

    ws5 = wb["Red Flag Details"]
    assert ws5["C5"].value == "Intraday only"
    assert ws5["D5"].value == "High"


@pytest.fixture
def notif_app(tmp_path, monkeypatch):
    db_file = tmp_path / "test_notif.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_file.as_posix()}")
    monkeypatch.setenv("RUN_WORKER_IN_PROCESS", "false")
    monkeypatch.setenv("RUN_NOTIFICATION_WORKER_IN_PROCESS", "false")

    import app.database as db_mod
    import app.store as store_mod
    import app.main as main_mod
    import app.notifications as notif_mod
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from app.database import Base

    engine_test = create_async_engine(f"sqlite+aiosqlite:///{db_file.as_posix()}", future=True)
    session_factory = async_sessionmaker(bind=engine_test, class_=AsyncSession, expire_on_commit=False)

    monkeypatch.setattr(db_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(store_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(main_mod, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(notif_mod, "AsyncSessionLocal", session_factory)

    # Configure Resend + Meta WhatsApp test credentials
    monkeypatch.setattr(main_mod.CFG, "RESEND_API_KEY", "re_test_key")
    monkeypatch.setattr(main_mod.CFG, "RESEND_FROM_EMAIL", "alerts@tts.example.com")
    monkeypatch.setattr(main_mod.CFG, "WHATSAPP_ACCESS_TOKEN", "wa_test_tok")
    monkeypatch.setattr(main_mod.CFG, "WHATSAPP_PHONE_NUMBER_ID", "100200300")
    monkeypatch.setattr(main_mod.CFG, "WHATSAPP_APP_SECRET", "meta_secret_123")
    monkeypatch.setattr(main_mod.CFG, "WHATSAPP_VERIFY_TOKEN", "verify_tok_999")
    monkeypatch.setattr(main_mod.CFG, "RUN_WORKER_IN_PROCESS", False)
    monkeypatch.setattr(main_mod.CFG, "RUN_NOTIFICATION_WORKER_IN_PROCESS", False)

    async def _init():
        async with engine_test.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await main_mod.DB.put_user_async("admin", {
            "user": "admin", "role": "admin", "pw": main_mod.hash_pw("adminpass"),
        })
        await main_mod.DB._put_setting("main", {
            **main_mod.DEFAULT_SETTINGS,
            "alerts": {
                "telegram": "",
                "whatsapp": "+919876543210, +919811122233",
                "email": "client@gmail.com",
                "onHigh": True,
                "dailySummary": True,
                "summaryTime": "15:35",
                "emailEnabled": True,
                "whatsappEnabled": True,
            },
        })
        await main_mod.DB._put_trader("TR-1", {"id": "TR-1", "name": "Trader One", "status": "Live"})
        await main_mod.DB.put_account_async("AC-1", {"id": "AC-1", "name": "Zebu One", "broker": "Zebu", "trader": "TR-1", "zebu_uid": "47000S23"})

    asyncio.run(_init())
    yield main_mod
    asyncio.run(engine_test.dispose())


def test_end_to_end_high_flag_daily_report_isolation_recovery_and_webhook(notif_app, monkeypatch):
    main_mod = notif_app
    client = TestClient(main_mod.app)
    r_adm = client.post("/api/login", json={"user": "admin", "password": "adminpass"})
    admin_hdr = {"Authorization": f"Bearer {r_adm.json()['token']}"}

    sent_emails = []
    sent_whatsapps = []
    fail_wa = {"flag": False}
    fail_email = {"flag": False}

    async def fake_resend(*, to_email, subject, html_body, idempotency_key, attachments=None):
        if fail_email["flag"]:
            raise RuntimeError("Simulated Resend 503 outage")
        sent_emails.append({"to": to_email, "subject": subject, "html": html_body, "idem": idempotency_key, "attachments": attachments})
        return f"re_msg_{len(sent_emails)}"

    async def fake_wa(*, to_phone, template_name, body_params, lang_code=None):
        if fail_wa["flag"]:
            raise RuntimeError("Simulated Meta WhatsApp 500 error")
        sent_whatsapps.append({"to": to_phone, "template": template_name, "params": body_params})
        return f"wamid.TEST_{len(sent_whatsapps)}"

    monkeypatch.setattr(main_mod.NOTIF, "send_resend_email", fake_resend)
    monkeypatch.setattr(main_mod.NOTIF, "send_whatsapp_template", fake_wa)

    async def _run():
        alerts_cfg = (await main_mod.DB._get_setting("main"))["alerts"]

        # 1. Medium flag must NOT trigger high-only alert
        q_med = await main_mod.NOTIF.enqueue_high_flag_alert(
            flag_key="AC-1|T-MED|Max 4 trades",
            trading_date="2026-11-22",
            alerts_cfg=alerts_cfg,
            payload={"severity": "Medium", "rule_violated": "Max 4 trades"},
        )
        assert q_med == {"email": [], "whatsapp": []}

        # 2. High flag triggers 1 email + 2 individual WhatsApp jobs
        q_hi = await main_mod.NOTIF.enqueue_high_flag_alert(
            flag_key="AC-1|T-HI|Intraday only",
            trading_date="2026-11-22",
            alerts_cfg=alerts_cfg,
            payload={
                "trader_name": "Trader One", "trader_id": "TR-1", "account_id": "AC-1",
                "instrument": "BANKNIFTY 52000 CE", "severity": "High", "rule_violated": "Intraday only",
                "detection_time": "2026-11-22 10:15 IST", "observed_value": "NRML", "allowed_value": "MIS",
                "explanation": "Carried position overnight",
            },
        )
        assert len(q_hi["email"]) == 1
        assert len(q_hi["whatsapp"]) == 2

        # 3. Duplicate sync cycle for same flag does NOT create duplicate jobs
        q_dup = await main_mod.NOTIF.enqueue_high_flag_alert(
            flag_key="AC-1|T-HI|Intraday only",
            trading_date="2026-11-22",
            alerts_cfg=alerts_cfg,
            payload={"severity": "High", "rule_violated": "Intraday only"},
        )
        assert q_dup == {"email": [], "whatsapp": []}

        # 4. Channel isolation: fail WhatsApp -> Email STILL succeeds!
        fail_wa["flag"] = True
        await main_mod.NOTIF.process_pending_jobs_once(batch_size=10)
        assert len(sent_emails) == 1
        assert sent_emails[0]["to"] == "client@gmail.com"
        assert len(sent_whatsapps) == 0

        # Restore WhatsApp and simulate retry timer elapsed -> both WhatsApp recipients succeed individually
        fail_wa["flag"] = False
        async with main_mod.AsyncSessionLocal() as s:
            from sqlalchemy import select
            from app.models import NotificationJob
            rows = (await s.execute(select(NotificationJob).where(NotificationJob.status == "RETRYING"))).scalars().all()
            assert len(rows) == 2
            for row in rows:
                row.next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=5)
            await s.commit()

        await main_mod.NOTIF.process_pending_jobs_once(batch_size=10)
        assert len(sent_whatsapps) == 2
        assert {w["to"] for w in sent_whatsapps} == {"919876543210", "919811122233"}

        # 5. Daily report: generates Excel attachment + prevents duplicate dispatches for same date
        await main_mod.DB._put_trade("T-D1", {
            "id": "T-D1", "account": "AC-1", "trader": "TR-1", "date": "2026-11-22", "time": "10:00",
            "exitDate": "2026-11-22", "exitTime": "10:30", "symbol": "NIFTY", "strike": 24500, "opt": "CE",
            "expiry": "2026-11-26", "side": "BUY", "lots": 1, "lotSize": 75, "entry": 100.0, "exit": 120.0,
            "pnl": 1500.0, "product": "MIS", "open": False,
        })
        rep_data = await main_mod.build_daily_report_data_async("2026-11-22")
        assert rep_data["report_status"] == "PROVISIONAL"  # not finalized yet
        d1 = await main_mod.NOTIF.enqueue_daily_report(trading_date="2026-11-22", alerts_cfg=alerts_cfg, report_data=rep_data, force=False)
        assert d1["skipped"] is False
        assert len(d1["jobs"]["email"]) == 1
        assert len(d1["jobs"]["whatsapp"]) == 2

        # Second call without force is deduplicated
        d2 = await main_mod.NOTIF.enqueue_daily_report(trading_date="2026-11-22", alerts_cfg=alerts_cfg, report_data=rep_data, force=False)
        assert d2["skipped"] is True

        # Process daily report jobs -> verify Resend received valid 6-sheet .xlsx attachment
        await main_mod.NOTIF.process_pending_jobs_once(batch_size=10)
        daily_email = sent_emails[-1]
        assert "TTS Daily Trading Report" in daily_email["subject"]
        assert len(daily_email["attachments"]) == 1
        att = daily_email["attachments"][0]
        assert att["filename"] == "TTS_Daily_Trading_Report_2026-11-22.xlsx"
        wb = openpyxl.load_workbook(io.BytesIO(base64.b64decode(att["content"])))
        assert len(wb.sheetnames) == 6

        # 6. Simulate worker crash (abandoned PROCESSING job older than stale lock) -> recovers cleanly
        async with main_mod.AsyncSessionLocal() as s:
            from app.models import NotificationJob
            now_u = datetime.now(timezone.utc)
            s.add(NotificationJob(
                id="NJOB-CRASHED",
                event_key="CRASH-TEST-1",
                client_id="main",
                trading_date="2026-11-22",
                channel="email",
                notification_type="test",
                recipient="client@gmail.com",
                status="PROCESSING",
                attempt_count=0,
                max_attempts=3,
                next_attempt_at=now_u - timedelta(minutes=10),
                locked_at=now_u - timedelta(minutes=10),
                locked_by="dead-worker",
                payload_json={"subject": "Recovered Job", "html": "<p>OK</p>"},
                created_at=now_u - timedelta(minutes=10),
                updated_at=now_u - timedelta(minutes=10),
            ))
            await s.commit()

        rec_n = await main_mod.NOTIF.process_pending_jobs_once(batch_size=10)
        assert rec_n == 1
        assert sent_emails[-1]["subject"] == "Recovered Job"

    asyncio.run(_run())

    # 7. Meta WhatsApp Webhook: verification challenge + HMAC-SHA256 signature validation + DELIVERED status update
    r_ver = client.get("/api/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=verify_tok_999&hub.challenge=CHAL123")
    assert r_ver.status_code == 200
    assert r_ver.text == "CHAL123"
    assert client.get("/api/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=WRONG&hub.challenge=X").status_code == 403

    wh_payload = json.dumps({
        "entry": [{"changes": [{"value": {"statuses": [{"id": "wamid.TEST_1", "status": "delivered"}]}}]}]
    }).encode("utf-8")
    good_sig = "sha256=" + hmac.new(b"meta_secret_123", wh_payload, hashlib.sha256).hexdigest()

    assert client.post("/api/webhooks/whatsapp", content=wh_payload, headers={"X-Hub-Signature-256": "sha256=bad"}).status_code == 403
    r_wh = client.post("/api/webhooks/whatsapp", content=wh_payload, headers={"X-Hub-Signature-256": good_sig})
    assert r_wh.status_code == 200
    assert r_wh.json()["updated"] == 1

    # 8. Admin delivery history endpoint reflects DELIVERED status and never leaks API keys
    r_hist = client.get("/api/notifications/history", headers=admin_hdr)
    assert r_hist.status_code == 200
    assert "re_test_key" not in r_hist.text
    assert "wa_test_tok" not in r_hist.text
    hist_items = r_hist.json()["items"]
    assert any(i["provider_message_id"] == "wamid.TEST_1" and i["status"] == "DELIVERED" for i in hist_items)
