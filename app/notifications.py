"""Centralized Notification Engine: PostgreSQL Transactional Outbox, Resend Email, and Meta WhatsApp Cloud API.

Features:
- PostgreSQL-backed outbox (`notification_jobs`) with `FOR UPDATE SKIP LOCKED` multi-worker safety
- Independent Email (Resend) and WhatsApp (Meta Cloud API) deliveries — failure in one NEVER blocks the other
- Strict deduplication via unique `event_key` and `DailyReportDispatch`
- Exponential backoff retries + abandoned `PROCESSING` job recovery after worker restarts
- HTML email templates + private 6-sheet Excel report attachment (`TTS_Daily_Trading_Report_YYYY-MM-DD.xlsx`)
- Approved Meta WhatsApp template messaging, E.164 normalization, opt-out filtering, and HMAC-SHA256 webhook verification
- Safe disabled fallback when Meta WhatsApp or Resend credentials are not yet configured
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import html as html_lib
import logging
import re
import socket
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import and_, or_, select

from app.config import CFG
from app.database import AsyncSessionLocal, async_engine
from app.excel_report import generate_daily_excel_bytes
from app.models import DailyReportDispatch, NotificationJob
from app.scheduler import IST, now_ist

log = logging.getLogger("tts.notifications")

_BACKOFF_SECONDS = (30, 120, 300, 900, 1800)


def normalize_phone_e164(raw: str, default_country: Optional[str] = None) -> Optional[str]:
    """Normalize phone number into Meta Cloud API international digits-only E.164 format (no leading '+')."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    has_plus = s.startswith("+")
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    cc = re.sub(r"\D+", "", str(default_country or CFG.WHATSAPP_DEFAULT_COUNTRY_CODE or "91")) or "91"
    if not has_plus and len(digits) == 10:
        digits = cc + digits
    elif digits.startswith("0") and len(digits) == 11:
        digits = cc + digits[1:]
    if len(digits) < 10 or len(digits) > 15:
        return None
    return digits


def parse_whatsapp_recipients(alerts_cfg: Optional[dict]) -> List[str]:
    """Extract and deduplicate all configured WhatsApp numbers while preserving legacy `whatsapp` field."""
    cfg = alerts_cfg or {}
    raw_items: List[str] = []
    primary = cfg.get("whatsapp")
    if isinstance(primary, str) and primary.strip():
        raw_items.extend(re.split(r"[,;\n]+", primary))
    extra = cfg.get("whatsappNumbers") or cfg.get("whatsapp_numbers") or []
    if isinstance(extra, str):
        raw_items.extend(re.split(r"[,;\n]+", extra))
    elif isinstance(extra, list):
        raw_items.extend(str(x) for x in extra if x)

    opted_out_raw = cfg.get("optedOutWhatsApp") or cfg.get("opted_out_whatsapp") or []
    opted_out = set()
    for item in (opted_out_raw if isinstance(opted_out_raw, list) else re.split(r"[,;\n]+", str(opted_out_raw))):
        norm = normalize_phone_e164(item)
        if norm:
            opted_out.add(norm)

    out: List[str] = []
    seen = set()
    for item in raw_items:
        norm = normalize_phone_e164(item)
        if norm and norm not in seen and norm not in opted_out:
            seen.add(norm)
            out.append(norm)
    return out


def parse_email_recipients(alerts_cfg: Optional[dict]) -> List[str]:
    """Extract and validate configured email recipient(s) from existing `alerts.email` setting."""
    cfg = alerts_cfg or {}
    raw = str(cfg.get("email") or "").strip()
    if not raw:
        return []
    out: List[str] = []
    seen = set()
    for part in re.split(r"[,;\n]+", raw):
        em = part.strip().lower()
        if em and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", em) and em not in seen:
            seen.add(em)
            out.append(em)
    return out


def _fmt_inr(val: Optional[float]) -> str:
    if val is None:
        return "Unavailable"
    sign = "-" if val < 0 else ("+" if val > 0 else "")
    return f"{sign}₹{abs(val):,.2f}"


def build_high_flag_email_html(payload: Dict[str, Any]) -> Tuple[str, str]:
    """Build (subject, html_body) for a high-severity red-flag alert."""
    trader_name = str(payload.get("trader_name") or payload.get("trader_id") or "Unknown")
    trader_id = str(payload.get("trader_id") or "")
    account_id = str(payload.get("account_id") or "—")
    instrument = str(payload.get("instrument") or "—")
    severity = str(payload.get("severity") or "High")
    rule = str(payload.get("rule_violated") or "—")
    ts = str(payload.get("detection_time") or "—")
    observed = str(payload.get("observed_value") or "—")
    allowed = str(payload.get("allowed_value") or "—")
    explanation = str(payload.get("explanation") or observed)
    link = str(payload.get("dashboard_url") or f"{CFG.FRONTEND_URL.rstrip('/')}/#flags")

    subject = f"[TTS High Red Flag] {trader_name} · {rule}"
    e = html_lib.escape
    html_body = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:620px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden">
      <div style="background:#0f172a;color:#ffffff;padding:18px 22px">
        <div style="font-size:12px;letter-spacing:0.06em;text-transform:uppercase;color:#f87171;font-weight:700">High-Severity Red Flag Alert</div>
        <div style="font-size:19px;font-weight:700;margin-top:4px">{e(rule)}</div>
      </div>
      <div style="padding:20px 22px;color:#111827;font-size:14px;line-height:1.55">
        <p style="margin:0 0 14px">{e(explanation)}</p>
        <table style="width:100%;border-collapse:collapse;font-size:13.5px">
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Trader</td><td style="padding:7px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(trader_name)} ({e(trader_id)})</td></tr>
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Broker Account</td><td style="padding:7px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(account_id)}</td></tr>
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Instrument</td><td style="padding:7px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(instrument)}</td></tr>
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Severity</td><td style="padding:7px 0;font-weight:700;color:#dc2626;text-align:right;border-bottom:1px solid #f1f5f9">{e(severity)}</td></tr>
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Detected At</td><td style="padding:7px 0;text-align:right;border-bottom:1px solid #f1f5f9">{e(ts)}</td></tr>
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Observed Value</td><td style="padding:7px 0;font-weight:700;color:#dc2626;text-align:right;border-bottom:1px solid #f1f5f9">{e(observed)}</td></tr>
          <tr><td style="padding:7px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Allowed Value</td><td style="padding:7px 0;text-align:right;border-bottom:1px solid #f1f5f9">{e(allowed)}</td></tr>
        </table>
        <div style="margin-top:18px">
          <a href="{e(link)}" style="display:inline-block;background:#0f172a;color:#ffffff;text-decoration:none;padding:10px 16px;border-radius:8px;font-weight:600;font-size:13px">Open Red Flags in TTS</a>
        </div>
      </div>
    </div>
    """
    return subject, html_body


def build_daily_summary_email_html(report_data: Dict[str, Any]) -> Tuple[str, str]:
    """Build (subject, html_body) for the automated daily consolidated trading report email."""
    t_date = str(report_data.get("trading_date") or "")
    status = str(report_data.get("report_status") or "FINAL").upper()
    summary = report_data.get("summary") or {}
    sync_note = str(report_data.get("sync_note") or "")
    e = html_lib.escape

    rp = summary.get("realized_pnl")
    um = summary.get("unrealized_mtm")
    ov = summary.get("overall_pnl")
    av = summary.get("available_funds")
    ut = summary.get("utilized_funds")
    rf = summary.get("rule_follow_pct")
    rf_str = f"{float(rf):.1f}%" if rf is not None else "N/A"
    badge_bg = "#e7f6ee" if status == "FINAL" else "#fef3c7"
    badge_fg = "#0f9d58" if status == "FINAL" else "#b45309"

    subject = f"TTS Daily Trading Report — {t_date}" + (" (PROVISIONAL)" if status != "FINAL" else "")
    html_body = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden">
      <div style="background:#0f172a;color:#ffffff;padding:18px 22px">
        <div style="font-size:12px;letter-spacing:0.06em;text-transform:uppercase;color:#94a3b8;font-weight:700">Trader Tracking System</div>
        <div style="font-size:20px;font-weight:700;margin-top:4px">Daily Trading Report — {e(t_date)}</div>
        <span style="display:inline-block;margin-top:8px;padding:3px 10px;border-radius:999px;font-size:11px;font-weight:700;background:{badge_bg};color:{badge_fg}">{e(status)}</span>
      </div>
      <div style="padding:20px 22px;color:#111827;font-size:14px">
        {f'<div style="background:#fef3c7;color:#92400e;padding:10px 12px;border-radius:8px;font-size:12.5px;margin-bottom:14px">{e(sync_note)}</div>' if sync_note and status != 'FINAL' else ''}
        <table style="width:100%;border-collapse:collapse;font-size:13.5px">
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Realized P&amp;L</td><td style="padding:8px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(_fmt_inr(rp))}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Unrealized MTM</td><td style="padding:8px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(_fmt_inr(um))}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Overall P&amp;L</td><td style="padding:8px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(_fmt_inr(ov))}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Total Available Funds</td><td style="padding:8px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(_fmt_inr(av))}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Total Utilized Funds</td><td style="padding:8px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{e(_fmt_inr(ut))}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Total Trades</td><td style="padding:8px 0;font-weight:700;text-align:right;border-bottom:1px solid #f1f5f9">{int(summary.get('total_trades') or 0)}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;border-bottom:1px solid #f1f5f9">Red Flags (High / Total)</td><td style="padding:8px 0;font-weight:700;color:#dc2626;text-align:right;border-bottom:1px solid #f1f5f9">{int(summary.get('high_flags') or 0)} High / {int(summary.get('total_flags') or 0)} Total ({int(summary.get('traders_with_flags') or 0)} traders)</td></tr>
          <tr><td style="padding:8px 0;color:#64748b">Rule-Follow Score</td><td style="padding:8px 0;font-weight:700;text-align:right">{e(rf_str)}</td></tr>
        </table>
        <p style="margin:16px 0 0;font-size:12.5px;color:#475569">
          The detailed 6-sheet Excel report (<b>TTS_Daily_Trading_Report_{e(t_date)}.xlsx</b>) is attached directly to this email.
        </p>
      </div>
    </div>
    """
    return subject, html_body


def verify_whatsapp_signature(raw_body: bytes, signature_header: str, app_secret: Optional[str] = None) -> bool:
    """Verify Meta WhatsApp webhook X-Hub-Signature-256 HMAC-SHA256 signature."""
    secret = (app_secret if app_secret is not None else CFG.WHATSAPP_APP_SECRET) or ""
    if not secret or not signature_header:
        return False
    sig = signature_header.strip()
    if sig.startswith("sha256="):
        sig = sig[7:]
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)


class NotificationEngine:
    """PostgreSQL transactional-outbox notification engine for Resend Email & Meta WhatsApp Cloud API."""

    def __init__(self, report_data_builder: Optional[Callable[[str], Coroutine[Any, Any, Dict[str, Any]]]] = None):
        self._http: Optional[httpx.AsyncClient] = None
        self._report_data_builder = report_data_builder
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self.worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"

    def set_report_data_builder(self, builder: Callable[[str], Coroutine[Any, Any, Dict[str, Any]]]):
        self._report_data_builder = builder

    def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=25.0)
        return self._http

    async def aclose(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    def email_configured(self) -> bool:
        return bool(CFG.RESEND_API_KEY and CFG.RESEND_FROM_EMAIL)

    def whatsapp_configured(self) -> bool:
        return bool(CFG.WHATSAPP_ACCESS_TOKEN and CFG.WHATSAPP_PHONE_NUMBER_ID)

    # ---------------- Enqueue Helpers (Idempotent) ----------------
    async def _enqueue_job(
        self,
        *,
        event_key: str,
        channel: str,
        notification_type: str,
        recipient: str,
        payload: Dict[str, Any],
        trading_date: Optional[str] = None,
        client_id: str = "main",
    ) -> Optional[str]:
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as session:
            existing = (await session.execute(
                select(NotificationJob).where(NotificationJob.event_key == event_key)
            )).scalar_one_or_none()
            if existing:
                return None
            job_id = f"NJOB-{uuid.uuid4().hex[:14]}"
            job = NotificationJob(
                id=job_id,
                event_key=event_key,
                client_id=client_id,
                trading_date=trading_date,
                channel=channel,
                notification_type=notification_type,
                recipient=recipient,
                status="PENDING",
                attempt_count=0,
                max_attempts=CFG.NOTIFICATION_MAX_ATTEMPTS,
                next_attempt_at=now,
                payload_json=payload,
                created_at=now,
                updated_at=now,
            )
            session.add(job)
            try:
                await session.commit()
                return job_id
            except Exception:
                await session.rollback()
                return None

    async def enqueue_high_flag_alert(
        self,
        *,
        flag_key: str,
        trading_date: str,
        alerts_cfg: Dict[str, Any],
        payload: Dict[str, Any],
        client_id: str = "main",
    ) -> Dict[str, List[str]]:
        """Queue independent Email & per-recipient WhatsApp jobs for a qualifying High-severity red flag."""
        queued: Dict[str, List[str]] = {"email": [], "whatsapp": []}
        if not alerts_cfg.get("onHigh"):
            return queued
        if str(payload.get("severity") or "") != "High":
            return queued

        email_enabled = alerts_cfg.get("emailEnabled", True) is not False
        wa_enabled = alerts_cfg.get("whatsappEnabled", True) is not False

        if email_enabled:
            for em in parse_email_recipients(alerts_cfg):
                ek = f"FLAG|{flag_key}|email|{em}"
                jid = await self._enqueue_job(
                    event_key=ek,
                    channel="email",
                    notification_type="high_red_flag",
                    recipient=em,
                    payload=payload,
                    trading_date=trading_date,
                    client_id=client_id,
                )
                if jid:
                    queued["email"].append(jid)

        if wa_enabled:
            for phone in parse_whatsapp_recipients(alerts_cfg):
                ek = f"FLAG|{flag_key}|whatsapp|{phone}"
                jid = await self._enqueue_job(
                    event_key=ek,
                    channel="whatsapp",
                    notification_type="high_red_flag",
                    recipient=phone,
                    payload=payload,
                    trading_date=trading_date,
                    client_id=client_id,
                )
                if jid:
                    queued["whatsapp"].append(jid)

        return queued

    async def enqueue_daily_report(
        self,
        *,
        trading_date: str,
        alerts_cfg: Dict[str, Any],
        report_data: Dict[str, Any],
        client_id: str = "main",
        force: bool = False,
    ) -> Dict[str, Any]:
        """Record DailyReportDispatch (1 per client/date unless force=True) and enqueue Email + WhatsApp jobs."""
        dispatch_id = f"{client_id}|{trading_date}"
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as session:
            existing = (await session.execute(
                select(DailyReportDispatch).where(DailyReportDispatch.id == dispatch_id)
            )).scalar_one_or_none()
            if existing and not force:
                return {"skipped": True, "reason": "already_dispatched", "dispatch_id": dispatch_id, "jobs": {"email": [], "whatsapp": []}}
            if existing:
                existing.report_status = str(report_data.get("report_status") or "FINAL")
                existing.summary_json = report_data.get("summary") or {}
                existing.updated_at = now
            else:
                session.add(DailyReportDispatch(
                    id=dispatch_id,
                    client_id=client_id,
                    trading_date=trading_date,
                    report_status=str(report_data.get("report_status") or "FINAL"),
                    summary_json=report_data.get("summary") or {},
                    created_at=now,
                    updated_at=now,
                ))
            await session.commit()

        suffix = f"|manual|{uuid.uuid4().hex[:6]}" if force else ""
        queued: Dict[str, List[str]] = {"email": [], "whatsapp": []}
        email_enabled = alerts_cfg.get("emailEnabled", True) is not False
        wa_enabled = alerts_cfg.get("whatsappEnabled", True) is not False

        if email_enabled:
            for em in parse_email_recipients(alerts_cfg):
                ek = f"DAILY|{client_id}|{trading_date}|email|{em}{suffix}"
                jid = await self._enqueue_job(
                    event_key=ek,
                    channel="email",
                    notification_type="daily_summary",
                    recipient=em,
                    payload={"trading_date": trading_date, "report_data": report_data},
                    trading_date=trading_date,
                    client_id=client_id,
                )
                if jid:
                    queued["email"].append(jid)

        if wa_enabled:
            wa_payload = {
                "trading_date": trading_date,
                "report_status": report_data.get("report_status") or "FINAL",
                "summary": report_data.get("summary") or {},
            }
            for phone in parse_whatsapp_recipients(alerts_cfg):
                ek = f"DAILY|{client_id}|{trading_date}|whatsapp|{phone}{suffix}"
                jid = await self._enqueue_job(
                    event_key=ek,
                    channel="whatsapp",
                    notification_type="daily_summary",
                    recipient=phone,
                    payload=wa_payload,
                    trading_date=trading_date,
                    client_id=client_id,
                )
                if jid:
                    queued["whatsapp"].append(jid)

        return {"skipped": False, "dispatch_id": dispatch_id, "report_status": report_data.get("report_status"), "jobs": queued}

    # ---------------- Provider Senders ----------------
    async def send_resend_email(
        self,
        *,
        to_email: str,
        subject: str,
        html_body: str,
        idempotency_key: str,
        attachments: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """Send transactional email via Resend REST API and return Resend message ID."""
        if not self.email_configured():
            raise RuntimeError("Resend email is not configured (set RESEND_API_KEY and RESEND_FROM_EMAIL).")
        body: Dict[str, Any] = {
            "from": CFG.RESEND_FROM_EMAIL,
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        }
        if attachments:
            body["attachments"] = attachments
        headers = {
            "Authorization": f"Bearer {CFG.RESEND_API_KEY}",
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key[:250],
        }
        resp = await self._client().post("https://api.resend.com/emails", json=body, headers=headers)
        if resp.status_code >= 400:
            raise RuntimeError(f"Resend HTTP {resp.status_code}: {resp.text[:250]}")
        data = resp.json()
        msg_id = str(data.get("id") or "")
        if not msg_id:
            raise RuntimeError("Resend response missing message id")
        return msg_id

    async def send_whatsapp_template(
        self,
        *,
        to_phone: str,
        template_name: str,
        body_params: List[str],
        lang_code: Optional[str] = None,
    ) -> str:
        """Send an approved WhatsApp Cloud API template message to a single normalized recipient."""
        if not self.whatsapp_configured():
            raise RuntimeError("Meta WhatsApp Cloud API is disabled/unconfigured (set WHATSAPP_ACCESS_TOKEN and WHATSAPP_PHONE_NUMBER_ID).")
        url = f"https://graph.facebook.com/{CFG.WHATSAPP_API_VERSION}/{CFG.WHATSAPP_PHONE_NUMBER_ID}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to_phone,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": lang_code or CFG.WHATSAPP_TEMPLATE_LANG or "en_US"},
                "components": [
                    {
                        "type": "body",
                        "parameters": [{"type": "text", "text": str(p)[:1000]} for p in body_params],
                    }
                ],
            },
        }
        headers = {
            "Authorization": f"Bearer {CFG.WHATSAPP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }
        resp = await self._client().post(url, json=payload, headers=headers)
        if resp.status_code >= 400:
            raise RuntimeError(f"Meta WhatsApp HTTP {resp.status_code}: {resp.text[:250]}")
        data = resp.json()
        msgs = data.get("messages") or []
        msg_id = str(msgs[0].get("id") or "") if msgs and isinstance(msgs[0], dict) else ""
        if not msg_id:
            raise RuntimeError("Meta WhatsApp response missing message id")
        return msg_id

    # ---------------- Outbox Claim, Execute, Retry & Recovery ----------------
    async def _execute_job(self, job: NotificationJob) -> str:
        payload = job.payload_json or {}
        if job.channel == "email":
            if job.notification_type == "high_red_flag":
                subject, html_body = build_high_flag_email_html(payload)
                return await self.send_resend_email(
                    to_email=job.recipient,
                    subject=subject,
                    html_body=html_body,
                    idempotency_key=job.event_key,
                )
            elif job.notification_type == "daily_summary":
                report_data = payload.get("report_data")
                if not report_data and self._report_data_builder and job.trading_date:
                    report_data = await self._report_data_builder(job.trading_date)
                if not report_data:
                    raise RuntimeError("Daily report data unavailable")
                subject, html_body = build_daily_summary_email_html(report_data)
                xlsx_name, xlsx_bytes = generate_daily_excel_bytes(report_data)
                attachment = {
                    "filename": xlsx_name,
                    "content": base64.b64encode(xlsx_bytes).decode("ascii"),
                }
                return await self.send_resend_email(
                    to_email=job.recipient,
                    subject=subject,
                    html_body=html_body,
                    idempotency_key=job.event_key,
                    attachments=[attachment],
                )
            else:
                subject = str(payload.get("subject") or "TTS Test Notification")
                html_body = str(payload.get("html") or "<p>Test email from Trader Tracking System (Resend).</p>")
                return await self.send_resend_email(
                    to_email=job.recipient,
                    subject=subject,
                    html_body=html_body,
                    idempotency_key=job.event_key,
                )

        if job.channel == "whatsapp":
            if job.notification_type == "high_red_flag":
                params = [
                    str(payload.get("trader_name") or payload.get("trader_id") or "—"),
                    str(payload.get("account_id") or "—"),
                    str(payload.get("instrument") or "—"),
                    str(payload.get("rule_violated") or "—"),
                    str(payload.get("observed_value") or payload.get("explanation") or "—"),
                    str(payload.get("detection_time") or "—"),
                ]
                return await self.send_whatsapp_template(
                    to_phone=job.recipient,
                    template_name=CFG.WHATSAPP_ALERT_TEMPLATE,
                    body_params=params,
                )
            elif job.notification_type == "daily_summary":
                s = payload.get("summary") or {}
                rf = s.get("rule_follow_pct")
                rf_s = f"{float(rf):.1f}%" if rf is not None else "N/A"
                params = [
                    str(payload.get("trading_date") or job.trading_date or "—"),
                    _fmt_inr(s.get("realized_pnl")),
                    _fmt_inr(s.get("unrealized_mtm")),
                    _fmt_inr(s.get("overall_pnl")),
                    str(int(s.get("total_trades") or 0)),
                    f"{int(s.get('high_flags') or 0)} High / {int(s.get('total_flags') or 0)} Total",
                    str(int(s.get("traders_with_flags") or 0)),
                    rf_s,
                ]
                return await self.send_whatsapp_template(
                    to_phone=job.recipient,
                    template_name=CFG.WHATSAPP_SUMMARY_TEMPLATE,
                    body_params=params,
                )
            else:
                return await self.send_whatsapp_template(
                    to_phone=job.recipient,
                    template_name=CFG.WHATSAPP_ALERT_TEMPLATE,
                    body_params=["Test Trader", "TEST-ACC", "NIFTY", "Test Alert", "Verification OK", now_ist().strftime("%H:%M IST")],
                )

        raise RuntimeError(f"Unsupported notification channel: {job.channel}")

    async def process_pending_jobs_once(self, batch_size: int = 10) -> int:
        """Recover abandoned PROCESSING jobs and atomically claim & dispatch up to `batch_size` due jobs."""
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(seconds=CFG.NOTIFICATION_STALE_LOCK_SECONDS)
        claimed_ids: List[str] = []

        async with AsyncSessionLocal() as session:
            try:
                bind = session.get_bind()
                is_pg = bool(bind and "postgresql" in str(bind.url))
            except Exception:
                is_pg = "postgresql" in str(async_engine.url)
            stmt = (
                select(NotificationJob)
                .where(
                    or_(
                        and_(NotificationJob.status.in_(("PENDING", "RETRYING")), NotificationJob.next_attempt_at <= now),
                        and_(NotificationJob.status == "PROCESSING", NotificationJob.locked_at <= stale_cutoff),
                    )
                )
                .order_by(NotificationJob.next_attempt_at.asc(), NotificationJob.created_at.asc())
                .limit(batch_size)
            )
            if is_pg:
                stmt = stmt.with_for_update(skip_locked=True)

            jobs = (await session.execute(stmt)).scalars().all()
            for j in jobs:
                j.status = "PROCESSING"
                j.locked_at = now
                j.locked_by = self.worker_id
                j.updated_at = now
                claimed_ids.append(j.id)
            await session.commit()

        processed = 0
        for jid in claimed_ids:
            async with AsyncSessionLocal() as session:
                job = (await session.execute(
                    select(NotificationJob).where(NotificationJob.id == jid)
                )).scalar_one_or_none()
                if not job:
                    continue
                attempt_ts = datetime.now(timezone.utc)
                job.attempt_count = int(job.attempt_count or 0) + 1
                job.last_attempt_at = attempt_ts
                try:
                    msg_id = await self._execute_job(job)
                    job.status = "SENT"
                    job.provider_message_id = msg_id
                    job.last_error = None
                    job.locked_at = None
                    job.locked_by = None
                except Exception as exc:
                    err_str = str(exc)[:400]
                    job.last_error = err_str
                    job.locked_at = None
                    job.locked_by = None
                    # Permanent config-disabled error -> fail cleanly without spinning retries
                    is_unconfigured = "not configured" in err_str.lower() or "disabled/unconfigured" in err_str.lower()
                    if is_unconfigured or job.attempt_count >= (job.max_attempts or CFG.NOTIFICATION_MAX_ATTEMPTS):
                        job.status = "FAILED"
                    else:
                        delay = _BACKOFF_SECONDS[min(job.attempt_count - 1, len(_BACKOFF_SECONDS) - 1)]
                        job.status = "RETRYING"
                        job.next_attempt_at = attempt_ts + timedelta(seconds=delay)
                    log.warning(
                        "Notification job %s (%s/%s -> %s) attempt %d: %s",
                        job.id, job.channel, job.notification_type, job.recipient, job.attempt_count, err_str,
                    )
                job.updated_at = datetime.now(timezone.utc)
                await session.commit()
                processed += 1

        return processed

    # ---------------- Meta WhatsApp Webhook Status Updates ----------------
    async def process_whatsapp_webhook(self, body: Dict[str, Any]) -> int:
        """Update NotificationJob delivery statuses (SENT, DELIVERED, READ, FAILED) from Meta webhook payload."""
        updated = 0
        status_map = {
            "sent": "SENT",
            "delivered": "DELIVERED",
            "read": "READ",
            "failed": "FAILED",
        }
        entries = body.get("entry") or []
        async with AsyncSessionLocal() as session:
            for entry in entries:
                for change in (entry.get("changes") or []):
                    val = change.get("value") or {}
                    for st in (val.get("statuses") or []):
                        wamid = str(st.get("id") or "").strip()
                        raw_st = str(st.get("status") or "").strip().lower()
                        mapped = status_map.get(raw_st)
                        if not wamid or not mapped:
                            continue
                        job = (await session.execute(
                            select(NotificationJob).where(NotificationJob.provider_message_id == wamid)
                        )).scalar_one_or_none()
                        if not job:
                            continue
                        job.status = mapped
                        if mapped == "FAILED":
                            errs = st.get("errors") or []
                            if errs and isinstance(errs[0], dict):
                                job.last_error = f"{errs[0].get('code')}: {errs[0].get('title') or errs[0].get('message')}"[:300]
                        job.updated_at = datetime.now(timezone.utc)
                        updated += 1
            if updated:
                await session.commit()
        return updated

    # ---------------- History & Background Loop ----------------
    async def list_history(self, limit: int = 100, channel: Optional[str] = None, trading_date: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(NotificationJob).order_by(NotificationJob.created_at.desc()).limit(limit)
            if channel:
                stmt = stmt.where(NotificationJob.channel == channel)
            if trading_date:
                stmt = stmt.where(NotificationJob.trading_date == trading_date)
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": j.id,
                    "event_key": j.event_key,
                    "client_id": j.client_id,
                    "trading_date": j.trading_date,
                    "channel": j.channel,
                    "notification_type": j.notification_type,
                    "recipient": j.recipient,
                    "status": j.status,
                    "attempt_count": j.attempt_count,
                    "max_attempts": j.max_attempts,
                    "provider_message_id": j.provider_message_id,
                    "last_error": j.last_error,
                    "last_attempt_at": j.last_attempt_at.isoformat() if j.last_attempt_at else None,
                    "created_at": j.created_at.isoformat() if j.created_at else None,
                }
                for j in rows
            ]

    async def start_worker(self):
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        log.info("NotificationEngine background worker started (%s)", self.worker_id)

    async def stop_worker(self):
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        await self.aclose()
        log.info("NotificationEngine background worker stopped (%s)", self.worker_id)

    async def _worker_loop(self):
        await asyncio.sleep(5)
        while self._running:
            try:
                n = await self.process_pending_jobs_once(batch_size=10)
                if n > 0:
                    continue
            except Exception as e:
                log.error("Notification worker loop error: %s", e, exc_info=True)
            await asyncio.sleep(max(2, CFG.NOTIFICATION_POLL_SECONDS))
