"""Centralized Production-Grade Zebu Broker Session & Token Manager.

Single authoritative source of truth for all Zebu broker authentication,
session reuse, distributed single-flight locking (keyed by zebu_uid),
reactive recovery on genuine session expiry, cooldown/backoff, and
WebSocket token synchronization.

Design invariants:
- No other module calls ZEBU.quick_auth_async() directly.
- Every REST/WS caller obtains (token, auth_mode) from PostgreSQL via this manager.
- Distributed locking uses per-zebu_uid asyncio.Lock + PostgreSQL pg_advisory_lock
  so concurrent tasks and multiple backend workers never duplicate QuickAuth calls.
- Double-checked locking: after acquiring the lock, always re-reads PostgreSQL;
  if another worker/request already refreshed the token, reuses it immediately.
- Never fabricates a 10-year expires_at; broker expiry is unknown (None) unless
  explicitly returned by the broker, and validity is determined by actual broker
  responses.
- Temporary network errors, timeouts, HTTP 429, HTTP 5xx, and empty books NEVER
  disconnect accounts or trigger QuickAuth.
- External login conflicts (e.g. MYNT mobile app login) are detected and throttled
  by a configurable cooldown to prevent infinite login wars.
"""
import asyncio
import hashlib
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from sqlalchemy import select, text

from app.config import CFG
from app import database as _db_mod
from app.database import AsyncSessionLocal
from app.models import Account, BrokerSession
from app.security import decrypt_token
from app.zebu import RateLimitError, SessionExpired, ZebuClient, ZebuError

log = logging.getLogger("tts.session_mgr")

# Permanent/credential error markers that must immediately halt automatic retries
_CREDENTIAL_BLOCK_MARKERS = (
    "blocked",
    "invalid password",
    "invalid input",
    "invalid app key",
    "wrong password",
    "user is blocked",
    "password expired",
    "invalid pan",
    "invalid factor2",
    "invalid user",
)


def _is_credential_or_block_error(msg: str) -> bool:
    low = (msg or "").lower()
    return any(m in low for m in _CREDENTIAL_BLOCK_MARKERS)


def _uid_advisory_lock_key(uid_key: str) -> int:
    """Deterministic signed 64-bit PostgreSQL advisory lock key scoped to a zebu_uid."""
    digest = hashlib.blake2b(f"tts_zebu_auth:{uid_key}".encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


@dataclass
class BrokerSessionInfo:
    account_id: str
    uid: str
    actid: str
    token: str
    auth_mode: str  # "quickauth" | "oauth"
    status: str
    cliname: Optional[str] = None
    refresh_token: Optional[str] = None
    linked_at: Optional[str] = None


@dataclass
class _UidAuthTracker:
    last_attempt_mono: float = 0.0
    last_ok_mono: float = 0.0
    consecutive_failures: int = 0
    cooldown_until_mono: float = 0.0
    last_error: Optional[str] = None
    external_conflict_detected: bool = False


class BrokerSessionManager:
    """Centralized broker session lifecycle manager for all Zebu accounts."""

    def __init__(
        self,
        db: Any,
        zebu: ZebuClient,
        conn_mgr: Any = None,
        ws_sync_cb: Optional[Callable[[dict, str, str], Awaitable[None]]] = None,
        now_ist_fn: Optional[Callable[[], datetime]] = None,
    ):
        self.db = db
        self.zebu = zebu
        self.conn_mgr = conn_mgr
        self.ws_sync_cb = ws_sync_cb
        self.now_ist_fn = now_ist_fn or (lambda: datetime.now(timezone.utc))

        self._uid_locks: Dict[str, asyncio.Lock] = {}
        self._trackers: Dict[str, _UidAuthTracker] = {}

    def _get_uid_lock(self, uid_key: str) -> asyncio.Lock:
        lk = self._uid_locks.get(uid_key)
        if lk is None:
            lk = asyncio.Lock()
            self._uid_locks[uid_key] = lk
        return lk

    def _get_tracker(self, uid_key: str) -> _UidAuthTracker:
        tr = self._trackers.get(uid_key)
        if tr is None:
            tr = _UidAuthTracker()
            self._trackers[uid_key] = tr
        return tr

    def reset_tracker(self, uid_key: str):
        """Resets in-memory cooldown/backoff tracker (used on manual reconnect or tests)."""
        clean = self.zebu._clean_uid(uid_key) or str(uid_key).strip().upper()
        self._trackers.pop(clean, None)

    @asynccontextmanager
    async def _distributed_uid_lock(self, uid_key: str):
        """Acquires an in-process asyncio.Lock plus a transaction-scoped PostgreSQL
        advisory lock (pg_advisory_xact_lock) keyed by normalized zebu_uid.

        Because pg_advisory_xact_lock is bound to the PostgreSQL transaction, it is
        automatically released on commit, rollback, exception, or asyncio.CancelledError —
        preventing leaked session-level locks on pooled connections.
        """
        in_proc_lock = self._get_uid_lock(uid_key)
        async with in_proc_lock:
            is_pg = bool(
                _db_mod.async_engine is not None
                and _db_mod.async_engine.dialect.name == "postgresql"
            )
            if not is_pg:
                yield
                return

            lock_id = _uid_advisory_lock_key(uid_key)
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    await session.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": lock_id})
                    yield

    async def _resolve_account_and_uid(self, account_id: str) -> Tuple[Optional[dict], str]:
        acc = await self.db.get_account_async(account_id)
        if not acc:
            return None, self.zebu._clean_uid(account_id) or str(account_id).strip().upper()
        uid = self.zebu._clean_uid(acc.get("zebu_uid") or acc.get("zebu_actid") or account_id)
        return acc, (uid or str(account_id).strip().upper())

    async def get_valid_session(self, account_id: str) -> Optional[BrokerSessionInfo]:
        """Reads the authoritative session from PostgreSQL without triggering QuickAuth.

        - Always retrieves the stored `auth_mode` from PostgreSQL and registers it on `self.zebu`.
        - Never marks a token expired based on an invented timestamp; only respects an explicit
          non-None `expires_at` if a broker actually supplied one, or explicit DB status.
        - Returns None for manually Disconnected / Not linked / AUTH_BLOCKED / expired sessions.
        """
        async with AsyncSessionLocal() as session:
            stmt = select(BrokerSession).where(BrokerSession.account_id == account_id)
            bs = (await session.execute(stmt)).scalar_one_or_none()
            cur_acc = (
                await session.execute(
                    select(Account).where((Account.id == account_id) | (Account.zebu_uid == account_id))
                )
            ).scalars().first()

            if (not bs or not bs.access_token_enc) and cur_acc and cur_acc.zebu_uid:
                match_stmt = (
                    select(BrokerSession)
                    .join(Account, Account.id == BrokerSession.account_id)
                    .where(
                        (Account.zebu_uid == cur_acc.zebu_uid) | (BrokerSession.account_id == cur_acc.zebu_uid),
                        BrokerSession.access_token_enc.isnot(None),
                    )
                )
                bs = (await session.execute(match_stmt)).scalars().first()

            if not bs or not bs.access_token_enc:
                return None

            status = bs.status or "Not linked"
            if status in ("Not linked", "Disconnected", "Login expired", "REAUTH_REQUIRED", "RECONNECT_REQUIRED", "AUTH_BLOCKED"):
                return None

            # Respect explicit broker-provided expires_at only if set and genuinely past
            if bs.expires_at is not None:
                exp = bs.expires_at
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if exp < datetime.now(timezone.utc):
                    return None

            tok_plain = decrypt_token(bs.access_token_enc)
            if not tok_plain:
                return None

            auth_mode = (getattr(bs, "auth_mode", None) or "oauth").strip().lower()
            self.zebu.register_token_mode(tok_plain, auth_mode)

            uid = self.zebu._clean_uid(cur_acc.zebu_uid if cur_acc else account_id)
            actid = self.zebu._clean_uid((cur_acc.zebu_actid if cur_acc else None) or uid)
            return BrokerSessionInfo(
                account_id=account_id,
                uid=uid,
                actid=actid,
                token=tok_plain,
                auth_mode=auth_mode,
                status=status,
                cliname=bs.cliname,
                refresh_token=decrypt_token(bs.refresh_token_enc) if bs.refresh_token_enc else None,
                linked_at=bs.linked_at.isoformat() if bs.linked_at else None,
            )

    async def register_session(
        self,
        account_id: str,
        token: str,
        auth_mode: str,
        uid: Optional[str] = None,
        actid: Optional[str] = None,
        cliname: Optional[str] = None,
        refresh_token: Optional[str] = None,
        expires_at: Optional[float] = None,
        sync_message: str = "Connected to Zebu",
        sync_ws: bool = True,
    ) -> BrokerSessionInfo:
        """Atomically persists a verified broker session in PostgreSQL, propagates to all accounts
        sharing the same zebu_uid, registers auth_mode on ZebuClient, and updates WebSocket clients."""
        norm_mode = (auth_mode or "oauth").strip().lower()
        clean_uid = self.zebu._clean_uid(uid or "")
        clean_actid = self.zebu._clean_uid(actid or clean_uid)

        acc = await self.db.get_account_async(account_id)
        if acc and not clean_uid:
            clean_uid = self.zebu._clean_uid(acc.get("zebu_uid") or "")
        if acc and not clean_actid:
            clean_actid = self.zebu._clean_uid(acc.get("zebu_actid") or clean_uid)

        self.zebu.register_token_mode(token, norm_mode)

        now_str = self.now_ist_fn().isoformat(timespec="seconds")
        doc: Dict[str, Any] = {
            "access_token": token,
            "refresh_token": refresh_token,
            "auth_mode": norm_mode,
            "expires_at": expires_at,  # None when broker expiry is unknown (no fake 10-year timestamp)
            "linked_at": now_str,
            "status": "Linked",
            "ws_status": "REAL-TIME",
            "cliname": cliname,
            "sync_message": sync_message,
        }
        await self.db.put_token_async(account_id, doc)

        all_acc = await self.db.get_all_accounts_async()
        updated_accounts: List[dict] = []
        for a_row in all_acc:
            row_uid = self.zebu._clean_uid(a_row.get("zebu_uid") or "")
            if a_row["id"] == account_id or (clean_uid and row_uid == clean_uid):
                if a_row["id"] != account_id:
                    await self.db.put_token_async(a_row["id"], doc)
                await self.db.put_sync_state_async(a_row["id"], {
                    "status": "LIVE",
                    "is_stale": False,
                    "last_error": None,
                    "consecutive_failures": 0,
                    "circuit_open": False,
                })
                if self.conn_mgr:
                    st_obj = self.conn_mgr.get_account_state(a_row["id"])
                    st_obj.record_success()
                    st_obj.status = "REAL-TIME"
                updated_accounts.append(a_row)

        if acc and (clean_uid or clean_actid):
            acc_update = dict(acc)
            if clean_uid:
                acc_update["zebu_uid"] = clean_uid
            if clean_actid:
                acc_update["zebu_actid"] = clean_actid
            await self.db.put_account_async(account_id, acc_update)

        if sync_ws and self.ws_sync_cb:
            for a_row in updated_accounts:
                try:
                    await self.ws_sync_cb(a_row, token, norm_mode)
                except Exception as wse:
                    log.debug("WS sync notice after register_session for %s: %s", a_row["id"], wse)

        if clean_uid:
            tr = self._get_tracker(clean_uid)
            tr.last_ok_mono = time.monotonic()
            tr.consecutive_failures = 0
            tr.cooldown_until_mono = 0.0
            tr.last_error = None
            tr.external_conflict_detected = False

        return BrokerSessionInfo(
            account_id=account_id,
            uid=clean_uid,
            actid=clean_actid,
            token=token,
            auth_mode=norm_mode,
            status="Linked",
            cliname=cliname,
            refresh_token=refresh_token,
            linked_at=now_str,
        )

    async def invalidate_session_state(
        self,
        account_id: str,
        status: str = "REAUTH_REQUIRED",
        error_msg: str = "Broker session expired — reauthorization required",
    ):
        """Marks session status in PostgreSQL & ConnectionManager without deleting stored tokens or historical data."""
        token_status = "Login expired" if status in ("REAUTH_REQUIRED", "RECONNECT_REQUIRED") else status
        await self.db.put_token_async(account_id, {
            "status": token_status,
            "ws_status": status,
            "sync_message": error_msg[:300],
        })
        await self.db.put_sync_state_async(account_id, {
            "status": status,
            "is_stale": True,
            "last_error": error_msg[:300],
        })
        if self.conn_mgr:
            st = self.conn_mgr.get_account_state(account_id)
            st.status = status

    async def authenticate_once(
        self,
        account_id: str,
        reason: str = "manual",
        force: bool = False,
        failed_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Single authoritative QuickAuth entry point for the entire backend.

        Acquires a distributed lock keyed by `zebu_uid`, double-checks PostgreSQL for an
        already-refreshed session, respects cooldown/backoff/AUTH_BLOCKED for automatic
        callers, performs at most ONE QuickAuth call, and atomically updates DB + WS.
        """
        acc, uid_key = await self._resolve_account_and_uid(account_id)
        if not acc:
            raise ZebuError(f"Account '{account_id}' not found")

        is_manual = (reason == "manual")
        if is_manual:
            self.reset_tracker(uid_key)

        async with self._distributed_uid_lock(uid_key):
            # 1. Double-check PostgreSQL after acquiring the distributed lock!
            existing = await self.get_valid_session(account_id)
            if existing:
                # If another task/worker already refreshed the token while we waited on the lock, reuse it!
                if failed_token and existing.token != failed_token:
                    log.info(
                        "Single-flight hit: reusing already-refreshed session for account=%s uid=%s (reason=%s)",
                        account_id, uid_key, reason,
                    )
                    return {
                        "ok": True,
                        "access_token": existing.token,
                        "cliname": existing.cliname,
                        "auth_mode": existing.auth_mode,
                        "uid": existing.uid,
                        "actid": existing.actid,
                        "reused": True,
                    }
                # If not forced and no failed_token was reported, reuse valid session
                if not force and not failed_token:
                    return {
                        "ok": True,
                        "access_token": existing.token,
                        "cliname": existing.cliname,
                        "auth_mode": existing.auth_mode,
                        "uid": existing.uid,
                        "actid": existing.actid,
                        "reused": True,
                    }

            # 2. For non-manual callers, enforce manual-disconnect protection, AUTH_BLOCKED, cooldown & backoff
            if not is_manual:
                sync_st = (await self.db.get_sync_state_async(account_id)) or {}
                tok_doc = (await self.db.get_token_async(account_id)) or {}
                st_upper = str(sync_st.get("status") or "").upper()
                if tok_doc.get("status") == "Disconnected" or (not tok_doc.get("access_token") and st_upper == "DISCONNECTED"):
                    raise ZebuError("Account was manually disconnected; automatic reauthentication skipped.")
                if st_upper == "AUTH_BLOCKED":
                    raise ZebuError(f"Account is AUTH_BLOCKED ({sync_st.get('last_error') or 'credential/lockout error'}); manual intervention required.")

                tr = self._get_tracker(uid_key)
                now_m = time.monotonic()

                # Detect external login conflict (e.g. MYNT mobile app logged in right after TTS)
                if failed_token and tr.last_ok_mono > 0 and (now_m - tr.last_ok_mono) < 120.0:
                    tr.external_conflict_detected = True
                    ext_cd = max(int(getattr(CFG, "ZEBU_REAUTH_EXTERNAL_CONFLICT_COOLDOWN_SECONDS", 180)), 60)
                    if (now_m - tr.last_ok_mono) < ext_cd:
                        tr.cooldown_until_mono = max(tr.cooldown_until_mono, tr.last_ok_mono + ext_cd)
                        log.warning(
                            "External login conflict detected for account=%s uid=%s (session invalidated %.1fs after auth). "
                            "Enforcing %ds cooldown to prevent infinite login loop.",
                            account_id, uid_key, now_m - tr.last_ok_mono, ext_cd,
                        )
                        raise ZebuError(
                            f"External MYNT login conflict detected; cooling down for {int(tr.cooldown_until_mono - now_m)}s"
                        )

                if tr.cooldown_until_mono > now_m:
                    wait_s = int(tr.cooldown_until_mono - now_m)
                    raise ZebuError(f"QuickAuth cooldown active for {uid_key} ({wait_s}s remaining)")

                min_cd = max(int(getattr(CFG, "ZEBU_REAUTH_COOLDOWN_SECONDS", 60)), 5)
                if tr.last_attempt_mono > 0 and (now_m - tr.last_attempt_mono) < min_cd:
                    wait_s = int(min_cd - (now_m - tr.last_attempt_mono))
                    raise ZebuError(f"QuickAuth minimum cooldown active for {uid_key} ({wait_s}s remaining)")

            # 3. Load credentials securely from PostgreSQL
            creds = await self.db.get_account_credentials_async(account_id)
            if not creds:
                raise ZebuError(f"Account '{account_id}' not found")

            uid = creds.get("zebu_uid") or ""
            vc = creds.get("zebu_vc") or (f"{uid}_U" if uid else "")
            factor2 = creds.get("zebu_factor2") or ""
            api_secret = creds.get("zebu_secret_key") or CFG.ZEBU_SECRET_KEY or ""
            password = creds.get("zebu_password") or ""
            proxy_url = creds.get("proxy_url")

            missing = []
            if not uid:
                missing.append("Zebu User ID (UID)")
            if not vc:
                missing.append("Vendor Code (VC)")
            if not factor2:
                missing.append("Two Factor (PAN or DOB)")
            if not api_secret:
                missing.append("Secret App Key")
            if not password:
                missing.append("Password")
            if missing:
                raise ZebuError(f"Missing Zebu QuickAuth credentials: {', '.join(missing)}")

            tr = self._get_tracker(uid_key)
            tr.last_attempt_mono = time.monotonic()

            try:
                res = await asyncio.wait_for(
                    self.zebu.quick_auth_async(
                        uid=uid,
                        password=password,
                        factor2=factor2,
                        vc=vc,
                        api_secret=api_secret,
                        proxy=proxy_url,
                    ),
                    timeout=15.0,
                )
            except RateLimitError as rle:
                tr.consecutive_failures += 1
                backoff = min(
                    int(getattr(CFG, "ZEBU_REAUTH_COOLDOWN_SECONDS", 60)) * (2 ** min(tr.consecutive_failures - 1, 5)),
                    int(getattr(CFG, "ZEBU_REAUTH_BACKOFF_MAX_SECONDS", 1800)),
                )
                tr.cooldown_until_mono = time.monotonic() + backoff
                tr.last_error = str(rle)
                raise
            except Exception as e:
                err_str = str(e)
                tr.consecutive_failures += 1
                tr.last_error = err_str
                if _is_credential_or_block_error(err_str):
                    log.error("QuickAuth credential/block rejection for account=%s uid=%s: %s", account_id, uid_key, err_str)
                    await self.invalidate_session_state(account_id, status="AUTH_BLOCKED", error_msg=err_str)
                else:
                    backoff = min(
                        int(getattr(CFG, "ZEBU_REAUTH_COOLDOWN_SECONDS", 60)) * (2 ** min(tr.consecutive_failures - 1, 5)),
                        int(getattr(CFG, "ZEBU_REAUTH_BACKOFF_MAX_SECONDS", 1800)),
                    )
                    tr.cooldown_until_mono = time.monotonic() + backoff
                raise

            susertoken = str(res["susertoken"]).strip()
            clean_uid = self.zebu._clean_uid(res.get("uid") or uid)
            clean_actid = self.zebu._clean_uid(res.get("actid") or creds.get("zebu_actid") or clean_uid)
            cliname = res.get("uname") or res.get("cliname") or res.get("dname") or None

            info = await self.register_session(
                account_id=account_id,
                token=susertoken,
                auth_mode="quickauth",
                uid=clean_uid,
                actid=clean_actid,
                cliname=cliname,
                refresh_token=None,
                expires_at=None,
                sync_message=f"Connected via Zebu QuickAuth ({reason})",
                sync_ws=True,
            )

            log.info("Zebu QuickAuth succeeded for account=%s uid=%s actid=%s reason=%s", account_id, clean_uid, clean_actid, reason)
            return {
                "ok": True,
                "access_token": info.token,
                "cliname": info.cliname,
                "auth_mode": info.auth_mode,
                "uid": info.uid,
                "actid": info.actid,
                "reused": False,
            }

    async def recover_expired_session(
        self,
        account_id: str,
        failed_token: Optional[str] = None,
        reason: str = "session_expired",
    ) -> Optional[BrokerSessionInfo]:
        """Coordinated recovery when a broker API call genuinely returns SessionExpired.

        State machine:
          LINKED -> EXPIRED_RETRYING -> (1 coordinated QuickAuth) ->
            - Success -> LINKED (returns fresh BrokerSessionInfo)
            - Invalid credentials / blocked -> AUTH_BLOCKED (returns None)
            - Auto-reauth disabled / OAuth-only without QuickAuth creds -> REAUTH_REQUIRED (returns None)
            - Temporary network/5xx/429 during QuickAuth -> STALE (retains token, returns None)
        """
        if not getattr(CFG, "ZEBU_AUTO_REAUTH_ON_SESSION_EXPIRED", True):
            log.info("Auto-reauth disabled (ZEBU_AUTO_REAUTH_ON_SESSION_EXPIRED=false) for account=%s", account_id)
            await self.invalidate_session_state(
                account_id,
                status="REAUTH_REQUIRED",
                error_msg="Broker session expired (auto-reauth disabled)",
            )
            return None

        # Transition to EXPIRED_RETRYING in sync state without wiping token
        try:
            await self.db.put_sync_state_async(account_id, {
                "status": "EXPIRED_RETRYING",
                "is_stale": True,
                "last_error": f"Recovering expired session ({reason})",
            })
        except Exception:
            pass

        try:
            res = await self.authenticate_once(
                account_id=account_id,
                reason=reason,
                force=True,
                failed_token=failed_token,
            )
            if res and res.get("ok") and res.get("access_token"):
                return BrokerSessionInfo(
                    account_id=account_id,
                    uid=res.get("uid") or "",
                    actid=res.get("actid") or res.get("uid") or "",
                    token=res["access_token"],
                    auth_mode=res.get("auth_mode") or "quickauth",
                    status="Linked",
                    cliname=res.get("cliname"),
                )
        except (RateLimitError,) as rle:
            # Temporary rate limit -> STALE, retain existing token
            await self.db.put_sync_state_async(account_id, {
                "status": "STALE",
                "is_stale": True,
                "last_error": f"Rate limited during recovery: {rle}",
            })
            return None
        except Exception as e:
            err_s = str(e)
            low = err_s.lower()
            if _is_credential_or_block_error(err_s) or "auth_blocked" in low:
                # Already transitioned to AUTH_BLOCKED inside authenticate_once
                return None
            if "network error" in low or "timeout" in low or "proxy" in low or "http 5" in low:
                # Temporary network failure -> STALE (never disconnect!)
                await self.db.put_sync_state_async(account_id, {
                    "status": "STALE",
                    "is_stale": True,
                    "last_error": f"Temporary network error during recovery: {err_s[:220]}",
                })
                return None
            if "cooldown" in low or "external mynt login conflict" in low:
                await self.db.put_sync_state_async(account_id, {
                    "status": "REAUTH_REQUIRED",
                    "is_stale": True,
                    "last_error": err_s[:260],
                })
                return None
            if "manually disconnected" in low:
                return None
            # Missing QuickAuth creds (e.g. OAuth-only account) or unrecoverable broker rejection
            await self.invalidate_session_state(
                account_id,
                status="REAUTH_REQUIRED",
                error_msg=f"Session expired; recovery unavailable: {err_s[:220]}",
            )
            return None

        return None

    async def health_check_session(self, account_id: str, recover_if_expired: bool = True) -> str:
        """Lightweight read-only session health check using ClientDetails.

        - NEVER calls QuickAuth if the existing session is valid.
        - On genuine SessionExpired, performs at most ONE coordinated recovery (if enabled).
        - On temporary network error / 429 / 5xx, leaves session Linked and returns 'stale'.
        """
        acc = await self.db.get_account_async(account_id)
        if not acc or (acc.get("broker") or "Zebu") != "Zebu" or not acc.get("zebu_uid"):
            return "skipped"

        sync_st = (await self.db.get_sync_state_async(account_id)) or {}
        st_upper = str(sync_st.get("status") or "").upper()
        if st_upper in ("DISCONNECTED", "AUTH_BLOCKED"):
            return f"skipped_{st_upper.lower()}"

        sess = await self.get_valid_session(account_id)
        if not sess:
            tok_doc = (await self.db.get_token_async(account_id)) or {}
            t_status = tok_doc.get("status") or "Not linked"
            if t_status in ("Disconnected", "Not linked", "AUTH_BLOCKED"):
                return "skipped_unlinked"
            if recover_if_expired and t_status in ("Login expired", "REAUTH_REQUIRED", "RECONNECT_REQUIRED"):
                rec = await self.recover_expired_session(account_id, reason="health_check")
                return "recovered" if rec else "expired"
            return "unlinked"

        try:
            await asyncio.wait_for(
                self.zebu.client_details_async(
                    sess.token,
                    sess.uid,
                    sess.actid,
                    proxy=acc.get("proxy_url"),
                    auth_mode=sess.auth_mode,
                ),
                timeout=10.0,
            )
            return "valid"
        except SessionExpired:
            if recover_if_expired:
                rec = await self.recover_expired_session(account_id, failed_token=sess.token, reason="health_check")
                return "recovered" if rec else "expired"
            return "expired"
        except Exception as e:
            log.debug("Health check transient error for %s (retaining session): %s", account_id, e)
            return "stale"

    async def restore_sessions_on_startup(self, accounts: List[dict]) -> int:
        """Loads existing PostgreSQL sessions on backend startup, registers exact auth_mode on
        ZebuClient, and attaches WebSocket streams without performing unnecessary QuickAuth."""
        restored = 0
        for acc in accounts:
            if (acc.get("broker") or "Zebu") != "Zebu" or not acc.get("zebu_uid"):
                continue
            sess = await self.get_valid_session(acc["id"])
            if sess:
                restored += 1
                if self.ws_sync_cb:
                    try:
                        await self.ws_sync_cb(acc, sess.token, sess.auth_mode)
                    except Exception as wse:
                        log.debug("Startup WS restore notice for %s: %s", acc["id"], wse)
        log.info("Startup session restore complete: %d active Zebu sessions restored from PostgreSQL.", restored)
        return restored
