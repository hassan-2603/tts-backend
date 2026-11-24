"""Production-Grade, Read-Only Financial Summary Service (Realized P&L & Unrealized MTM).

Authoritative source: Official Zebu OAuth PositionBook API
  POST /NorenWClientAPI/PositionBook
  Authorization: Bearer <access_token>
  Content-Type: text/plain
  Body: jData={"uid":"<uid>","actid":"<actid>"}

Live MTM updates: Official Zebu NorenWSTP WebSocket Touchline feed (t=t / t=u)
  Recalculates ONLY open positions using exact Decimal arithmetic:
    MTM = netqty * (LTP - netavgprc) * prcftr
  NEVER modifies Realized P&L (rpnl) from price movements.

Strictly READ-ONLY: never places, modifies, cancels orders, or mutates broker state.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

from app.scheduler import IST, now_ist
from app.zebu import RateLimitError, SessionExpired, ZebuClient, ZebuError

log = logging.getLogger("tts.financial_summary")

TWO_DP = Decimal("0.01")
FEED_STALE_SECONDS = 45.0
CACHE_TTL_SECONDS = 3.0  # Short single-flight reuse window to avoid hammering broker API
FORCE_REFRESH_COOLDOWN_SECONDS = 2.0  # Prevents force=1 abuse from triggering unlimited broker calls
FORCE_REFRESH_MAX_PER_MINUTE = 10     # Per-account sliding-window cap for forced refreshes


class FinancialValidationError(Exception):
    """Raised when a broker PositionBook response fails structural or account integrity checks."""


def _clean_id(val: Optional[str]) -> str:
    s = (val or "").strip().upper()
    return s[:-2] if s.endswith("_U") else s


def _parse_decimal_strict(val: Any) -> Optional[Decimal]:
    """Parses a numeric value into Decimal without accepting booleans, NaN, Infinity, or blanks."""
    if val is None or isinstance(val, bool):
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        d = Decimal(s)
        if not d.is_finite():
            return None
        return d
    except (InvalidOperation, ValueError):
        return None


def _quantize_money(d: Decimal) -> Decimal:
    return d.quantize(TWO_DP, rounding=ROUND_HALF_UP)


def _fmt_iso_utc(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _fmt_ist_display(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).strftime("%d %b %Y, %H:%M:%S IST")


@dataclass
class OpenPositionState:
    key: str                  # Canonical key: EXCH|TOKEN|PRD or EXCH|TSYM|PRD
    scrip_key: str            # Touchline key: EXCH|TOKEN
    exch: str
    token: str
    tsym: str
    prd: str
    netqty: Decimal
    netavgprc: Decimal
    prcftr: Decimal
    lp: Optional[Decimal]
    broker_urmtom: Optional[Decimal]
    live_urmtom: Optional[Decimal]
    last_tick_at: Optional[datetime] = None
    last_feed_ft: Optional[str] = None


@dataclass
class ValidatedPositionSnapshot:
    account_id: str
    uid: str
    actid: str
    fetched_at: datetime
    request_time: Optional[str]
    positions_count: int
    open_positions_count: int
    duplicate_rows_ignored: int
    empty_confirmed: bool
    realized_pnl: Optional[Decimal]
    realized_valid: bool
    realized_error: Optional[str]
    unrealized_mtm: Optional[Decimal]
    mtm_valid: bool
    mtm_error: Optional[str]
    position_pnl: Optional[Decimal] = None
    total_mtm: Optional[Decimal] = None
    open_positions: Dict[str, OpenPositionState] = field(default_factory=dict)
    raw_audit_rows: List[dict] = field(default_factory=list)


def compute_mynt_position_row_pnl(row: dict) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """Booked (Realized) P&L and Open Unrealized MTM from a raw Zebu PositionBook row.

    Returns (booked_pnl, open_mtm):
      - open_mtm   = urmtom (0.00 when netqty == 0; open positions' unrealized MTM only)
      - booked_pnl = closed/booked P&L only (never includes unrealized MTM of held open qty):
          - When netqty == 0 and carry-forward (cfbuyqty > 0 or cfsellqty > 0) with upldprc > 0:
              (cfsellqty * upldprc * prcftr + daysellamt) - (cfbuyqty * upldprc * prcftr + daybuyamt)
          - Otherwise (pure intraday closed, or held open position):
              rpnl  (0.00 for untouched held open positions)
    """
    if not isinstance(row, dict):
        return None, None
    rpnl_dec = _parse_decimal_strict(row.get("rpnl"))
    urmtom_dec = _parse_decimal_strict(row.get("urmtom"))
    netqty_dec = _parse_decimal_strict(row.get("netqty")) or Decimal("0")
    netavgprc_dec = _parse_decimal_strict(row.get("netavgprc"))
    lp_dec = _parse_decimal_strict(row.get("lp")) or Decimal("0")

    raw_prcftr = row.get("prcftr")
    if raw_prcftr is not None and str(raw_prcftr).strip() != "":
        prcftr_dec = _parse_decimal_strict(raw_prcftr) or Decimal("1")
    else:
        prcftr_dec = Decimal("1")

    if urmtom_dec is None and netavgprc_dec is not None:
        urmtom_dec = netqty_dec * (lp_dec - netavgprc_dec) * prcftr_dec

    open_mtm = urmtom_dec

    cfbuy_qty = _parse_decimal_strict(row.get("cfbuyqty")) or Decimal("0")
    cfsell_qty = _parse_decimal_strict(row.get("cfsellqty")) or Decimal("0")
    upldprc = _parse_decimal_strict(row.get("upldprc")) or Decimal("0")
    daybuy_amt = _parse_decimal_strict(row.get("daybuyamt")) or Decimal("0")
    daysell_amt = _parse_decimal_strict(row.get("daysellamt")) or Decimal("0")

    if netqty_dec == Decimal("0") and (cfbuy_qty > Decimal("0") or cfsell_qty > Decimal("0")) and upldprc > Decimal("0"):
        orig_buy_amt = cfbuy_qty * upldprc * prcftr_dec + daybuy_amt
        orig_sell_amt = cfsell_qty * upldprc * prcftr_dec + daysell_amt
        booked_pnl = orig_sell_amt - orig_buy_amt
    else:
        booked_pnl = rpnl_dec

    return booked_pnl, open_mtm


@dataclass
class AccountFinancialCacheEntry:
    cache_key: str
    account_id: str
    uid: str
    actid: str
    trader_id: Optional[str] = None
    last_verified: Optional[ValidatedPositionSnapshot] = None
    last_fetch_attempt_at: Optional[datetime] = None
    last_fetch_mono: float = 0.0
    last_latency_ms: Optional[float] = None
    last_error: Optional[str] = None
    last_error_type: Optional[str] = None
    ws_status: str = "DISCONNECTED"
    last_feed_at: Optional[datetime] = None
    last_feed_mono: float = 0.0
    last_reconciled_at: Optional[datetime] = None
    cache_hits: int = 0
    cache_misses: int = 0
    live_mtm_override: Optional[Decimal] = None
    force_refresh_timestamps: List[float] = field(default_factory=list)


def validate_and_aggregate_position_book(
    raw_payload: Any,
    account_id: str,
    expected_uid: str,
    expected_actid: str,
) -> ValidatedPositionSnapshot:
    """Validates raw PositionBook JSON and aggregates Realized P&L (rpnl) and Unrealized MTM (urmtom)
    independently using exact Decimal arithmetic.

    Raises:
      SessionExpired: when broker indicates expired/invalid session.
      FinancialValidationError: when response is malformed, Not_Ok (non-empty error), or belongs to another account.
    """
    exp_uid = _clean_id(expected_uid)
    exp_actid = _clean_id(expected_actid) or exp_uid
    now_utc = datetime.now(timezone.utc)

    # 1. Validate top-level JSON structure & broker stat semantics
    if isinstance(raw_payload, dict):
        stat = str(raw_payload.get("stat") or "").strip()
        emsg = str(raw_payload.get("emsg") or "").strip()
        low = emsg.lower()
        if stat.lower() == "not_ok":
            if "session" in low or ("invalid" in low and "key" in low) or "token expired" in low or "unauthorized" in low:
                raise SessionExpired(f"PositionBook session expired: {emsg}")
            # Verified Zebu empty-position semantics: {"stat": "Not_Ok", "emsg": "no data : No Data"}
            if "no data" in low or "no record" in low or "no position" in low or "empty" in low:
                return ValidatedPositionSnapshot(
                    account_id=account_id,
                    uid=exp_uid,
                    actid=exp_actid,
                    fetched_at=now_utc,
                    request_time=raw_payload.get("request_time"),
                    positions_count=0,
                    open_positions_count=0,
                    duplicate_rows_ignored=0,
                    empty_confirmed=True,
                    realized_pnl=Decimal("0.00"),
                    realized_valid=True,
                    realized_error=None,
                    unrealized_mtm=Decimal("0.00"),
                    mtm_valid=True,
                    mtm_error=None,
                    position_pnl=Decimal("0.00"),
                    total_mtm=Decimal("0.00"),
                    open_positions={},
                    raw_audit_rows=[],
                )
            raise FinancialValidationError(f"Broker PositionBook returned Not_Ok: {emsg or 'Unknown error'}")
        raise FinancialValidationError(f"Unexpected PositionBook object response (stat={stat!r})")

    if not isinstance(raw_payload, list):
        raise FinancialValidationError(f"Malformed PositionBook payload type: {type(raw_payload).__name__}")

    if len(raw_payload) == 0:
        # Empty list returned directly from broker (confirmed no positions)
        return ValidatedPositionSnapshot(
            account_id=account_id,
            uid=exp_uid,
            actid=exp_actid,
            fetched_at=now_utc,
            request_time=None,
            positions_count=0,
            open_positions_count=0,
            duplicate_rows_ignored=0,
            empty_confirmed=True,
            realized_pnl=Decimal("0.00"),
            realized_valid=True,
            realized_error=None,
            unrealized_mtm=Decimal("0.00"),
            mtm_valid=True,
            mtm_error=None,
            open_positions={},
            raw_audit_rows=[],
        )

    # 2. Validate every row, enforce account ownership, and deduplicate
    seen_keys: set[str] = set()
    dup_count = 0
    realized_sum = Decimal("0")
    realized_valid = True
    realized_error: Optional[str] = None
    position_pnl_sum = Decimal("0")
    total_mtm_sum = Decimal("0")

    mtm_sum = Decimal("0")
    mtm_valid = True
    mtm_error: Optional[str] = None

    open_positions: Dict[str, OpenPositionState] = {}
    audit_rows: List[dict] = []
    req_time: Optional[str] = None

    for idx, row in enumerate(raw_payload):
        if not isinstance(row, dict):
            raise FinancialValidationError(f"Malformed PositionBook row at index {idx}: expected dict")

        row_stat = row.get("stat")
        if row_stat is not None and str(row_stat).strip().lower() != "ok":
            emsg = str(row.get("emsg") or "")
            if "session" in emsg.lower():
                raise SessionExpired(emsg)
            raise FinancialValidationError(f"PositionBook row {idx} has stat={row_stat!r} ({emsg})")

        # Strict UID / ACTID validation — never accept rows from another account
        row_uid = _clean_id(row.get("uid"))
        row_actid = _clean_id(row.get("actid"))
        if row_uid and exp_uid and row_uid != exp_uid:
            raise FinancialValidationError(
                f"Account UID mismatch in PositionBook row {idx}: expected {exp_uid}, got {row_uid}"
            )
        if row_actid and exp_actid and row_actid != exp_actid:
            raise FinancialValidationError(
                f"Account ACTID mismatch in PositionBook row {idx}: expected {exp_actid}, got {row_actid}"
            )

        exch = str(row.get("exch") or "").strip().upper()
        tsym = str(row.get("tsym") or "").strip()
        token = str(row.get("token") or "").strip()
        prd = str(row.get("prd") or "").strip().upper() or "NA"

        if not exch or (not token and not tsym):
            raise FinancialValidationError(
                f"Missing instrument identifier in PositionBook row {idx} (exch={exch!r}, token={token!r}, tsym={tsym!r})"
            )

        canon_key = f"{exch}|{token or tsym}|{prd}"
        if canon_key in seen_keys:
            dup_count += 1
            log.warning(
                "Duplicate PositionBook row ignored for account=%s key=%s (row %d)",
                account_id, canon_key, idx,
            )
            continue
        seen_keys.add(canon_key)

        if not req_time and row.get("request_time"):
            req_time = str(row.get("request_time"))

        # Sanitized audit snapshot (only documented PositionBook fields, no secrets)
        audit_rows.append({
            k: row.get(k)
            for k in ("exch", "tsym", "token", "prd", "netqty", "netavgprc", "lp", "rpnl", "urmtom", "prcftr", "mult", "pp", "request_time")
            if k in row
        })

        # --- CARD 1: Realized P&L (rpnl) ---
        rpnl_dec = _parse_decimal_strict(row.get("rpnl"))
        if rpnl_dec is None:
            realized_valid = False
            realized_error = f"Missing or invalid rpnl on {exch}:{tsym or token}"
        elif realized_valid:
            realized_sum += rpnl_dec

        # --- CARD 2: Unrealized MTM (urmtom) ---
        netqty_dec = _parse_decimal_strict(row.get("netqty"))
        netavgprc_dec = _parse_decimal_strict(row.get("netavgprc"))
        lp_dec = _parse_decimal_strict(row.get("lp"))

        # prcftr handling: never assume prcftr=1 if present & invalid; use exact broker prcftr when present
        raw_prcftr = row.get("prcftr")
        prcftr_dec: Optional[Decimal]
        if raw_prcftr is not None and str(raw_prcftr).strip() != "":
            prcftr_dec = _parse_decimal_strict(raw_prcftr)
        else:
            # Zebu omits prcftr on some standard equity/F&O rows where factor is 1.000000
            prcftr_dec = Decimal("1")

        urmtom_dec = _parse_decimal_strict(row.get("urmtom"))
        if urmtom_dec is not None:
            if mtm_valid:
                mtm_sum += urmtom_dec
        else:
            # Fallback to Zebu documented formula ONLY when all required fields are valid & non-null
            if (
                netqty_dec is not None
                and netavgprc_dec is not None
                and lp_dec is not None
                and prcftr_dec is not None
            ):
                calc_mtm = netqty_dec * (lp_dec - netavgprc_dec) * prcftr_dec
                urmtom_dec = calc_mtm
                if mtm_valid:
                    mtm_sum += calc_mtm
            else:
                mtm_valid = False
                mtm_error = f"Missing or invalid urmtom/price fields on {exch}:{tsym or token}"

        # Exact MYNT Position P&L & Total MTM per row
        row_pos_pnl, row_tot_mtm = compute_mynt_position_row_pnl(row)
        if row_pos_pnl is not None and realized_valid:
            position_pnl_sum += row_pos_pnl
        if row_tot_mtm is not None and mtm_valid:
            total_mtm_sum += row_tot_mtm

        # Track open positions (netqty != 0) for live WebSocket Touchline MTM updates
        if netqty_dec is not None and netqty_dec != Decimal("0"):
            scrip_key = f"{exch}|{token}" if token else ""
            if netavgprc_dec is not None and prcftr_dec is not None:
                open_positions[canon_key] = OpenPositionState(
                    key=canon_key,
                    scrip_key=scrip_key,
                    exch=exch,
                    token=token,
                    tsym=tsym,
                    prd=prd,
                    netqty=netqty_dec,
                    netavgprc=netavgprc_dec,
                    prcftr=prcftr_dec,
                    lp=lp_dec,
                    broker_urmtom=urmtom_dec,
                    live_urmtom=urmtom_dec,
                    last_tick_at=now_utc,
                )

    return ValidatedPositionSnapshot(
        account_id=account_id,
        uid=exp_uid,
        actid=exp_actid,
        fetched_at=now_utc,
        request_time=req_time,
        positions_count=len(seen_keys),
        open_positions_count=len(open_positions),
        duplicate_rows_ignored=dup_count,
        empty_confirmed=False,
        realized_pnl=_quantize_money(realized_sum) if realized_valid else None,
        realized_valid=realized_valid,
        realized_error=realized_error,
        unrealized_mtm=_quantize_money(mtm_sum) if mtm_valid else None,
        mtm_valid=mtm_valid,
        mtm_error=mtm_error,
        position_pnl=_quantize_money(position_pnl_sum) if realized_valid else None,
        total_mtm=_quantize_money(total_mtm_sum) if (realized_valid and mtm_valid) else None,
        open_positions=open_positions,
        raw_audit_rows=audit_rows,
    )


class FinancialSummaryService:
    """Dedicated, account-isolated, read-only financial summary service for Overview P&L & MTM."""

    def __init__(
        self,
        zebu: ZebuClient,
        conn_mgr: Any = None,
        ws_hub: Any = None,
        token_resolver: Optional[Callable[[str], Coroutine[Any, Any, Optional[str]]]] = None,
        link_state_resolver: Optional[Callable[[dict], Coroutine[Any, Any, str]]] = None,
        position_persister: Optional[Callable[..., Coroutine[Any, Any, Any]]] = None,
        reauth_resolver: Optional[Callable[..., Coroutine[Any, Any, Optional[Any]]]] = None,
        session_resolver: Optional[Callable[[str], Coroutine[Any, Any, Optional[Any]]]] = None,
    ):
        self.zebu = zebu
        self.conn_mgr = conn_mgr
        self.ws_hub = ws_hub
        self.token_resolver = token_resolver
        self.link_state_resolver = link_state_resolver
        self.position_persister = position_persister
        self.reauth_resolver = reauth_resolver
        self.session_resolver = session_resolver

        # Account-isolated cache keyed by "zebu:<uid>:<actid>:<account_id>"
        self._cache: Dict[str, AccountFinancialCacheEntry] = {}
        # Index from account_id -> cache_key
        self._key_by_account: Dict[str, str] = {}
        # Single-flight deduplication map keyed by cache_key
        self._in_flight: Dict[str, asyncio.Task] = {}
        # Reconcile tasks keyed by account_id (prevents duplicate background refresh jobs)
        self._reconcile_tasks: Dict[str, asyncio.Task] = {}
        # Last active selected account per user for account-switch diagnostics
        self._last_selected_by_user: Dict[str, str] = {}

    @staticmethod
    def make_cache_key(account_id: str, uid: str, actid: str) -> str:
        return f"zebu:{_clean_id(uid)}:{_clean_id(actid) or _clean_id(uid)}:{account_id}"

    def _get_or_create_entry(self, account_id: str, uid: str, actid: str) -> AccountFinancialCacheEntry:
        key = self.make_cache_key(account_id, uid, actid)
        entry = self._cache.get(key)
        if entry is None:
            entry = AccountFinancialCacheEntry(
                cache_key=key,
                account_id=account_id,
                uid=_clean_id(uid),
                actid=_clean_id(actid) or _clean_id(uid),
            )
            self._cache[key] = entry
        self._key_by_account[account_id] = key
        return entry

    def get_entry_for_account(self, account_id: str) -> Optional[AccountFinancialCacheEntry]:
        key = self._key_by_account.get(account_id)
        return self._cache.get(key) if key else None

    def clear_account(self, account_id: str):
        key = self._key_by_account.pop(account_id, None)
        if key:
            self._cache.pop(key, None)

    async def get_summary(
        self,
        acc: dict,
        user_id: str = "system",
        force: bool = False,
        ensure_ws_cb: Optional[Callable[[dict, str], Coroutine[Any, Any, None]]] = None,
    ) -> dict:
        """Returns verified account-specific Realized P&L and Unrealized MTM summary.

        - Uses single-flight deduplication so simultaneous Overview requests share one broker call.
        - Never mixes cached data across accounts.
        - Never overwrites a last verified snapshot on broker/network/validation failure.
        - Never fabricates zero values when data is unavailable.
        """
        aid = acc["id"]
        uid = _clean_id(acc.get("zebu_uid"))
        actid = _clean_id(acc.get("zebu_actid") or uid)

        prev_sel = self._last_selected_by_user.get(user_id)
        if prev_sel and prev_sel != aid:
            log.info("Account switch detected: user=%s from=%s to=%s", user_id, prev_sel, aid)
        self._last_selected_by_user[user_id] = aid

        entry = self._get_or_create_entry(aid, uid, actid)
        entry.trader_id = acc.get("trader") or entry.trader_id

        # Resolve token + auth_mode securely server-side via centralized SessionManager
        token: Optional[str] = None
        auth_mode: Optional[str] = None
        if self.session_resolver:
            sess_info = await self.session_resolver(aid)
            if sess_info:
                token = getattr(sess_info, "token", None)
                auth_mode = getattr(sess_info, "auth_mode", None)
        elif self.token_resolver:
            token = await self.token_resolver(aid)

        if not token or not uid:
            lstate = await self.link_state_resolver(acc) if self.link_state_resolver else "Not linked"
            if lstate in ("Login expired", "REAUTH_REQUIRED", "RECONNECT_REQUIRED"):
                # Attempt one coordinated recovery if auto-reauth is enabled
                if self.reauth_resolver and uid:
                    try:
                        rec = await self.reauth_resolver(aid, failed_token=None, reason="financial_summary")
                        if rec:
                            token = getattr(rec, "token", rec) if not isinstance(rec, str) else rec
                            auth_mode = getattr(rec, "auth_mode", auth_mode) if not isinstance(rec, str) else auth_mode
                    except Exception:
                        pass
            if not token or not uid:
                if lstate in ("Login expired", "REAUTH_REQUIRED", "RECONNECT_REQUIRED", "AUTH_BLOCKED"):
                    entry.last_error_type = "SESSION_EXPIRED"
                    entry.last_error = "Zebu session expired — re-authorization required"
                    log.warning("FinancialSummary token expired: account=%s uid=%s", aid, uid)
                    return self._build_response(entry, acc, overall_status="REAUTH_REQUIRED", cache_hit=False)
                entry.last_error_type = "NOT_LINKED"
                entry.last_error = "Account is not linked to Zebu"
                return self._build_response(entry, acc, overall_status="NOT_LINKED", cache_hit=False)

        # Serve recent verified snapshot within short TTL unless forced
        now_mono = time.monotonic()
        if force and entry.last_verified is not None and entry.last_error is None:
            # Guard against force=1 abuse: enforce minimum cooldown & per-minute sliding-window cap
            entry.force_refresh_timestamps = [t for t in entry.force_refresh_timestamps if now_mono - t < 60.0]
            if (
                (now_mono - entry.last_fetch_mono) < FORCE_REFRESH_COOLDOWN_SECONDS
                or len(entry.force_refresh_timestamps) >= FORCE_REFRESH_MAX_PER_MINUTE
            ):
                entry.cache_hits += 1
                log.info("FinancialSummary force refresh throttled: account=%s (serving verified snapshot)", aid)
                resp = self._build_response(entry, acc, overall_status=None, cache_hit=True)
                resp["force_throttled"] = True
                return resp
            entry.force_refresh_timestamps.append(now_mono)

        if (
            not force
            and entry.last_verified is not None
            and (now_mono - entry.last_fetch_mono) < CACHE_TTL_SECONDS
            and entry.last_error is None
        ):
            entry.cache_hits += 1
            log.debug("FinancialSummary cache HIT: account=%s key=%s", aid, entry.cache_key)
            return self._build_response(entry, acc, overall_status=None, cache_hit=True)

        entry.cache_misses += 1

        # Single-flight deduplication per account cache key
        in_flight = self._in_flight.get(entry.cache_key)
        if in_flight is not None and not in_flight.done():
            log.debug("FinancialSummary joining in-flight request: account=%s", aid)
            await asyncio.shield(in_flight)
            return self._build_response(entry, acc, overall_status=None, cache_hit=False)

        task = asyncio.create_task(
            self._fetch_and_validate_position_book(entry, acc, token, ensure_ws_cb=ensure_ws_cb, auth_mode=auth_mode)
        )
        self._in_flight[entry.cache_key] = task
        try:
            await asyncio.shield(task)
        finally:
            if self._in_flight.get(entry.cache_key) is task:
                self._in_flight.pop(entry.cache_key, None)

        return self._build_response(entry, acc, overall_status=None, cache_hit=False)

    async def _fetch_and_validate_position_book(
        self,
        entry: AccountFinancialCacheEntry,
        acc: dict,
        token: str,
        ensure_ws_cb: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
        _retried_auth: bool = False,
        auth_mode: Optional[str] = None,
    ):
        aid = entry.account_id
        uid = entry.uid
        actid = entry.actid
        proxy_url = acc.get("proxy_url")
        entry.last_fetch_attempt_at = datetime.now(timezone.utc)
        t0 = time.perf_counter()
        log.info("PositionBook fetch START: account=%s uid=%s actid=%s", aid, uid, actid)

        try:
            raw_payload, http_status, elapsed_ms = await self.zebu.position_book_snapshot_async(
                token=token,
                uid=uid,
                actid=actid,
                proxy=proxy_url,
                auth_mode=auth_mode,
            )
            entry.last_latency_ms = elapsed_ms

            snapshot = validate_and_aggregate_position_book(
                raw_payload=raw_payload,
                account_id=aid,
                expected_uid=uid,
                expected_actid=actid,
            )

            # Publish validated snapshot atomically (never overwrite with failed/malformed data)
            entry.last_verified = snapshot
            entry.last_fetch_mono = time.monotonic()
            entry.last_reconciled_at = snapshot.fetched_at
            entry.last_error = None
            entry.last_error_type = None
            entry.live_mtm_override = None

            # Sync validated current PositionBook snapshot to DB atomically so Trader Report & Overview match 100%
            if self.position_persister and isinstance(raw_payload, list) or (self.position_persister and snapshot.empty_confirmed):
                try:
                    await self.position_persister(
                        account_id=aid,
                        positions=raw_payload if isinstance(raw_payload, list) else [],
                        expected_uid=uid,
                        expected_actid=actid,
                        allow_empty=bool(snapshot.empty_confirmed and self._is_market_open_ist()),
                        snapshot_time=snapshot.fetched_at,
                    )
                except Exception as pe:
                    log.warning("Position snapshot DB sync notice for account=%s: %s", aid, pe)

            log.info(
                "PositionBook fetch OK: account=%s uid=%s http=%s duration_ms=%.2f positions=%d open=%d rpnl=%s urmtom=%s",
                aid, uid, http_status, elapsed_ms, snapshot.positions_count,
                snapshot.open_positions_count, snapshot.realized_pnl, snapshot.unrealized_mtm,
            )

            # Ensure WebSocket stream & subscribe ONLY to this account's open positions
            if ensure_ws_cb:
                try:
                    await ensure_ws_cb(acc, token)
                except Exception as wse:
                    log.debug("WS ensure warning for account=%s: %s", aid, wse)

            if self.conn_mgr:
                conn_st = self.conn_mgr.get_account_state(aid)
                entry.ws_status = conn_st.status
                open_scrips = sorted({
                    pos.scrip_key for pos in snapshot.open_positions.values() if pos.scrip_key
                })
                try:
                    await self.conn_mgr.set_account_touchline_subscriptions(aid, open_scrips)
                except Exception as sube:
                    log.debug("Touchline subscription sync warning for %s: %s", aid, sube)

        except SessionExpired as se:
            if not _retried_auth and self.reauth_resolver:
                try:
                    rec = await self.reauth_resolver(aid, failed_token=token, reason="financial_summary")
                    fresh_tok = getattr(rec, "token", rec) if (rec and not isinstance(rec, str)) else rec
                    fresh_mode = getattr(rec, "auth_mode", auth_mode) if (rec and not isinstance(rec, str)) else auth_mode
                    if fresh_tok:
                        log.info("PositionBook recovered session via SessionManager for account=%s; retrying", aid)
                        await self._fetch_and_validate_position_book(
                            entry, acc, fresh_tok, ensure_ws_cb=ensure_ws_cb, _retried_auth=True, auth_mode=fresh_mode
                        )
                        return
                except Exception as re_err:
                    log.warning("PositionBook auto-reauth failed for account=%s: %s", aid, re_err)
            entry.last_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            entry.last_error = f"Session expired: {se}"
            entry.last_error_type = "SESSION_EXPIRED"
            log.warning("PositionBook SESSION_EXPIRED: account=%s uid=%s duration_ms=%.2f", aid, uid, entry.last_latency_ms)
        except FinancialValidationError as ve:
            entry.last_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            entry.last_error = f"Validation failed: {ve}"
            entry.last_error_type = "VALIDATION_ERROR"
            log.error("PositionBook VALIDATION_ERROR: account=%s uid=%s err=%s", aid, uid, ve)
        except RateLimitError as rle:
            entry.last_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            entry.last_error = f"Broker rate limit exceeded: {rle}"
            entry.last_error_type = "RATE_LIMIT"
            log.warning("PositionBook RATE_LIMIT: account=%s uid=%s", aid, uid)
        except ZebuError as ze:
            entry.last_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            entry.last_error = f"Broker API error: {ze}"
            entry.last_error_type = "NETWORK_ERROR"
            log.error("PositionBook NETWORK_ERROR: account=%s uid=%s duration_ms=%.2f err=%s", aid, uid, entry.last_latency_ms, ze)
        except Exception as e:
            entry.last_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            entry.last_error = f"Unexpected error: {e}"
            entry.last_error_type = "UNKNOWN_ERROR"
            log.error("PositionBook UNKNOWN_ERROR: account=%s uid=%s err=%s", aid, uid, e)

    async def on_touchline_tick(self, account_id: str, msg: Dict[str, Any]) -> bool:
        """Processes a Zebu WebSocket Touchline tick (e, tk, lp, ft) for an account.

        Recalculates ONLY Unrealized MTM for matching open positions using:
          MTM = netqty * (LTP - netavgprc) * prcftr
        NEVER changes Realized P&L (rpnl).
        Returns True if MTM changed and was updated.
        """
        entry = self.get_entry_for_account(account_id)
        if not entry or not entry.last_verified:
            return False

        snap = entry.last_verified
        if not snap.open_positions:
            return False

        exch = str(msg.get("e") or "").strip().upper()
        tk = str(msg.get("tk") or "").strip()
        if not exch or not tk:
            return False

        lp_dec = _parse_decimal_strict(msg.get("lp"))
        if lp_dec is None or lp_dec <= Decimal("0"):
            return False

        scrip_key = f"{exch}|{tk}"
        now_utc = datetime.now(timezone.utc)
        ft_raw = str(msg.get("ft") or "").strip() or None
        matched = False

        for pos in snap.open_positions.values():
            if pos.scrip_key == scrip_key:
                pos.lp = lp_dec
                pos.live_urmtom = pos.netqty * (lp_dec - pos.netavgprc) * pos.prcftr
                pos.last_tick_at = now_utc
                pos.last_feed_ft = ft_raw
                matched = True

        if not matched:
            return False

        entry.last_feed_at = now_utc
        entry.last_feed_mono = time.monotonic()
        entry.ws_status = "REAL-TIME"

        # Sum MTM across all open positions (using exact Decimal)
        total_mtm = Decimal("0")
        all_valid = True
        for pos in snap.open_positions.values():
            if pos.live_urmtom is None:
                all_valid = False
                break
            total_mtm += pos.live_urmtom

        if all_valid:
            live_open_mtm = _quantize_money(total_mtm)
            booked_pnl_val = snap.position_pnl if snap.position_pnl is not None else snap.realized_pnl
            entry.live_mtm_override = live_open_mtm
            if self.ws_hub:
                await self.ws_hub.broadcast({
                    "type": "financial_summary_updated",
                    "account_id": account_id,
                    "trader_id": entry.trader_id,
                    "uid": entry.uid,
                    "actid": entry.actid,
                    "mtm": {
                        "value": str(live_open_mtm),
                        "status": "LIVE",
                        "source": "zebu_websocket_touchline",
                        "last_feed_at": _fmt_iso_utc(now_utc),
                        "last_feed_ist": _fmt_ist_display(now_utc),
                    },
                    "pnl": {
                        "value": str(booked_pnl_val) if (snap.realized_valid and booked_pnl_val is not None) else None,
                        "status": "LIVE" if snap.realized_valid else "UNAVAILABLE",
                    },
                })
            return True
        return False

    async def on_ws_status_change(self, account_id: str, new_status: str, acc: Optional[dict] = None):
        """Tracks WebSocket status transitions and triggers fresh PositionBook reconciliation on reconnect."""
        entry = self.get_entry_for_account(account_id)
        if not entry:
            return
        prev = entry.ws_status
        entry.ws_status = new_status
        log.info("FinancialSummary WS status: account=%s %s -> %s", account_id, prev, new_status)

        if new_status == "REAL-TIME" and prev != "REAL-TIME" and acc is not None:
            self.schedule_reconcile(acc, reason="ws_reconnected")

    def schedule_reconcile(self, acc: dict, reason: str = "order_update"):
        """Schedules a single non-duplicate background PositionBook reconciliation for an account."""
        aid = acc["id"]
        existing = self._reconcile_tasks.get(aid)
        if existing and not existing.done():
            return

        async def _run():
            try:
                log.info("Scheduled PositionBook reconciliation: account=%s reason=%s", aid, reason)
                await self.get_summary(acc, user_id="reconciler", force=True)
            except Exception as e:
                log.debug("Reconcile error for %s: %s", aid, e)

        t = asyncio.create_task(_run())
        self._reconcile_tasks[aid] = t

    def _is_market_open_ist(self) -> bool:
        n = now_ist()
        if n.weekday() >= 5:
            return False
        return (9, 15) <= (n.hour, n.minute) <= (15, 30)

    def _is_feed_stale(self, entry: AccountFinancialCacheEntry) -> bool:
        snap = entry.last_verified
        if not snap or snap.open_positions_count == 0:
            return False
        if not self._is_market_open_ist():
            return False
        now_m = time.monotonic()
        # A freshly fetched PositionBook snapshot within FEED_STALE_SECONDS is fresh even while WS connects
        snapshot_fresh = entry.last_fetch_mono > 0 and (now_m - entry.last_fetch_mono) <= FEED_STALE_SECONDS
        if entry.ws_status != "REAL-TIME":
            return not snapshot_fresh
        if entry.last_feed_mono <= 0:
            return not snapshot_fresh
        return (now_m - entry.last_feed_mono) > FEED_STALE_SECONDS

    def _build_response(
        self,
        entry: AccountFinancialCacheEntry,
        acc: dict,
        overall_status: Optional[str],
        cache_hit: bool,
    ) -> dict:
        snap = entry.last_verified
        has_error = bool(entry.last_error)

        # Case 1: No verified snapshot exists at all
        if snap is None:
            st = overall_status or (
                "REAUTH_REQUIRED" if entry.last_error_type == "SESSION_EXPIRED" else "UNAVAILABLE"
            )
            return {
                "account_id": entry.account_id,
                "account_name": acc.get("name") or entry.account_id,
                "uid": entry.uid,
                "actid": entry.actid,
                "status": st,
                "source": "zebu_position_book",
                "read_only": True,
                "pnl": {
                    "label": "P&L",
                    "subtitle": "Realized P&L",
                    "value": None,
                    "status": st,
                    "error": entry.last_error or "Data unavailable",
                },
                "mtm": {
                    "label": "MTM",
                    "subtitle": "Unrealized Mark-to-Market",
                    "value": None,
                    "status": st,
                    "error": entry.last_error or "Data unavailable",
                    "live_feed": False,
                },
                "positions_count": 0,
                "open_positions_count": 0,
                "last_updated_at": None,
                "last_updated_ist": None,
                "last_feed_at": None,
                "last_feed_ist": None,
                "ws_status": entry.ws_status,
                "error": entry.last_error,
                "error_type": entry.last_error_type,
                "cache_hit": cache_hit,
                "latency_ms": entry.last_latency_ms,
            }

        # Case 2: We have a previously verified snapshot, but the latest refresh failed -> STALE
        if has_error or overall_status in ("REAUTH_REQUIRED", "NOT_LINKED"):
            stale_status = "REAUTH_REQUIRED" if (overall_status == "REAUTH_REQUIRED" or entry.last_error_type == "SESSION_EXPIRED") else "STALE"
            pnl_dec = snap.position_pnl if snap.position_pnl is not None else snap.realized_pnl
            mtm_dec = entry.live_mtm_override if entry.live_mtm_override is not None else (snap.total_mtm if snap.total_mtm is not None else snap.unrealized_mtm)
            pnl_val = str(pnl_dec) if snap.realized_valid and pnl_dec is not None else None
            mtm_val = str(mtm_dec) if snap.mtm_valid and mtm_dec is not None else None
            return {
                "account_id": entry.account_id,
                "account_name": acc.get("name") or entry.account_id,
                "uid": entry.uid,
                "actid": entry.actid,
                "status": stale_status,
                "source": "zebu_position_book_cached",
                "read_only": True,
                "pnl": {
                    "label": "Realized P&L",
                    "subtitle": "Booked",
                    "value": pnl_val,
                    "status": stale_status if pnl_val is not None else "UNAVAILABLE",
                    "error": entry.last_error or snap.realized_error,
                },
                "mtm": {
                    "label": "Unrealized MTM",
                    "subtitle": "Open exposure",
                    "value": mtm_val,
                    "status": stale_status if mtm_val is not None else "UNAVAILABLE",
                    "error": entry.last_error or snap.mtm_error,
                    "live_feed": False,
                },
                "positions_count": snap.positions_count,
                "open_positions_count": snap.open_positions_count,
                "last_updated_at": _fmt_iso_utc(snap.fetched_at),
                "last_updated_ist": _fmt_ist_display(snap.fetched_at),
                "last_feed_at": _fmt_iso_utc(entry.last_feed_at),
                "last_feed_ist": _fmt_ist_display(entry.last_feed_at),
                "ws_status": entry.ws_status,
                "error": entry.last_error,
                "error_type": entry.last_error_type,
                "cache_hit": cache_hit,
                "latency_ms": entry.last_latency_ms,
            }

        # Case 3: Verified fresh snapshot (independent card validation + live feed freshness check)
        feed_stale = self._is_feed_stale(entry)
        pnl_dec = snap.position_pnl if snap.position_pnl is not None else snap.realized_pnl
        base_mtm = snap.total_mtm if snap.total_mtm is not None else snap.unrealized_mtm
        mtm_dec = entry.live_mtm_override if (entry.live_mtm_override is not None and not feed_stale) else base_mtm
        mtm_source = "zebu_websocket_touchline" if (entry.live_mtm_override is not None and not feed_stale) else "zebu_position_book"

        pnl_status = "LIVE" if snap.realized_valid else "UNAVAILABLE"
        if not snap.mtm_valid:
            mtm_status = "UNAVAILABLE"
        elif feed_stale:
            mtm_status = "STALE"
        else:
            mtm_status = "LIVE"

        overall = "LIVE"
        if pnl_status == "UNAVAILABLE" and mtm_status == "UNAVAILABLE":
            overall = "UNAVAILABLE"
        elif mtm_status == "STALE":
            overall = "STALE"

        return {
            "account_id": entry.account_id,
            "account_name": acc.get("name") or entry.account_id,
            "uid": entry.uid,
            "actid": entry.actid,
            "status": overall,
            "source": mtm_source,
            "read_only": True,
            "pnl": {
                "label": "Realized P&L",
                "subtitle": "Booked",
                "value": str(pnl_dec) if snap.realized_valid and pnl_dec is not None else None,
                "status": pnl_status,
                "error": snap.realized_error,
            },
            "mtm": {
                "label": "Unrealized MTM",
                "subtitle": "Open exposure",
                "value": str(mtm_dec) if snap.mtm_valid and mtm_dec is not None else None,
                "status": mtm_status,
                "error": ("Live price feed stale — waiting for fresh tick" if feed_stale else snap.mtm_error),
                "live_feed": bool(snap.open_positions_count > 0 and entry.ws_status == "REAL-TIME" and not feed_stale),
            },
            "positions_count": snap.positions_count,
            "open_positions_count": snap.open_positions_count,
            "empty_confirmed": snap.empty_confirmed,
            "duplicate_rows_ignored": snap.duplicate_rows_ignored,
            "last_updated_at": _fmt_iso_utc(snap.fetched_at),
            "last_updated_ist": _fmt_ist_display(snap.fetched_at),
            "last_feed_at": _fmt_iso_utc(entry.last_feed_at),
            "last_feed_ist": _fmt_ist_display(entry.last_feed_at),
            "ws_status": entry.ws_status,
            "error": None,
            "error_type": None,
            "cache_hit": cache_hit,
            "latency_ms": entry.last_latency_ms,
        }

    def get_diagnostics(self) -> dict:
        """Returns safe server-side diagnostics (zero tokens or secrets)."""
        accounts = {}
        for key, e in self._cache.items():
            accounts[e.account_id] = {
                "cache_key": key,
                "uid": e.uid,
                "actid": e.actid,
                "ws_status": e.ws_status,
                "last_reconciled_at": _fmt_iso_utc(e.last_reconciled_at),
                "last_feed_at": _fmt_iso_utc(e.last_feed_at),
                "last_latency_ms": e.last_latency_ms,
                "cache_hits": e.cache_hits,
                "cache_misses": e.cache_misses,
                "last_error_type": e.last_error_type,
                "last_error": e.last_error,
                "positions_count": e.last_verified.positions_count if e.last_verified else 0,
                "open_positions_count": e.last_verified.open_positions_count if e.last_verified else 0,
            }
        return {"accounts": accounts}
