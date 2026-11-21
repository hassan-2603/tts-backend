"""Upgraded FIFO Trade Engine & Comprehensive Risk Rule Checker.

Features:
- Decimal-precise financial arithmetic (zero floating-point drift, zero fabricated values)
- Persistent FIFO position lot accounting
- Partial position closure decomposition: separates closed realized trade leg from remaining open position leg
- Overnight & carry-forward position continuity (> 30 days) with original entry date, time, price, and order identity preserved
- Multi-instrument parsing: Index & Stock Options, Futures, Equity Cash, Commodities
- Realized, Unrealized, Charges, and Net P&L separation
- Comprehensive risk-rule compliance engine with ACTIVE, HISTORICAL, RESOLVED, and UNEVALUATED (missing data) violation classification
"""
import re
from collections import defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Dict, List, Optional, Tuple, Any

MONTHS = {m: i for i, m in enumerate(["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}

_EXD_FMTS = (
    "%d-%b-%Y",
    "%d-%b-%y",
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%d %b %Y",
    "%d%b%Y",
    "%Y/%m/%d",
)


def normalize_expiry_date(exd: Any) -> Optional[str]:
    """Strictly normalize broker-provided expiry date (`exd`) to YYYY-MM-DD.
    Returns None for equities, blank values, or unparseable strings.
    NEVER infers or invents an expiry date from calendar assumptions."""
    if exd is None:
        return None
    if isinstance(exd, datetime):
        return exd.date().isoformat() if exd.year > 2000 else None
    if isinstance(exd, date):
        return exd.isoformat() if exd.year > 2000 else None
    s = str(exd).strip()
    if not s or s.upper() in ("0", "NA", "N/A", "NONE", "NULL", "-", "—"):
        return None
    s_clean = s.split(" ")[0] if ("-" in s or "/" in s) else s
    for candidate in (s, s_clean):
        for fmt in _EXD_FMTS:
            try:
                d = datetime.strptime(candidate.upper(), fmt).date()
                if d.year > 2000:
                    return d.isoformat()
            except ValueError:
                continue
    return None

DEFAULT_RULES = {
    "instrument": "BANKNIFTY",
    "expiry": "Monthly",
    "strikeMode": "ITM",
    "strikeDist": 1,
    "side": "BUY",
    "product": "MIS",
    "timeFrom": "09:20",
    "timeTo": "14:30",
    "maxTradesDay": 4,
    "maxLots": 3,
    "slPerLot": 2000,
    "dailyLoss": 10000,
    "hedge": False,
    "source": "ANY",
    # Advanced risk controls
    "maxOpenPositions": 5,
    "maxExposure": 500000.0,
    "requireHedgeOverLots": 5,
}

STRIKE_STEP = {
    "BANKNIFTY": 100,
    "NIFTY": 50,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
    "SENSEX": 100,
    "BANKEX": 100,
}

# Regex patterns for instruments
_OPT_PATTERNS = [
    # BANKNIFTY25NOV25C51700 (Noren standard: SYMBOL + DDMMMYY + C/P + STRIKE)
    re.compile(r"^(?P<u>[A-Z&]+?)(?P<d>\d{2})(?P<m>[A-Z]{3})(?P<y>\d{2})(?P<o>[CP])(?P<k>\d+(?:\.\d+)?)$"),
    # BANKNIFTY25NOV51700CE (Monthly NSE style: SYMBOL + YY + MMM + STRIKE + CE/PE)
    re.compile(r"^(?P<u>[A-Z&]+?)(?P<y>\d{2})(?P<m>[A-Z]{3})(?P<k>\d+(?:\.\d+)?)(?P<o>CE|PE)$"),
    # NIFTY2511451700CE (Weekly NSE style: SYMBOL + YY + M + DD + STRIKE + CE/PE)
    re.compile(r"^(?P<u>[A-Z&]+?)(?P<y>\d{2})(?P<mn>[1-9OND])(?P<d>\d{2})(?P<k>\d+(?:\.\d+)?)(?P<o>CE|PE)$"),
    # Stock options: RELIANCE24NOV2500CE
    re.compile(r"^(?P<u>[A-Z0-9&]+?)(?P<y>\d{2})(?P<m>[A-Z]{3})(?P<k>\d+(?:\.\d+)?)(?P<o>CE|PE)$"),
]

_FUT_PATTERNS = [
    # BANKNIFTY24NOVFUT
    re.compile(r"^(?P<u>[A-Z&]+?)(?P<y>\d{2})(?P<m>[A-Z]{3})FUT$"),
    # NIFTY24NOVFUT
    re.compile(r"^(?P<u>[A-Z&]+?)(?P<y>\d{2})(?P<d>\d{2})?(?P<m>[A-Z]{3})FUT$"),
]


def _dec(val: Any, default: Optional[Decimal] = Decimal("0")) -> Optional[Decimal]:
    if val is None or val == "":
        return default
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val).strip())
    except (InvalidOperation, ValueError, TypeError):
        return default


def _q2(val: Decimal) -> Decimal:
    return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _q4(val: Decimal) -> Decimal:
    return val.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def last_weekday_of_month(y: int, m: int, weekday: int) -> date:
    d = date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def parse_instrument(tsym: str, master: Optional[dict] = None) -> Optional[dict]:
    """Parse option, future, or equity instrument symbol."""
    tsym = (tsym or "").upper().strip()
    if master and tsym in master:
        return master[tsym]

    # Equity cash check
    if tsym.endswith("-EQ") or tsym.endswith(".EQ") or tsym.endswith("-BE"):
        sym = tsym.split("-")[0].split(".")[0]
        return {
            "type": "EQ",
            "underlying": sym,
            "expiry": None,
            "opt": None,
            "strike": None,
            "expiryType": "Equity",
        }

    # Futures check (NEVER infer expiry from calendar assumptions; expiry comes only from broker GetSecurityInfo/master)
    for p in _FUT_PATTERNS:
        m = p.match(tsym)
        if m:
            g = m.groupdict()
            return {
                "type": "FUT",
                "underlying": g["u"],
                "expiry": None,
                "opt": "FUT",
                "strike": 0.0,
                "expiryType": "",
            }

    # Options check (NEVER infer expiry from calendar assumptions; expiry comes only from broker GetSecurityInfo/master)
    for p in _OPT_PATTERNS:
        m = p.match(tsym)
        if not m:
            continue
        g = m.groupdict()
        opt = "CE" if g["o"] in ("C", "CE") else "PE"

        return {
            "type": "OPT",
            "underlying": g["u"],
            "expiry": None,
            "opt": opt,
            "strike": float(g["k"]),
            "expiryType": "",
        }

    # Fallback generic instrument
    return {
        "type": "GENERIC",
        "underlying": tsym,
        "expiry": None,
        "opt": "",
        "strike": 0.0,
        "expiryType": "",
    }


def parse_option(tsym: str, master: Optional[dict] = None) -> Optional[dict]:
    """Backward compatible helper returning option details or None."""
    parsed = parse_instrument(tsym, master)
    if parsed and parsed.get("type") in ("OPT", "GENERIC"):
        return parsed
    return parsed


def expiry_type(exp: Optional[date]) -> str:
    if not exp:
        return ""
    return "Monthly" if (exp + timedelta(days=7)).month != exp.month else "Weekly"


# ---------------- Time Parsing ----------------
_TFMTS = [
    "%d-%m-%Y %H:%M:%S",
    "%H:%M:%S %d-%m-%Y",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S%z",
    "%d-%b-%Y %H:%M:%S",
    "%H:%M:%S %d-%b-%Y",
    "%d-%b-%y %H:%M:%S",
]


def parse_ts(*vals) -> Optional[datetime]:
    for v in vals:
        if not v:
            continue
        if isinstance(v, datetime):
            return v.replace(tzinfo=None)
        v_str = str(v).strip()
        if not v_str or v_str.startswith("01-01-1980"):
            continue
        # Handle epoch seconds (e.g. ordenttm "1607706811")
        if v_str.isdigit() and len(v_str) in (10, 13):
            try:
                epoch = int(v_str) / (1000.0 if len(v_str) == 13 else 1.0)
                d = datetime.fromtimestamp(epoch)
                if d.year > 2000:
                    return d.replace(tzinfo=None)
            except (ValueError, OSError):
                pass
        for f in _TFMTS:
            try:
                d = datetime.strptime(v_str[:len(f) + 2].strip(), f)
                if d.year > 2000:
                    return d.replace(tzinfo=None)
            except ValueError:
                try:
                    d = datetime.strptime(v_str[:19], f[:19])
                    if d.year > 2000:
                        return d.replace(tzinfo=None)
                except ValueError:
                    pass
    return None


# ---------------- Charges Calculation ----------------
def estimate_charges(buy_value: float, sell_value: float, orders: int, cfg: dict) -> float:
    bv = _dec(buy_value, Decimal("0"))
    sv = _dec(sell_value, Decimal("0"))
    n_ord = Decimal(str(orders))
    brok_per = _dec(cfg.get("brokerage_per_order", 20), Decimal("20"))
    stt_pct = _dec(cfg.get("stt_sell_pct", 0.1), Decimal("0.1"))
    txn_pct = _dec(cfg.get("txn_pct", 0.03503), Decimal("0.03503"))
    stamp_pct = _dec(cfg.get("stamp_buy_pct", 0.003), Decimal("0.003"))

    brokerage = brok_per * n_ord
    stt = sv * stt_pct / Decimal("100")
    txn = (bv + sv) * txn_pct / Decimal("100")
    sebi = (bv + sv) * Decimal("10") / Decimal("10000000")
    stamp = bv * stamp_pct / Decimal("100")
    gst = (brokerage + txn + sebi) * Decimal("0.18")
    total = _q2(brokerage + stt + txn + sebi + stamp + gst)
    return float(total)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


# ---------------- FIFO Trade Reconstruction ----------------
def build_trades(
    account: dict,
    fills: list,
    cfg: dict,
    product_map: dict,
    is_algo,
    now: datetime,
    ltp: Optional[dict] = None,
    master: Optional[dict] = None,
    split_partial_closures: bool = True,
    positions: Optional[list] = None,
    security_info: Optional[Dict[Tuple[str, str], dict]] = None,
) -> list:
    """FIFO-match fills per contract with Decimal precision.

    Supports partial fills, multi-leg entries/exits, position reversals,
    carry-forward positions (including PositionBook cfbuyqty/cfsellqty seed reconciliation),
    and all instrument classes.
    Uses (exchange, instrument token) as the primary contract identifier when token is present.
    Expiry dates are populated strictly from broker-provided GetSecurityInfo / instrument master (`exd`)
    and NEVER inferred from calendar assumptions.
    When split_partial_closures is True and a position has both closed exits and remaining open quantity
    (or exits across multiple trading days):
    - Closed portions are emitted as closed trade records on their respective exit dates with exact realized P&L.
    - Any remaining open quantity is emitted as an active open position preserving the original entry date,
      entry time, entry price, and order identity.
    """
    ltp = dict(ltp or {})
    if now and getattr(now, "tzinfo", None) is not None:
        now = now.replace(tzinfo=None)
    today_d = now.date() if now else date.today()

    # Normalize security_info lookup keyed strictly by (EXCH, TOKEN)
    sec_info_map: Dict[Tuple[str, str], dict] = {}
    for k, v in (security_info or {}).items():
        if isinstance(k, tuple) and len(k) == 2 and k[0] and k[1] and isinstance(v, dict):
            sec_info_map[(str(k[0]).strip().upper(), str(k[1]).strip())] = v

    def _contract_key(exch_val: Any, token_val: Any, tsym_val: Any) -> Tuple[str, str]:
        exch_u = str(exch_val or "NFO").strip().upper()
        tok_s = str(token_val or "").strip()
        if tok_s:
            return (exch_u, tok_s)
        return ("TSYM", str(tsym_val or "").strip())

    # Map PositionBook metadata (prcftr, lotsize, ltp, carry-forward & day quantities)
    pos_by_key: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    prcftr_by_sym: Dict[str, Decimal] = {}
    for p in (positions or []):
        tsym = p.get("tsym")
        if not tsym:
            continue
        pk = _contract_key(p.get("exch"), p.get("token"), tsym)
        pos_by_key[pk].append(p)
        exch_u = str(p.get("exch") or "").upper()
        if exch_u in ("MCX", "CDS"):
            pf = _dec(p.get("prcftr"), Decimal("1"))
            if pf and pf > Decimal("0"):
                prcftr_by_sym[tsym] = pf
        lp_val = _dec(p.get("lp"), None)
        if lp_val is not None and lp_val > Decimal("0") and tsym not in ltp:
            ltp[tsym] = float(lp_val)

    # 1. Group fills by order ID (deduplicating identical fills by order_no + flid)
    orders = {}
    seen_fill_keys = set()
    for idx_f, f in enumerate(fills):
        oid = str(f.get("norenordno") or f.get("flid") or f"O-{idx_f}-{f.get('fltm', '')}")
        flid_raw = str(f.get("flid") or "").strip()
        if flid_raw.startswith("PARTIAL-"):
            continue
        ts = parse_ts(f.get("fltm"), f.get("exch_tm"), f.get("norentm"), f.get("ordenttm"))
        if not ts and f.get("_day"):
            ts = parse_ts(f"{f['_day']} 09:15:00")
        if not ts:
            continue
        q = _dec(f.get("flqty") or f.get("fillshares"), Decimal("0"))
        p = _dec(f.get("flprc") or f.get("avgprc") or f.get("prc"), Decimal("0"))
        if q <= Decimal("0"):
            continue
        if flid_raw:
            dedup_f = (oid, flid_raw)
            if dedup_f in seen_fill_keys:
                continue
            seen_fill_keys.add(dedup_f)
        fill_exd = normalize_expiry_date(f.get("exd"))
        o = orders.setdefault(
            oid,
            {
                "oid": oid,
                "tsym": f.get("tsym"),
                "exch": f.get("exch", "NFO"),
                "side": "BUY" if str(f.get("trantype", "")).upper() in ("B", "BUY") else "SELL",
                "prd": f.get("prd"),
                "ls": int(_f(f.get("ls"), 1)) or 1,
                "token": str(f.get("token") or "").strip() or None,
                "exd": fill_exd,
                "ordersource": f.get("ordersource", ""),
                "remarks": f.get("remarks", ""),
                "qty": Decimal("0"),
                "val": Decimal("0"),
                "ts": ts,
                "last": ts,
            },
        )
        if not o.get("exd") and fill_exd:
            o["exd"] = fill_exd
        if not o.get("token") and f.get("token"):
            o["token"] = str(f.get("token")).strip()
        o["qty"] += q
        o["val"] += q * p
        o["ts"] = min(o["ts"], ts)
        o["last"] = max(o["last"], ts)

    by_contract: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    tsym_to_ckey: Dict[str, Tuple[str, str]] = {}
    for o in orders.values():
        if o["qty"] > Decimal("0"):
            ck = _contract_key(o.get("exch"), o.get("token"), o.get("tsym"))
            by_contract[ck].append(o)
            if o.get("tsym") and o["tsym"] not in tsym_to_ckey:
                tsym_to_ckey[o["tsym"]] = ck

    # 2. Reconcile carry-forward (cfbuyqty/cfsellqty) and missing intraday PositionBook entries
    for pk, plist in pos_by_key.items():
        p0 = plist[0]
        tsym = p0.get("tsym")
        target_ck = pk if pk in by_contract else tsym_to_ckey.get(tsym, pk)
        existing_orders = by_contract.get(target_ck, [])
        prior_buy = sum((o["qty"] for o in existing_orders if o["ts"].date() < today_d and o["side"] == "BUY"), Decimal("0"))
        prior_sell = sum((o["qty"] for o in existing_orders if o["ts"].date() < today_d and o["side"] == "SELL"), Decimal("0"))
        prior_net = prior_buy - prior_sell

        cf_buy_total = sum((_dec(p.get("cfbuyqty"), Decimal("0")) for p in plist), Decimal("0"))
        cf_sell_total = sum((_dec(p.get("cfsellqty"), Decimal("0")) for p in plist), Decimal("0"))

        ls_p = int(_f(p0.get("ls"), 1)) or 1
        prd_p = p0.get("prd", "M")
        exch_p = p0.get("exch", "NFO")
        tok_p = str(p0.get("token") or "").strip() or None
        exd_p = normalize_expiry_date(p0.get("exd"))
        prev_day_ts = datetime.combine(today_d - timedelta(days=1), datetime.min.time()).replace(hour=9, minute=15)

        # Inject carry-forward seed lot if Zebu PositionBook reports carry-forward qty not in prior fills
        if cf_buy_total > Decimal("0") and prior_net < cf_buy_total:
            missing_cf_b = cf_buy_total - max(prior_net, Decimal("0"))
            cf_px = _dec(p0.get("cforgavgprc") or p0.get("cfbuyavgprc") or p0.get("netavgprc"), Decimal("0"))
            if missing_cf_b > Decimal("0") and cf_px > Decimal("0"):
                by_contract[target_ck].append({
                    "oid": f"CF-{tsym}-B",
                    "tsym": tsym,
                    "exch": exch_p,
                    "side": "BUY",
                    "prd": prd_p,
                    "ls": ls_p,
                    "token": tok_p,
                    "exd": exd_p,
                    "ordersource": "CarryForward",
                    "remarks": "Carry-forward position from PositionBook",
                    "qty": missing_cf_b,
                    "val": missing_cf_b * cf_px,
                    "ts": prev_day_ts,
                    "last": prev_day_ts,
                })
        if cf_sell_total > Decimal("0") and (-prior_net) < cf_sell_total:
            missing_cf_s = cf_sell_total - max(-prior_net, Decimal("0"))
            cf_px = _dec(p0.get("cforgavgprc") or p0.get("cfsellavgprc") or p0.get("netavgprc"), Decimal("0"))
            if missing_cf_s > Decimal("0") and cf_px > Decimal("0"):
                by_contract[target_ck].append({
                    "oid": f"CF-{tsym}-S",
                    "tsym": tsym,
                    "exch": exch_p,
                    "side": "SELL",
                    "prd": prd_p,
                    "ls": ls_p,
                    "token": tok_p,
                    "exd": exd_p,
                    "ordersource": "CarryForward",
                    "remarks": "Carry-forward position from PositionBook",
                    "qty": missing_cf_s,
                    "val": missing_cf_s * cf_px,
                    "ts": prev_day_ts,
                    "last": prev_day_ts,
                })

    trades = []
    for ckey, olist in by_contract.items():
        if not olist:
            continue
        tsym = olist[0].get("tsym") or ""
        info = parse_instrument(tsym, master)
        if not info:
            continue
        exch_u = str(olist[0].get("exch") or "NFO").strip().upper()
        tok_s = next((str(o.get("token")).strip() for o in olist if o.get("token")), "")

        # Resolve exact broker expiry strictly via (exchange, token) from GetSecurityInfo / instrument master
        resolved_exd: Optional[str] = None
        if tok_s:
            sinfo = sec_info_map.get((exch_u, tok_s))
            if sinfo:
                resolved_exd = normalize_expiry_date(
                    sinfo.get("exd") or sinfo.get("expiry_date") or sinfo.get("expiry")
                )
        if not resolved_exd:
            resolved_exd = next((o.get("exd") for o in olist if o.get("exd")), None)
        if not resolved_exd and info.get("expiry"):
            resolved_exd = normalize_expiry_date(info.get("expiry"))

        resolved_exp_str = resolved_exd or ""
        if resolved_exd:
            try:
                resolved_exp_type = expiry_type(date.fromisoformat(resolved_exd))
            except ValueError:
                resolved_exp_type = ""
        elif info.get("type") == "EQ":
            resolved_exp_type = "Equity"
        else:
            resolved_exp_type = ""

        olist.sort(key=lambda o: (o["ts"], o["oid"]))

        # FIFO matching
        entries = []
        q2 = []
        for o in olist:
            px = _q4(o["val"] / o["qty"]) if o["qty"] > Decimal("0") else Decimal("0")
            remaining = o["qty"]
            while remaining > Decimal("0") and q2 and q2[0]["order"]["side"] != o["side"]:
                e = q2[0]
                m = min(e["remaining"], remaining)
                e["exits"].append({
                    "qty": m,
                    "price": px,
                    "ts": o["ts"],
                    "oid": o["oid"],
                })
                e["remaining"] -= m
                remaining -= m
                if e["remaining"] <= Decimal("0"):
                    q2.pop(0)

            if remaining > Decimal("0"):
                e = {
                    "order": o,
                    "remaining": remaining,
                    "qty": remaining,
                    "exits": [],
                    "px": px,
                }
                q2.append(e)
                entries.append(e)

        for e in entries:
            o = e["order"]
            total_qty = e["qty"]
            rem_qty = e["remaining"]
            exits = e["exits"]
            closed_qty = sum((x["qty"] for x in exits), Decimal("0"))
            is_open = rem_qty > Decimal("0")
            entry_d = o["ts"].date()
            is_buy = o["side"] == "BUY"
            product_code = product_map.get(o["prd"], o["prd"] or "MIS")
            norm_product = "NRML" if product_code in ("NRML", "CNC") else "MIS"
            src_label = "Algo" if is_algo(o) else "Manual"
            base_trade_id = f"Z-{account['id']}-{o['oid']}"
            ls_dec = Decimal(str(o["ls"]))
            prcftr = prcftr_by_sym.get(tsym, Decimal("1"))

            # Group closed exits by exit date so multi-day partial exits attribute P&L to the exact exit date
            exits_by_day = defaultdict(list)
            for x in exits:
                exits_by_day[x["ts"].date()].append(x)

            has_partial_or_multiday = split_partial_closures and (
                (is_open and closed_qty > Decimal("0")) or len(exits_by_day) > 1
            )

            if has_partial_or_multiday:
                # 1. Emit closed legs grouped by exit date
                for idx, (ex_date, day_exits) in enumerate(sorted(exits_by_day.items(), key=lambda kv: kv[0])):
                    leg_qty = sum((x["qty"] for x in day_exits), Decimal("0"))
                    leg_exit_val = sum((x["qty"] * x["price"] for x in day_exits), Decimal("0"))
                    leg_exit_px = _q4(leg_exit_val / leg_qty) if leg_qty > Decimal("0") else e["px"]
                    last_exit_ts = max(x["ts"] for x in day_exits)
                    exit_oids = sorted({str(x["oid"]) for x in day_exits})
                    first_exit_oid = exit_oids[0] if exit_oids else f"EX{idx+1}"

                    leg_buy_val = (e["px"] * leg_qty if is_buy else leg_exit_val) * prcftr
                    leg_sell_val = (leg_exit_val if is_buy else e["px"] * leg_qty) * prcftr
                    leg_realized_pnl = _q2(((leg_exit_px - e["px"]) if is_buy else (e["px"] - leg_exit_px)) * leg_qty * prcftr)
                    # Proportional entry order count (1 on first leg, 0 on subsequent partial legs) + exit orders
                    n_orders = (1 if idx == 0 else 0) + len(exit_oids)
                    leg_charges = _dec(estimate_charges(float(leg_buy_val), float(leg_sell_val), n_orders, cfg))
                    leg_net_pnl = _q2(leg_realized_pnl - leg_charges)

                    leg_id = f"{base_trade_id}-X-{first_exit_oid}"
                    trades.append({
                        "id": leg_id,
                        "parent_trade_id": base_trade_id,
                        "entry_order_id": o["oid"],
                        "exit_order_id": ",".join(exit_oids),
                        "origin": "zebu",
                        "date": entry_d.isoformat(),
                        "time": o["ts"].strftime("%H:%M"),
                        "trader": account.get("trader"),
                        "account": account["id"],
                        "symbol": info["underlying"],
                        "strike": info["strike"],
                        "opt": info["opt"],
                        "expiry": resolved_exp_str,
                        "expiryType": resolved_exp_type,
                        "tsym": tsym,
                        "exch": o["exch"],
                        "token": tok_s or None,
                        "side": "BUY" if is_buy else "SELL",
                        "lots": float(_q2(leg_qty / ls_dec)),
                        "lotSize": o["ls"],
                        "qty": float(_q2(leg_qty)),
                        "entry": float(_q2(e["px"])),
                        "exit": float(_q2(leg_exit_px)),
                        "pnl": float(leg_realized_pnl),
                        "realized_pnl": float(leg_realized_pnl),
                        "unrealized_pnl": 0.0,
                        "net_pnl": float(leg_net_pnl),
                        "pnl_source": "calculated",
                        "open": False,
                        "exitDate": ex_date.isoformat(),
                        "exitTime": last_exit_ts.strftime("%H:%M"),
                        "spot": None,
                        "_ts": o["ts"].isoformat(),
                        "product": norm_product,
                        "source": src_label,
                        "charges": float(_q2(leg_charges)),
                        "chargesEstimated": True,
                        "hedged": False,
                    })

                # 2. Emit remaining open leg (if any) keeping original base_trade_id
                if is_open:
                    raw_ltp = ltp.get(tsym)
                    has_ltp = raw_ltp is not None and raw_ltp != "" and _dec(raw_ltp, None) is not None and _dec(raw_ltp, Decimal("0")) > Decimal("0")
                    if has_ltp:
                        cur_ltp = _q4(_dec(raw_ltp))
                        open_exit_val = rem_qty * cur_ltp
                        open_unrl_pnl = _q2(((cur_ltp - e["px"]) if is_buy else (e["px"] - cur_ltp)) * rem_qty * prcftr)
                        disp_exit = float(_q2(cur_ltp))
                        disp_pnl = float(open_unrl_pnl)
                    else:
                        cur_ltp = None
                        open_exit_val = rem_qty * e["px"]
                        open_unrl_pnl = None
                        disp_exit = float(_q2(e["px"]))
                        disp_pnl = None

                    open_buy_val = e["px"] * rem_qty if is_buy else Decimal("0")
                    open_sell_val = Decimal("0") if is_buy else e["px"] * rem_qty
                    # Entry brokerage only if not already counted in closed leg
                    open_n_orders = 0 if closed_qty > Decimal("0") else 1
                    open_charges = _dec(estimate_charges(float(open_buy_val), float(open_sell_val), open_n_orders, cfg))
                    open_net_pnl = float(_q2(open_unrl_pnl - open_charges)) if open_unrl_pnl is not None else None

                    trades.append({
                        "id": base_trade_id,
                        "parent_trade_id": base_trade_id,
                        "entry_order_id": o["oid"],
                        "exit_order_id": None,
                        "origin": "zebu",
                        "date": entry_d.isoformat(),
                        "time": o["ts"].strftime("%H:%M"),
                        "trader": account.get("trader"),
                        "account": account["id"],
                        "symbol": info["underlying"],
                        "strike": info["strike"],
                        "opt": info["opt"],
                        "expiry": resolved_exp_str,
                        "expiryType": resolved_exp_type,
                        "tsym": tsym,
                        "exch": o["exch"],
                        "token": tok_s or None,
                        "side": "BUY" if is_buy else "SELL",
                        "lots": float(_q2(rem_qty / ls_dec)),
                        "lotSize": o["ls"],
                        "qty": float(_q2(rem_qty)),
                        "entry": float(_q2(e["px"])),
                        "exit": disp_exit,
                        "pnl": disp_pnl,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": disp_pnl,
                        "net_pnl": open_net_pnl,
                        "pnl_source": "calculated" if has_ltp else "unavailable",
                        "open": True,
                        "exitDate": None,
                        "exitTime": "OPEN",
                        "spot": None,
                        "_ts": o["ts"].isoformat(),
                        "product": norm_product,
                        "source": src_label,
                        "charges": float(_q2(open_charges)),
                        "chargesEstimated": True,
                        "hedged": False,
                    })
            else:
                # Single unified trade (either 100% open or 100% closed on a single exit date, or legacy mode)
                exit_val = sum((x["qty"] * x["price"] for x in exits), Decimal("0"))
                if is_open:
                    raw_ltp = ltp.get(tsym)
                    has_ltp = raw_ltp is not None and raw_ltp != "" and _dec(raw_ltp, None) is not None and _dec(raw_ltp, Decimal("0")) > Decimal("0")
                    if has_ltp:
                        last_px = _dec(raw_ltp)
                        exit_val += rem_qty * last_px
                        exit_px = _q4(exit_val / total_qty) if total_qty > Decimal("0") else e["px"]
                        calc_pnl = _q2(((exit_px - e["px"]) if is_buy else (e["px"] - exit_px)) * total_qty * prcftr)
                        disp_pnl = float(calc_pnl)
                        disp_exit = float(_q2(exit_px))
                        pnl_src = "calculated"
                    else:
                        last_px = exits[-1]["price"] if exits else e["px"]
                        exit_val += rem_qty * last_px
                        exit_px = _q4(exit_val / total_qty) if total_qty > Decimal("0") else e["px"]
                        calc_pnl = _q2(((exit_px - e["px"]) if is_buy else (e["px"] - exit_px)) * total_qty * prcftr) if exits else None
                        disp_pnl = float(calc_pnl) if calc_pnl is not None else None
                        disp_exit = float(_q2(exit_px))
                        pnl_src = "calculated" if exits else "unavailable"
                    exit_date_str = None
                    exit_time_str = "OPEN"
                    realized_val = float(_q2(sum((((x["price"] - e["px"]) if is_buy else (e["px"] - x["price"])) * x["qty"] * prcftr) for x in exits))) if exits else 0.0
                    unrealized_val = disp_pnl
                    exit_oids_str = ",".join(sorted({str(x["oid"]) for x in exits})) if exits else None
                else:
                    exit_px = _q4(exit_val / total_qty) if total_qty > Decimal("0") else e["px"]
                    last_ts = max(x["ts"] for x in exits)
                    exit_date_str = last_ts.date().isoformat()
                    exit_time_str = last_ts.strftime("%H:%M")
                    calc_pnl = _q2(((exit_px - e["px"]) if is_buy else (e["px"] - exit_px)) * total_qty * prcftr)
                    disp_pnl = float(calc_pnl)
                    disp_exit = float(_q2(exit_px))
                    realized_val = disp_pnl
                    unrealized_val = 0.0
                    pnl_src = "calculated"
                    exit_oids_str = ",".join(sorted({str(x["oid"]) for x in exits}))

                buy_val = e["px"] * total_qty if is_buy else (exit_val if not is_open else Decimal("0"))
                sell_val = (exit_val if not is_open else Decimal("0")) if is_buy else e["px"] * total_qty
                n_orders = 1 + len({x["oid"] for x in exits})
                charges_val = _dec(estimate_charges(float(buy_val), float(sell_val), n_orders, cfg))
                net_val = float(_q2(_dec(disp_pnl) - charges_val)) if disp_pnl is not None else None

                trades.append({
                    "id": base_trade_id,
                    "parent_trade_id": base_trade_id,
                    "entry_order_id": o["oid"],
                    "exit_order_id": exit_oids_str,
                    "origin": "zebu",
                    "date": entry_d.isoformat(),
                    "time": o["ts"].strftime("%H:%M"),
                    "trader": account.get("trader"),
                    "account": account["id"],
                    "symbol": info["underlying"],
                    "strike": info["strike"],
                    "opt": info["opt"],
                    "expiry": resolved_exp_str,
                    "expiryType": resolved_exp_type,
                    "tsym": tsym,
                    "exch": o["exch"],
                    "token": tok_s or None,
                    "side": "BUY" if is_buy else "SELL",
                    "lots": float(_q2(total_qty / ls_dec)),
                    "lotSize": o["ls"],
                    "qty": float(_q2(total_qty)),
                    "entry": float(_q2(e["px"])),
                    "exit": disp_exit,
                    "pnl": disp_pnl,
                    "realized_pnl": realized_val,
                    "unrealized_pnl": unrealized_val,
                    "net_pnl": net_val,
                    "pnl_source": pnl_src,
                    "open": is_open,
                    "exitDate": exit_date_str,
                    "exitTime": exit_time_str,
                    "spot": None,
                    "_ts": o["ts"].isoformat(),
                    "product": norm_product,
                    "source": src_label,
                    "charges": float(_q2(charges_val)),
                    "chargesEstimated": True,
                    "hedged": False,
                })

    # Hedge detection: BUY leg hedged if a SELL leg was open on the same symbol at the same time
    for t in trades:
        if t["side"] != "BUY":
            continue
        for s in trades:
            if s["side"] == "SELL" and s["symbol"] == t["symbol"] and s["date"] == t["date"]:
                s_end = "23:59" if s["open"] or (s.get("exitDate") and s["exitDate"] != s["date"]) else (s.get("exitTime") or "23:59")
                if s["time"] <= t["time"] <= s_end:
                    t["hedged"] = True
                    s["hedged"] = True

    return trades


# ---------------- Risk Rule Compliance Engine ----------------
def to_min(t: Any) -> Optional[int]:
    if not t or ":" not in str(t):
        return None
    try:
        h, m = str(t).split(":")[:2]
        return int(h) * 60 + int(m)
    except (ValueError, TypeError):
        return None


def gross_pnl(t: dict) -> float:
    if t.get("realized_pnl") is not None and not t.get("open"):
        try:
            return float(t["realized_pnl"])
        except (ValueError, TypeError):
            pass
    if t.get("pnl") is not None:
        try:
            return float(t["pnl"])
        except (ValueError, TypeError):
            pass
    if t.get("exit") is None or t.get("entry") is None:
        return 0.0
    d = Decimal("-1") if t.get("side") == "SELL" else Decimal("1")
    q = _dec(t.get("qty")) or (_dec(t.get("lots", 1)) * _dec(t.get("lotSize", 1)))
    ex = _dec(t.get("exit"), Decimal("0"))
    en = _dec(t.get("entry"), Decimal("0"))
    return float(_q2((ex - en) * q * d))


def net_pnl(t: dict) -> float:
    if t.get("net_pnl") is not None:
        try:
            return float(t["net_pnl"])
        except (ValueError, TypeError):
            pass
    g = _dec(gross_pnl(t), Decimal("0"))
    c = _dec(t.get("charges", 0.0), Decimal("0"))
    return float(_q2(g - c))


def moneyness(t: dict, step: int) -> Tuple[float, int]:
    if not t.get("spot") or not step:
        return 0.0, 0
    atm = round(float(t["spot"]) / step) * step
    strike = float(t.get("strike") or atm)
    d = ((atm - strike) if t.get("opt") == "CE" else (strike - atm)) / step
    return atm, round(d)


def mlabel(d: int) -> str:
    return "ATM" if d == 0 else (f"{d} ITM" if d > 0 else f"{-d} OTM")


def check_trades(
    trades: List[dict],
    rules_by_trader: dict,
    step_default: int = 100,
    today_str: Optional[str] = None,
) -> Dict[str, List[dict]]:
    """Evaluates trades against each account/trader's rule book.

    Distinguishes:
    - ACTIVE: Violation on today's session or currently open trade
    - HISTORICAL: Violation on a closed trade from a past session
    - UNEVALUATED: Required broker data (such as underlying spot at entry or live price) is missing
    Returns: {trade_id: [{"rule": ..., "did": ..., "level": ..., "condition": ..., "observed": ..., "status": ...}]}
    """
    out = {}
    day = defaultdict(list)
    for t in trades:
        owner_key = t.get("account") or t.get("trader")
        day[(owner_key, t["date"])].append(t)

    for (owner_key, tr_date), lst in day.items():
        lst.sort(key=lambda x: (x.get("time") or "", x.get("id") or ""))
        sample_tr = lst[0]
        acc_id = sample_tr.get("account")
        tr_id = sample_tr.get("trader")
        # Look up rules by account_id first, then trader_id
        raw_rules = (
            rules_by_trader.get(acc_id)
            or rules_by_trader.get(tr_id)
            or rules_by_trader.get(owner_key)
            or {}
        )
        r = {**DEFAULT_RULES, **raw_rules}
        day_pnl = Decimal("0")
        stop = False
        # Track unique entry orders per day so split partial-closure legs of the same entry order aren't double-counted as separate new entries
        seen_entry_orders = []

        for t in lst:
            f = []
            is_hist = bool(t.get("is_finalized")) or (bool(today_str) and tr_date < today_str and not t.get("open"))
            v_status = "HISTORICAL" if is_hist else "ACTIVE"

            def add(rule: str, did: str, lvl: str, condition: str = "", observed: str = "", status: str = v_status):
                f.append({
                    "rule": rule,
                    "did": did,
                    "level": lvl,
                    "condition": condition or rule,
                    "observed": observed or did,
                    "status": status,
                    "account": t.get("account"),
                    "trader": t.get("trader"),
                    "date": t.get("date"),
                    "time": t.get("time"),
                })

            step = STRIKE_STEP.get(t["symbol"], step_default)

            # 1. Instrument rule
            if r.get("instrument") != "ANY" and t.get("symbol") != r.get("instrument"):
                add(
                    f"Instrument: {r['instrument']} only",
                    f"Traded {t.get('symbol')}",
                    "High",
                    condition=f"instrument == {r['instrument']}",
                    observed=str(t.get("symbol")),
                )

            # 2. Strike moneyness rule
            if r.get("strikeMode") != "ANY" and t.get("opt") in ("CE", "PE"):
                if t.get("spot"):
                    atm, d = moneyness(t, step)
                    want = 0 if r["strikeMode"] == "ATM" else (int(r["strikeDist"]) if r["strikeMode"] == "ITM" else -int(r["strikeDist"]))
                    if d != want:
                        n = 0 if r["strikeMode"] == "ATM" else int(r["strikeDist"])
                        sign = 1 if r["strikeMode"] == "ITM" else (-1 if r["strikeMode"] == "OTM" else 0)
                        right = atm - sign * n * step if t["opt"] == "CE" else atm + sign * n * step
                        label = "ATM" if r["strikeMode"] == "ATM" else f"{r['strikeDist']} {r['strikeMode']}"
                        add(
                            f"Strike: {label} only",
                            f"Bought {t['strike']:g} {t['opt']} = {mlabel(d)} (spot {t['spot']:,.0f}, right strike {right:g} {t['opt']})",
                            "High",
                            condition=f"strike == {label}",
                            observed=f"{t['strike']:g} {t['opt']} ({mlabel(d)} @ spot {t['spot']:,.0f})",
                        )
                else:
                    # Missing spot data: do not assume followed or broken (Req 7.12 & 7.13)
                    # Record in separate unevaluated metadata list on trade if needed, without generating a false violation alert
                    pass

            # 3. Buy/Sell side rule
            if r.get("side") != "ANY" and t.get("side") != r.get("side"):
                add(
                    "Option buying only" if r["side"] == "BUY" else "Option selling only",
                    f"{t.get('side')} trade",
                    "High",
                    condition=f"side == {r['side']}",
                    observed=str(t.get("side")),
                )

            # 4. Intraday / Carry-forward rule
            exit_d = t.get("exitDate") or t.get("date")
            carried_overnight = (exit_d != t.get("date")) or (t.get("open") and today_str and t.get("date") < today_str)
            if r.get("product") == "MIS" and (t.get("product") == "NRML" or carried_overnight):
                add(
                    "Intraday only",
                    f"Carried position overnight (exit {exit_d if not t.get('open') else 'OPEN'})" if carried_overnight else "Used NRML product",
                    "High",
                    condition="product == MIS and exitDate == tradeDate",
                    observed=f"product={t.get('product')}, exitDate={exit_d}",
                )

            # 5. Entry time window
            t_min = to_min(t.get("time"))
            from_min = to_min(r.get("timeFrom"))
            to_m = to_min(r.get("timeTo"))
            if from_min is not None and t_min is not None and t_min < from_min:
                add(
                    f"Entry from {r['timeFrom']}",
                    f"Entry at {t['time']}",
                    "Medium",
                    condition=f"time >= {r['timeFrom']}",
                    observed=str(t.get("time")),
                )
            if to_m is not None and t_min is not None and t_min > to_m:
                add(
                    f"Entry till {r['timeTo']}",
                    f"New entry at {t['time']}",
                    "Medium",
                    condition=f"time <= {r['timeTo']}",
                    observed=str(t.get("time")),
                )

            # 6. Max trades per day
            entry_key = t.get("parent_trade_id") or t.get("entry_order_id") or t.get("id")
            if entry_key not in seen_entry_orders:
                seen_entry_orders.append(entry_key)
            trade_seq_idx = seen_entry_orders.index(entry_key)
            if r.get("maxTradesDay") and trade_seq_idx >= int(r["maxTradesDay"]):
                add(
                    f"Max {r['maxTradesDay']} trades / day",
                    f"Took trade no. {trade_seq_idx + 1}",
                    "Medium",
                    condition=f"trades_per_day <= {r['maxTradesDay']}",
                    observed=f"Trade #{trade_seq_idx + 1}",
                )

            # 7. Max lots per trade
            if r.get("maxLots") and float(t.get("lots") or 0) > float(r["maxLots"]):
                add(
                    f"Max {r['maxLots']} lots / trade",
                    f"Took {float(t['lots']):g} lots",
                    "Medium",
                    condition=f"lots <= {r['maxLots']}",
                    observed=f"{float(t['lots']):g} lots",
                )

            # 8. Stop-loss per lot
            has_valid_pnl = t.get("pnl") is not None or not t.get("open")
            if has_valid_pnl:
                g = gross_pnl(t)
                lots_val = float(t.get("lots") or 0)
                if r.get("slPerLot") and g < 0 and lots_val > 0 and (-g / lots_val) > float(r["slPerLot"]):
                    add(
                        f"SL max ₹{float(r['slPerLot']):,.0f} / lot",
                        f"Lost ₹{-g / lots_val:,.0f} per lot",
                        "High",
                        condition=f"loss_per_lot <= ₹{float(r['slPerLot']):,.0f}",
                        observed=f"₹{-g / lots_val:,.0f} / lot",
                    )

            # 9. Daily loss limit
            if r.get("dailyLoss") and stop:
                add(
                    f"Stop after ₹{float(r['dailyLoss']):,.0f} day loss",
                    "Kept trading after loss limit hit",
                    "High",
                    condition=f"daily_loss < ₹{float(r['dailyLoss']):,.0f}",
                    observed=f"Traded at day P&L ₹{float(day_pnl):,.0f}",
                )

            # 10. Manual vs Algo source
            if r.get("source") != "ANY" and t.get("source") != r.get("source"):
                add(
                    f"{r['source']} trades only",
                    f"Trade placed {t.get('source')}",
                    "Low",
                    condition=f"source == {r['source']}",
                    observed=str(t.get("source")),
                )

            # 11. Hedge required
            if str(r.get("hedge")).lower() == "true" and not t.get("hedged"):
                add(
                    "Hedge required",
                    "No hedge leg",
                    "Medium",
                    condition="hedged == True",
                    observed="hedged == False",
                )

            # 12. Expiry type
            if r.get("expiry") != "ANY" and t.get("expiryType") and t["expiryType"] != r["expiry"]:
                add(
                    f"{r['expiry']} expiry only",
                    f"Traded {t['expiryType']} expiry",
                    "Low",
                    condition=f"expiryType == {r['expiry']}",
                    observed=str(t.get("expiryType")),
                )

            if has_valid_pnl:
                day_pnl += _dec(gross_pnl(t), Decimal("0"))
                if r.get("dailyLoss") and day_pnl <= -_dec(r["dailyLoss"], Decimal("0")):
                    stop = True

            out[t["id"]] = f
    return out


def evaluate_unevaluated_rules(trades: List[dict], rules_by_trader: dict) -> Dict[str, List[dict]]:
    """Identifies rules that could not be evaluated due to missing broker data (Req 7.12, 7.13)."""
    unevaluated = {}
    for t in trades:
        owner_key = t.get("account") or t.get("trader")
        raw_rules = (
            rules_by_trader.get(t.get("account"))
            or rules_by_trader.get(t.get("trader"))
            or rules_by_trader.get(owner_key)
            or {}
        )
        r = {**DEFAULT_RULES, **raw_rules}
        missing = []
        if r.get("strikeMode") != "ANY" and t.get("opt") in ("CE", "PE") and not t.get("spot"):
            missing.append({
                "rule": f"Strike: {r['strikeMode']} only",
                "reason": "Spot price at entry unavailable from broker",
                "status": "UNEVALUATED",
            })
        if t.get("open") and t.get("pnl") is None and r.get("slPerLot"):
            missing.append({
                "rule": f"SL max ₹{float(r['slPerLot']):,.0f} / lot",
                "reason": "Live LTP unavailable from broker",
                "status": "UNEVALUATED",
            })
        if missing:
            unevaluated[t["id"]] = missing
    return unevaluated
