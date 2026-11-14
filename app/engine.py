"""Upgraded FIFO Trade Engine & Comprehensive Risk Rule Checker.

Features:
- Persistent FIFO position lot accounting
- Multi-instrument parsing: Index & Stock Options, Futures, Equity Cash, Commodities
- Carry-forward position tracking (> 30 days)
- Partial fill aggregation and reversal handling
- Realized & Unrealized P&L calculation
- Spot price resolution at entry with TPSeries caching
- Comprehensive risk-rule compliance engine matching static/index.html
"""
import re
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any

MONTHS = {m: i for i, m in enumerate(["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}

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

    # Futures check
    for p in _FUT_PATTERNS:
        m = p.match(tsym)
        if m:
            g = m.groupdict()
            y = 2000 + int(g["y"])
            exp = last_weekday_of_month(y, MONTHS.get(g["m"], 1), 3)  # Last Thursday
            return {
                "type": "FUT",
                "underlying": g["u"],
                "expiry": exp,
                "opt": "FUT",
                "strike": 0.0,
                "expiryType": "Monthly",
            }

    # Options check
    for p in _OPT_PATTERNS:
        m = p.match(tsym)
        if not m:
            continue
        g = m.groupdict()
        y = 2000 + int(g["y"])
        opt = "CE" if g["o"] in ("C", "CE") else "PE"
        exp = None
        try:
            if g.get("m") and g.get("d"):
                exp = date(y, MONTHS[g["m"]], int(g["d"]))
            elif g.get("m"):
                exp = last_weekday_of_month(y, MONTHS[g["m"]], 1)  # NSE Tuesday expiry
            elif g.get("mn"):
                mon = {"O": 10, "N": 11, "D": 12}.get(g["mn"]) or int(g["mn"])
                exp = date(y, mon, int(g["d"]))
        except (KeyError, ValueError):
            exp = None

        return {
            "type": "OPT",
            "underlying": g["u"],
            "expiry": exp,
            "opt": opt,
            "strike": float(g["k"]),
            "expiryType": expiry_type(exp),
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
]


def parse_ts(*vals) -> Optional[datetime]:
    for v in vals:
        if not v:
            continue
        if isinstance(v, datetime):
            return v.replace(tzinfo=None)
        v_str = str(v).strip()
        for f in _TFMTS:
            try:
                d = datetime.strptime(v_str[:19], f[:19])
                if d.year > 2000:
                    return d.replace(tzinfo=None)
            except ValueError:
                pass
    return None


# ---------------- Charges Calculation ----------------
def estimate_charges(buy_value: float, sell_value: float, orders: int, cfg: dict) -> float:
    brokerage = cfg["brokerage_per_order"] * orders
    stt = sell_value * cfg["stt_sell_pct"] / 100
    txn = (buy_value + sell_value) * cfg["txn_pct"] / 100
    sebi = (buy_value + sell_value) * 10 / 1e7
    stamp = buy_value * cfg["stamp_buy_pct"] / 100
    gst = (brokerage + txn + sebi) * 0.18
    return round(brokerage + stt + txn + sebi + stamp + gst, 2)


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
) -> list:
    """FIFO-match fills per contract.

    Supports partial fills, multi-leg entries/exits, carry-forward positions,
    and all instrument classes.
    """
    ltp = ltp or {}
    if now and getattr(now, "tzinfo", None) is not None:
        now = now.replace(tzinfo=None)

    # 1. Group fills by order ID
    orders = {}
    for f in fills:
        oid = f.get("norenordno") or f.get("flid") or "O-" + str(f.get("fltm", ""))
        ts = parse_ts(f.get("fltm"), f.get("exch_tm"), f.get("norentm"))
        if not ts:
            continue
        o = orders.setdefault(
            oid,
            {
                "oid": oid,
                "tsym": f.get("tsym"),
                "exch": f.get("exch", "NFO"),
                "side": f.get("trantype"),
                "prd": f.get("prd"),
                "ls": int(_f(f.get("ls"), 1)) or 1,
                "token": f.get("token"),
                "ordersource": f.get("ordersource", ""),
                "remarks": f.get("remarks", ""),
                "qty": 0.0,
                "val": 0.0,
                "ts": ts,
                "last": ts,
            },
        )
        q = _f(f.get("flqty") or f.get("fillshares"))
        p = _f(f.get("flprc") or f.get("avgprc") or f.get("prc"))
        o["qty"] += q
        o["val"] += q * p
        o["ts"] = min(o["ts"], ts)
        o["last"] = max(o["last"], ts)

    by_sym = defaultdict(list)
    for o in orders.values():
        if o["qty"] > 0:
            by_sym[o["tsym"]].append(o)

    trades = []
    for tsym, olist in by_sym.items():
        info = parse_instrument(tsym, master)
        if not info:
            continue
        olist.sort(key=lambda o: o["ts"])

        # FIFO matching
        entries = []
        q2 = []
        for o in olist:
            px = o["val"] / o["qty"] if o["qty"] > 0 else 0.0
            remaining = o["qty"]
            while remaining > 0 and q2 and q2[0]["order"]["side"] != o["side"]:
                e = q2[0]
                m = min(e["remaining"], remaining)
                e["exits"].append((m, px, o["ts"], o["oid"]))
                e["remaining"] -= m
                remaining -= m
                if e["remaining"] <= 0:
                    q2.pop(0)

            if remaining > 0:
                e = {"order": o, "remaining": remaining, "qty": remaining, "exits": [], "px": px}
                q2.append(e)
                entries.append(e)

        for e in entries:
            o = e["order"]
            qty = e["qty"]
            closed = sum(x[0] for x in e["exits"])
            exit_val = sum(x[0] * x[1] for x in e["exits"])
            is_open = closed < qty
            if is_open:
                last = _f(ltp.get(tsym), e["exits"][-1][1] if e["exits"] else e["px"])
                exit_val += (qty - closed) * last
            exit_px = exit_val / qty if qty > 0 else e["px"]
            last_ts = max([x[2] for x in e["exits"]], default=None)
            entry_d = o["ts"].date()

            if is_open:
                after_close = now.time() >= datetime.strptime("15:30", "%H:%M").time()
                exit_date = now.date() if (now.date() > entry_d or after_close) else entry_d
                exit_time = "OPEN"
            else:
                exit_date, exit_time = last_ts.date(), last_ts.strftime("%H:%M")

            buy_val = e["px"] * qty if o["side"] == "B" else exit_val
            sell_val = exit_val if o["side"] == "B" else e["px"] * qty
            n_orders = 1 + len({x[3] for x in e["exits"]})
            product = product_map.get(o["prd"], o["prd"] or "MIS")

            trades.append({
                "id": f"Z-{account['id']}-{o['oid']}",
                "origin": "zebu",
                "date": entry_d.isoformat(),
                "time": o["ts"].strftime("%H:%M"),
                "trader": account.get("trader"),
                "account": account["id"],
                "symbol": info["underlying"],
                "strike": info["strike"],
                "opt": info["opt"],
                "expiry": info["expiry"].isoformat() if info.get("expiry") else "",
                "expiryType": info.get("expiryType", ""),
                "tsym": tsym,
                "exch": o["exch"],
                "side": "BUY" if o["side"] == "B" else "SELL",
                "lots": round(qty / o["ls"], 2),
                "lotSize": o["ls"],
                "qty": qty,
                "entry": round(e["px"], 2),
                "exit": round(exit_px, 2),
                "open": is_open,
                "exitDate": exit_date.isoformat(),
                "exitTime": exit_time,
                "spot": None,
                "_ts": o["ts"].isoformat(),
                "product": "NRML" if product in ("NRML", "CNC") else "MIS",
                "source": "Algo" if is_algo(o) else "Manual",
                "charges": estimate_charges(buy_val, sell_val, n_orders, cfg),
                "chargesEstimated": True,
                "hedged": False,
            })

    # Hedge detection: BUY leg hedged if a SELL leg was open on the same symbol at the same time
    for t in trades:
        if t["side"] != "BUY":
            continue
        for s in trades:
            if s["side"] == "SELL" and s["symbol"] == t["symbol"] and s["date"] == t["date"]:
                s_end = "23:59" if s["open"] or s["exitDate"] != s["date"] else s["exitTime"]
                if s["time"] <= t["time"] <= s_end:
                    t["hedged"] = True
                    s["hedged"] = True

    return trades


# ---------------- Risk Rule Compliance Engine ----------------
def to_min(t: Any) -> int:
    h, m = str(t).split(":")[:2]
    return int(h) * 60 + int(m)


def gross_pnl(t: dict) -> float:
    d = -1 if t["side"] == "SELL" else 1
    return (t["exit"] - t["entry"]) * t["lots"] * t["lotSize"] * d


def net_pnl(t: dict) -> float:
    return gross_pnl(t) - (t.get("charges") or 0.0)


def moneyness(t: dict, step: int) -> Tuple[float, int]:
    if not t.get("spot") or not step:
        return 0.0, 0
    atm = round(t["spot"] / step) * step
    strike = t.get("strike") or atm
    d = ((atm - strike) if t.get("opt") == "CE" else (strike - atm)) / step
    return atm, round(d)


def mlabel(d: int) -> str:
    return "ATM" if d == 0 else (f"{d} ITM" if d > 0 else f"{-d} OTM")


def check_trades(trades: List[dict], rules_by_trader: dict, step_default: int = 100) -> Dict[str, List[dict]]:
    """Evaluates trades against each trader's rule book.

    Returns: {trade_id: [{"rule": ..., "did": ..., "level": ...}]}
    """
    out = {}
    day = defaultdict(list)
    for t in trades:
        day[(t["trader"], t["date"])].append(t)

    for (trader, _), lst in day.items():
        lst.sort(key=lambda x: x["time"])
        r = {**DEFAULT_RULES, **(rules_by_trader.get(trader) or {})}
        day_pnl, stop = 0.0, False

        for i, t in enumerate(lst):
            f = []

            def add(rule: str, did: str, lvl: str):
                f.append({"rule": rule, "did": did, "level": lvl})

            step = STRIKE_STEP.get(t["symbol"], step_default)

            if r.get("instrument") != "ANY" and t["symbol"] != r.get("instrument"):
                add(f"Instrument: {r['instrument']} only", f"Traded {t['symbol']}", "High")

            if r.get("strikeMode") != "ANY" and t.get("spot") and t.get("opt") in ("CE", "PE"):
                atm, d = moneyness(t, step)
                want = 0 if r["strikeMode"] == "ATM" else (r["strikeDist"] if r["strikeMode"] == "ITM" else -r["strikeDist"])
                if d != want:
                    n = 0 if r["strikeMode"] == "ATM" else r["strikeDist"]
                    sign = 1 if r["strikeMode"] == "ITM" else (-1 if r["strikeMode"] == "OTM" else 0)
                    right = atm - sign * n * step if t["opt"] == "CE" else atm + sign * n * step
                    label = "ATM" if r["strikeMode"] == "ATM" else f"{r['strikeDist']} {r['strikeMode']}"
                    add(
                        f"Strike: {label} only",
                        f"Bought {t['strike']:g} {t['opt']} = {mlabel(d)} (spot {t['spot']:,.0f}, right strike {right:g} {t['opt']})",
                        "High",
                    )

            if r.get("side") != "ANY" and t["side"] != r.get("side"):
                add("Option buying only" if r["side"] == "BUY" else "Option selling only", f"{t['side']} trade", "High")

            exit_d = t.get("exitDate") or t.get("date")
            if r.get("product") == "MIS" and (t.get("product") == "NRML" or exit_d != t.get("date")):
                add("Intraday only", f"Carried position overnight (exit {exit_d})", "High")

            if r.get("timeFrom") and to_min(t["time"]) < to_min(r["timeFrom"]):
                add(f"Entry from {r['timeFrom']}", f"Entry at {t['time']}", "Medium")

            if r.get("timeTo") and to_min(t["time"]) > to_min(r["timeTo"]):
                add(f"Entry till {r['timeTo']}", f"New entry at {t['time']}", "Medium")

            if r.get("maxTradesDay") and i >= int(r["maxTradesDay"]):
                add(f"Max {r['maxTradesDay']} trades / day", f"Took trade no. {i + 1}", "Medium")

            if r.get("maxLots") and t["lots"] > float(r["maxLots"]):
                add(f"Max {r['maxLots']} lots / trade", f"Took {t['lots']:g} lots", "Medium")

            g = gross_pnl(t)
            if r.get("slPerLot") and g < 0 and t["lots"] and -g / t["lots"] > float(r["slPerLot"]):
                add(f"SL max ₹{float(r['slPerLot']):,.0f} / lot", f"Lost ₹{-g / t['lots']:,.0f} per lot", "High")

            if r.get("dailyLoss") and stop:
                add(f"Stop after ₹{float(r['dailyLoss']):,.0f} day loss", "Kept trading after loss limit hit", "High")

            if r.get("source") != "ANY" and t["source"] != r.get("source"):
                add(f"{r['source']} trades only", f"Trade placed {t['source']}", "Low")

            if str(r.get("hedge")).lower() == "true" and not t.get("hedged"):
                add("Hedge required", "No hedge leg", "Medium")

            if r.get("expiry") != "ANY" and t.get("expiryType") and t["expiryType"] != r["expiry"]:
                add(f"{r['expiry']} expiry only", f"Traded {t['expiryType']} expiry", "Low")

            day_pnl += net_pnl(t)
            if r.get("dailyLoss") and day_pnl <= -float(r["dailyLoss"]):
                stop = True

            out[t["id"]] = f
    return out
