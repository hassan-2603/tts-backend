"""Unit tests for Trade Engine and FIFO position accounting."""
from datetime import datetime, timedelta
import pytest
from app.engine import build_trades, parse_instrument, check_trades, DEFAULT_RULES


def test_parse_instrument_multi_asset():
    # Index Option
    opt = parse_instrument("BANKNIFTY24NOV26C51700")
    assert opt["type"] == "OPT"
    assert opt["underlying"] == "BANKNIFTY"
    assert opt["strike"] == 51700.0
    assert opt["opt"] == "CE"

    # Future
    fut = parse_instrument("BANKNIFTY24NOVFUT")
    assert fut["type"] == "FUT"
    assert fut["underlying"] == "BANKNIFTY"
    assert fut["opt"] == "FUT"

    # Equity Cash
    eq = parse_instrument("RELIANCE-EQ")
    assert eq["type"] == "EQ"
    assert eq["underlying"] == "RELIANCE"


def test_fifo_carry_forward_and_partial_fills():
    acc = {"id": "AC-01", "trader": "TR-01"}
    now = datetime(2026, 11, 14, 11, 0)
    cfg = {"brokerage_per_order": 20, "stt_sell_pct": 0.1, "txn_pct": 0.035, "stamp_buy_pct": 0.003}
    pmap = {"I": "MIS", "M": "NRML"}

    # Entry fill from 25 days ago (carrying forward)
    old_fill_time = (now - timedelta(days=25)).strftime("%d-%m-%Y 10:15:00")
    today_fill_time = now.strftime("%d-%m-%Y 10:30:00")

    fills = [
        {
            "norenordno": "ORD-ENTRY-1",
            "flid": "F1",
            "tsym": "BANKNIFTY24NOV26C51700",
            "trantype": "B",
            "flqty": "60",
            "flprc": "300.00",
            "ls": "30",
            "prd": "M",
            "fltm": old_fill_time,
        },
        # Partial exit 1
        {
            "norenordno": "ORD-EXIT-1",
            "flid": "F2",
            "tsym": "BANKNIFTY24NOV26C51700",
            "trantype": "S",
            "flqty": "30",
            "flprc": "340.00",
            "ls": "30",
            "prd": "M",
            "fltm": today_fill_time,
        },
    ]

    trades = build_trades(acc, fills, cfg, pmap, lambda o: False, now, ltp={"BANKNIFTY24NOV26C51700": 350.0})

    assert len(trades) == 1
    t = trades[0]
    assert t["lots"] == 2.0  # Total 60 qty / 30 ls
    assert t["open"] is True  # 30 still open
    assert t["entry"] == 300.0
    assert t["product"] == "NRML"
    assert t["date"] == (now - timedelta(days=25)).date().isoformat()


def test_risk_rules_evaluation():
    trades = [
        {
            "id": "T-1",
            "trader": "TR-01",
            "date": "2026-11-14",
            "time": "09:05",  # Too early (rule is 09:20)
            "symbol": "BANKNIFTY",
            "strike": 51700,
            "opt": "CE",
            "side": "BUY",
            "lots": 5,  # Too large (rule max 3)
            "lotSize": 30,
            "entry": 300,
            "exit": 200,  # Loss of 100 pt * 30 * 5 = 15,000 loss
            "product": "NRML",  # Product violation (MIS only)
            "open": False,
            "charges": 50,
            "source": "Manual",
        }
    ]

    rules = {"TR-01": DEFAULT_RULES}
    flags = check_trades(trades, rules)

    flag_rules = [f["rule"] for f in flags["T-1"]]
    assert any("Entry from 09:20" in r for r in flag_rules)
    assert any("Max 3 lots" in r for r in flag_rules)
    assert any("Intraday only" in r for r in flag_rules)
