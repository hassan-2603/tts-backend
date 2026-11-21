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

    # Partial close decomposes into 1 closed realized leg (30 qty @ 340) + 1 remaining open leg (30 qty @ 300 entry, 350 LTP)
    assert len(trades) == 2
    closed_leg = next(x for x in trades if not x["open"])
    open_leg = next(x for x in trades if x["open"])

    assert closed_leg["qty"] == 30.0
    assert closed_leg["lots"] == 1.0
    assert closed_leg["entry"] == 300.0
    assert closed_leg["exit"] == 340.0
    assert closed_leg["realized_pnl"] == 1200.0
    assert closed_leg["exitDate"] == now.date().isoformat()

    assert open_leg["qty"] == 30.0
    assert open_leg["lots"] == 1.0
    assert open_leg["open"] is True  # 30 still open
    assert open_leg["entry"] == 300.0
    assert open_leg["exit"] == 350.0
    assert open_leg["unrealized_pnl"] == 1500.0
    assert open_leg["product"] == "NRML"
    assert open_leg["date"] == (now - timedelta(days=25)).date().isoformat()
    assert open_leg["parent_trade_id"] == closed_leg["parent_trade_id"]


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


def test_custom_plain_english_rules():
    from app.engine import parse_custom_rule

    assert parse_custom_rule("No BankNifty")["ok"] is True
    assert parse_custom_rule("No entry after 2:30 pm")["ok"] is True
    assert parse_custom_rule("Stop after 2 consecutive losses")["ok"] is True
    assert parse_custom_rule("No 0DTE")["ok"] is True
    assert parse_custom_rule("No naked selling")["ok"] is True
    assert parse_custom_rule("No revenge trading")["ok"] is True

    trades = [
        {
            "id": "C-1", "trader": "TR-02", "account": "AC-02", "date": "2026-11-26", "time": "10:00",
            "exitDate": "2026-11-26", "exitTime": "10:10", "expiry": "2026-11-26",
            "symbol": "BANKNIFTY", "strike": 52000, "opt": "CE", "side": "SELL",
            "lots": 1, "lotSize": 30, "entry": 100, "exit": 150, "pnl": -1500.0,
            "product": "MIS", "open": False, "charges": 0, "source": "Manual", "hedged": False,
        },
        {
            "id": "C-2", "trader": "TR-02", "account": "AC-02", "date": "2026-11-26", "time": "10:12",
            "exitDate": "2026-11-26", "exitTime": "10:20", "expiry": "2026-11-28",
            "symbol": "NIFTY", "strike": 24500, "opt": "PE", "side": "BUY",
            "lots": 1, "lotSize": 75, "entry": 100, "exit": 80, "pnl": -1500.0,
            "product": "MIS", "open": False, "charges": 0, "source": "Manual", "hedged": True,
        },
        {
            "id": "C-3", "trader": "TR-02", "account": "AC-02", "date": "2026-11-26", "time": "14:45",
            "exitDate": "2026-11-26", "exitTime": "14:55", "expiry": "2026-11-28",
            "symbol": "NIFTY", "strike": 24500, "opt": "PE", "side": "BUY",
            "lots": 1, "lotSize": 75, "entry": 100, "exit": 120, "pnl": 1500.0,
            "product": "MIS", "open": False, "charges": 0, "source": "Manual", "hedged": True,
        },
    ]
    custom_rules = {
        "TR-02": {
            "instrument": "ANY", "expiry": "ANY", "strikeMode": "ANY", "strikeDist": 0,
            "side": "ANY", "product": "ANY", "timeFrom": "", "timeTo": "",
            "maxTradesDay": 0, "maxLots": 0, "slPerLot": 0, "dailyLoss": 0,
            "hedge": False, "source": "ANY",
            "customRules": [
                {"id": "1", "text": "No BankNifty", "level": "High"},
                {"id": "2", "text": "No 0DTE", "level": "High"},
                {"id": "3", "text": "No naked selling", "level": "High"},
                {"id": "4", "text": "No revenge trading", "level": "Medium"},
                {"id": "5", "text": "Stop after 2 consecutive losses", "level": "High"},
                {"id": "6", "text": "No entry after 2:30 pm", "level": "Medium"},
            ],
        }
    }
    flags = check_trades(trades, custom_rules)
    r1 = [f["rule"] for f in flags["C-1"]]
    r2 = [f["rule"] for f in flags["C-2"]]
    r3 = [f["rule"] for f in flags["C-3"]]
    assert "Custom: No BankNifty" in r1
    assert "Custom: No 0DTE" in r1
    assert "Custom: No naked selling" in r1
    assert "Custom: No revenge trading" in r2
    assert "Custom: Stop after 2 consecutive losses" in r3
    assert "Custom: No entry after 2:30 pm" in r3
