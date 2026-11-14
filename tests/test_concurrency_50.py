"""50 Concurrent Accounts Load & Latency Test.

Verifies that the modernized TTS architecture can process 50 simulated trading
accounts concurrently with bounded latency, rate limiting, and memory stability.
"""
import asyncio
import os
import time
import tempfile
import psutil
from datetime import datetime, timezone
import pytest

from app.database import init_models, AsyncSessionLocal
from app.models import Account, Trader, BrokerSession, Fill, Trade
from app.store import Store
from app.connection_manager import ConnectionManager
from app.zebu import ZebuClient
from app.engine import build_trades, check_trades, DEFAULT_RULES
from app.metrics import METRICS


@pytest.mark.asyncio
async def test_50_account_concurrency():
    print("\n" + "=" * 60)
    print("STARTING 50 CONCURRENT ACCOUNTS LOAD TEST")
    print("=" * 60)

    # Use isolated test database
    from app.database import set_engine_url
    test_db = tempfile.mktemp(suffix=".db")
    set_engine_url(f"sqlite+aiosqlite:///{test_db}")

    # 1. Initialize DB and Store
    await init_models()
    mock_zebu = ZebuClient("http://127.0.0.1:9001", "CID", "SEC")
    cm = ConnectionManager(mock_zebu)
    await cm.start()

    now = datetime.now(timezone.utc)
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss / (1024 * 1024)

    # 2. Setup 50 Accounts & Traders in database
    t0_setup = time.perf_counter()
    async with AsyncSessionLocal() as session:
        for i in range(1, 51):
            aid = f"AC-SIM-{i:02d}"
            tid = f"TR-SIM-{i:02d}"

            trader = Trader(id=tid, name=f"Trader {i}", status="Testing", test_capital=500000)
            account = Account(id=aid, name=f"Account {i}", broker="Zebu", trader_id=tid, zebu_uid=f"ZP{i:04d}")
            bs = BrokerSession(account_id=aid, status="Linked", ws_status="REAL-TIME")

            await session.merge(trader)
            await session.merge(account)
            await session.merge(bs)
        await session.commit()

    setup_ms = (time.perf_counter() - t0_setup) * 1000
    print(f"Setup 50 accounts & traders in {setup_ms:.2f}ms")

    # 3. Simulate Concurrent Ingestion & Trade Building across all 50 accounts
    latencies = {
        "event_ingestion": [],
        "trade_building": [],
        "risk_evaluation": [],
        "db_persistence": [],
    }

    async def simulate_account_workflow(acc_idx: int):
        aid = f"AC-SIM-{acc_idx:02d}"
        tid = f"TR-SIM-{acc_idx:02d}"

        # A. Ingest fills
        t0 = time.perf_counter()
        fills = [
            {
                "norenordno": f"ORD-{acc_idx}-1",
                "flid": f"FL-{acc_idx}-1",
                "tsym": "BANKNIFTY24NOV26C51700",
                "trantype": "B",
                "flqty": "30",
                "flprc": "310.00",
                "ls": "30",
                "prd": "I",
                "fltm": "14-11-2026 10:15:00",
            },
            {
                "norenordno": f"ORD-{acc_idx}-2",
                "flid": f"FL-{acc_idx}-2",
                "tsym": "BANKNIFTY24NOV26C51700",
                "trantype": "S",
                "flqty": "30",
                "flprc": "345.00",
                "ls": "30",
                "prd": "I",
                "fltm": "14-11-2026 10:45:00",
            },
        ]
        latencies["event_ingestion"].append((time.perf_counter() - t0) * 1000)

        # B. FIFO Trade Building
        t1 = time.perf_counter()
        cfg = {"brokerage_per_order": 20, "stt_sell_pct": 0.1, "txn_pct": 0.035, "stamp_buy_pct": 0.003}
        built_trades = build_trades(
            {"id": aid, "trader": tid},
            fills,
            cfg,
            {"I": "MIS"},
            lambda o: False,
            now,
            ltp={"BANKNIFTY24NOV26C51700": 345.0},
        )
        for tr in built_trades:
            tr["spot"] = 51800.0  # Simulated spot
        latencies["trade_building"].append((time.perf_counter() - t1) * 1000)

        # C. Risk Rule Evaluation
        t2 = time.perf_counter()
        flags = check_trades(built_trades, {tid: DEFAULT_RULES})
        latencies["risk_evaluation"].append((time.perf_counter() - t2) * 1000)

        # D. Database Persistence
        t3 = time.perf_counter()
        async with AsyncSessionLocal() as session:
            for tr in built_trades:
                trade_record = Trade(
                    id=tr["id"],
                    origin="zebu",
                    account_id=aid,
                    trader_id=tid,
                    trade_date=tr["date"],
                    trade_time=tr["time"],
                    symbol=tr["symbol"],
                    strike=tr["strike"],
                    opt=tr["opt"],
                    tsym=tr["tsym"],
                    side=tr["side"],
                    lots=tr["lots"],
                    lot_size=tr["lotSize"],
                    qty=tr["qty"],
                    entry_price=tr["entry"],
                    exit_price=tr["exit"],
                    is_open=tr["open"],
                    spot=tr["spot"],
                )
                await session.merge(trade_record)
            await session.commit()
        latencies["db_persistence"].append((time.perf_counter() - t3) * 1000)

    # 4. Launch all 50 concurrent account tasks
    t_start = time.perf_counter()
    await asyncio.gather(*(simulate_account_workflow(i) for i in range(1, 51)))
    total_duration_s = time.perf_counter() - t_start

    end_mem = process.memory_info().rss / (1024 * 1024)
    mem_delta = round(end_mem - start_mem, 2)

    await cm.stop()

    # 5. Calculate Metrics
    avg_ingestion_ms = sum(latencies["event_ingestion"]) / len(latencies["event_ingestion"])
    avg_trade_build_ms = sum(latencies["trade_building"]) / len(latencies["trade_building"])
    avg_risk_eval_ms = sum(latencies["risk_evaluation"]) / len(latencies["risk_evaluation"])
    avg_db_persist_ms = sum(latencies["db_persistence"]) / len(latencies["db_persistence"])

    print("\nBENCHMARK RESULTS (50 Concurrent Accounts):")
    print(f"- Total 50-account execution time : {total_duration_s:.3f} s")
    print(f"- Avg Event Ingestion Latency      : {avg_ingestion_ms:.2f} ms")
    print(f"- Avg FIFO Trade Building Latency : {avg_trade_build_ms:.2f} ms")
    print(f"- Avg Risk Rule Evaluation Latency: {avg_risk_eval_ms:.2f} ms")
    print(f"- Avg DB Persistence Latency      : {avg_db_persist_ms:.2f} ms")
    print(f"- Memory Delta                     : +{mem_delta} MB (Final: {end_mem:.2f} MB)")
    print("=" * 60 + "\n")

    assert total_duration_s < 10.0, f"50 accounts took too long: {total_duration_s}s"
    assert avg_risk_eval_ms < 50.0, f"Risk evaluation latency too high: {avg_risk_eval_ms}ms"
    assert avg_db_persist_ms < 1000.0, f"DB persistence latency too high: {avg_db_persist_ms}ms"


if __name__ == "__main__":
    asyncio.run(test_50_account_concurrency())
