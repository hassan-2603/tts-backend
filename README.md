# Trader Tracking System (TTS) — Real-Time Production Platform

Modern, reliable, real-time trading monitoring platform supporting **50+ concurrent linked Zebu / MYNT trading accounts**.

Tracking & monitoring only. The server **reads** each Zebu account's trade book, order book, and positions using official read-only endpoints and real-time WebSockets. It strictly contains **no code that places, modifies, or cancels orders**.

---

## Architecture Overview

```
                      ┌─────────────────────────────────────────┐
                      │    FastAPI + Uvicorn Async Server       │
                      │  (Preserves static/index.html & APIs)   │
                      └───────┬──────────────────────┬──────────┘
                              │                      │
                 ┌────────────▼──────────┐ ┌─────────▼────────────┐
                 │ 50-Account Supervisor │ │  MarketScheduler     │
                 │   ConnectionManager   │ │ (Advisory Lock / EOD)│
                 └───────┬───────────────┘ └──────────────────────┘
                         │
        ┌────────────────┴──────────────────┐
        │                                   │
┌───────▼────────────────────────┐ ┌────────▼────────────────────────┐
│ Zebu Real-Time WebSocket Feed  │ │ Zebu Async REST Reconciler      │
│ (Order updates, Fills, Quotes) │ │ (TradeBook, PositionBook, Spot) │
└────────────────────────────────┘ └─────────────────────────────────┘
        │                                   │
        └────────────────┬──────────────────┘
                         ▼
        ┌───────────────────────────────────┐
        │       PostgreSQL 16 Database      │
        │   (SQLAlchemy 2.0 Async + WAL)    │
        │  Indexed Relational Models & FIFO │
        └───────────────────────────────────┘
```

---

## Key Capabilities & Enhancements

1. **Structured PostgreSQL Database (Permanent Source of Truth)**
   - Replaced flat SQLite document tables with 20+ relational tables (`users`, `traders`, `accounts`, `broker_sessions`, `fills`, `orders`, `trades`, `positions`, `position_lots`, `trading_rules`, `alerts`, `alert_delivery_logs`, `audit_logs`, etc.).
   - Indexed columns on `(account_id, trade_date)`, `(trader_id, trade_date)`, avoiding full-table scans.
   - Exact financial data types (`Numeric(12, 4)`) instead of floating-point inaccuracies.
2. **50-Account Concurrency**
   - Independent asynchronous event processing loops per account.
   - Semaphore concurrency pools (`ACCOUNT_SYNC_WORKERS=10`), backpressure, and automatic circuit breakers (stops hammering broker API if account encounters consecutive errors).
3. **Official Zebu / Noren WebSocket Integration**
   - Live order updates (`om`), execution fills, and tick updates via `wss://go.mynt.in/NorenWSTP/`.
   - Automatic fallback to REST reconciliation marking account state as `DEGRADED` if WebSocket is unavailable.
4. **Persistent FIFO Position Lot Engine**
   - Supports Index Options (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX), Stock Options, Futures, and Equity Cash.
   - Persistent `position_lots` ledger preserves carry-forward positions older than 10–30 days.
5. **Token Lifecycle & Security**
   - Encrypts access and refresh tokens at rest with AES/Fernet encryption.
   - Clear `REAUTH_REQUIRED` status exposed when session validity expires.
6. **Reliable Alerting**
   - Database-backed alert deduplication preventing duplicate Telegram, WhatsApp, or Email alerts across server restarts.
   - Delivery logging tracking channel, recipient, timestamp, and provider error messages.
7. **Singleton Market Scheduler**
   - Uses PostgreSQL Advisory Locks (`pg_try_advisory_lock`) or Redis distributed locks, allowing safe multi-worker scaling (`uvicorn --workers 4`) without duplicate jobs.

---

## Migration from Legacy SQLite

TTS provides a safe, non-destructive migration utility that backs up your existing `tts.db` and migrates all historical records:

```bash
# Activate virtual environment
source .venv/bin/activate  # Or .venv\Scripts\activate on Windows

# Run migration (creates tts.db.bak.<timestamp>)
python -m app.migration tts.db
```

---

## Quick Start (Local & Production)

### Option A: Docker Compose (Recommended)

```bash
# 1. Clone repository & configure environment
cp .env.example .env
# Edit .env with your ZEBU_CLIENT_ID, ZEBU_SECRET_KEY, and PUBLIC_URL

# 2. Launch PostgreSQL, Redis, and TTS API
docker compose up -d

# 3. View logs
docker compose logs -f api
```

### Option B: Native Host Startup

```bash
# 1. Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate.bat
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env

# 3. Apply database migrations
alembic upgrade head

# 4. Start the server
# Linux / macOS:
./start.sh
# Windows:
start.bat
```

Open `http://localhost:8000` (or your configured `PUBLIC_URL`) and sign in with admin credentials.

---

## Running the Automated Test Suite

```bash
# Run unit tests, SQLite migration tests, and 50-account concurrency benchmark
pytest tests/test_engine.py tests/test_migration.py tests/test_concurrency_50.py -v

# Run complete End-to-End integration test against mock Zebu server
python tests/e2e.py
```
