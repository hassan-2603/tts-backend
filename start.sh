#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
#  Trader Tracking System (TTS) – Linux / macOS Local Development Launcher
#  Usage: bash start.sh
# ══════════════════════════════════════════════════════════════════════════════
set -e
cd "$(dirname "$0")"

echo "[TTS] Trader Tracking System – Linux/macOS Startup"

# ── Create virtual environment if missing ─────────────────────────────────────
if [ ! -d ".venv" ]; then
    echo "[TTS] Creating Python virtual environment..."
    python3 -m venv .venv
fi

# ── Activate venv ─────────────────────────────────────────────────────────────
# shellcheck disable=SC1091
source .venv/bin/activate

# ── Install / upgrade dependencies ───────────────────────────────────────────
echo "[TTS] Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# ── Load .env if present ──────────────────────────────────────────────────────
if [ -f ".env" ]; then
    echo "[TTS] .env file found (pydantic-settings will load it automatically)"
else
    echo "[TTS] WARNING: No .env found. Copying .env.example as a starting point."
    echo "[TTS] Edit .env before running in production!"
    cp .env.example .env
fi

# ── Run database migrations ───────────────────────────────────────────────────
echo "[TTS] Running database migrations..."
alembic upgrade head || echo "[TTS] WARNING: Alembic migration step failed or skipped – continuing."

# ── Start the server ──────────────────────────────────────────────────────────
echo "[TTS] Starting Uvicorn on http://localhost:8000"
echo "[TTS] Press Ctrl+C to stop."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
