@echo off
REM ══════════════════════════════════════════════════════════════════════════════
REM  Trader Tracking System (TTS) – Windows Local Development Launcher
REM  Usage: double-click start.bat  OR  run from PowerShell / CMD
REM ══════════════════════════════════════════════════════════════════════════════
setlocal
cd /d "%~dp0"

echo [TTS] Trader Tracking System – Windows Startup

REM ── Create virtual environment if missing ────────────────────────────────────
if not exist ".venv\Scripts\activate.bat" (
    echo [TTS] Creating Python virtual environment...
    python -m venv .venv
)

REM ── Activate venv ────────────────────────────────────────────────────────────
call .venv\Scripts\activate.bat

REM ── Install / upgrade dependencies ──────────────────────────────────────────
echo [TTS] Installing dependencies...
pip install -q --upgrade pip
pip install -q -r requirements.txt

REM ── Load .env if present (pydantic-settings reads it automatically) ───────────
if exist ".env" (
    echo [TTS] Loading .env file
) else (
    echo [TTS] WARNING: No .env found. Copying .env.example as a starting point.
    echo [TTS] Edit .env before running in production!
    copy .env.example .env >nul
)

REM ── Run database migrations ──────────────────────────────────────────────────
echo [TTS] Running database migrations...
alembic upgrade head
if errorlevel 1 (
    echo [TTS] WARNING: Alembic migration step failed or skipped – continuing.
)

REM ── Start the server ─────────────────────────────────────────────────────────
echo [TTS] Starting Uvicorn on http://localhost:8000
echo [TTS] Press Ctrl+C to stop.
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

endlocal
