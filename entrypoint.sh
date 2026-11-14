#!/usr/bin/env sh
# Docker container entrypoint for Trader Tracking System.
# 1. Waits for PostgreSQL to be ready (up to 30 s).
# 2. Runs Alembic migrations (idempotent – safe to run on every start).
# 3. Starts the requested process (api / worker / scheduler).
set -e

# ── Colour helpers ────────────────────────────────────────────────────────────
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
info()  { printf '[TTS] %s\n' "$*"; }

# ── Wait for Postgres ─────────────────────────────────────────────────────────
wait_for_postgres() {
    info "Waiting for PostgreSQL..."
    for i in $(seq 1 30); do
        # Try a quick Python asyncpg ping instead of relying on pg_isready
        python - <<'PYEOF' 2>/dev/null && return 0
import asyncio, os, asyncpg
async def _ping():
    url = os.environ.get("DATABASE_URL","")
    # strip driver prefix so asyncpg.connect can use it
    url = url.replace("postgresql+asyncpg://","postgresql://")
    conn = await asyncpg.connect(url, timeout=3)
    await conn.close()
asyncio.run(_ping())
PYEOF
        info "  attempt $i/30 – not ready yet, retrying in 1 s..."
        sleep 1
    done
    red "PostgreSQL did not become ready in 30 seconds. Aborting."
    exit 1
}

# ── Run Alembic ───────────────────────────────────────────────────────────────
run_migrations() {
    info "Running Alembic migrations..."
    alembic upgrade head
    green "Migrations complete."
}

# ── Main ──────────────────────────────────────────────────────────────────────
# Only wait + migrate when DATABASE_URL points at PostgreSQL
DB_URL="${DATABASE_URL:-}"
case "$DB_URL" in
    postgresql*|postgres*)
        wait_for_postgres
        run_migrations
        ;;
    *)
        info "Non-PostgreSQL DATABASE_URL detected – skipping migration step."
        ;;
esac

# Hand off to the command passed to docker run / compose
info "Starting: $*"
exec "$@"
