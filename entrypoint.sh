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
    for i in $(seq 1 15); do
        python - <<'PYEOF' && return 0
import asyncio, os, sys, asyncpg
async def _ping():
    url = os.environ.get("DATABASE_URL","").strip()
    url = url.replace("postgresql+asyncpg://","postgresql://")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    conn = await asyncpg.connect(url, timeout=5)
    await conn.close()
try:
    asyncio.run(_ping())
except Exception as e:
    print(f"[TTS] PG ping error: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
        info "  attempt $i/15 – not ready yet, retrying in 1 s..."
        sleep 1
    done
    red "PostgreSQL did not become ready in 15 seconds – continuing startup so health endpoints bind port."
}

# ── Run Alembic ───────────────────────────────────────────────────────────────
run_migrations() {
    info "Running Alembic migrations..."
    alembic upgrade head || red "Alembic migration warning (will verify tables in app lifespan)."
    green "Migrations step complete."
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

# Hand off to the command passed to docker run / Railway Start Command
if [ "$#" -eq 0 ]; then
    set -- uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --workers 1
fi
info "Starting: $*"
case "$*" in
    *"&&"*|*"||"*|*";"*|*'$PORT'*)
        exec sh -c "$*"
        ;;
    *)
        exec "$@"
        ;;
esac
