"""Database engine and session management with PostgreSQL asyncpg and SQLite aiosqlite support."""
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy import text
from sqlalchemy.orm import declarative_base
from app.config import CFG

log = logging.getLogger("tts.database")

Base = declarative_base()


def _normalize_pg_url_and_ssl(raw_url: str) -> tuple[str, bool]:
    """Normalize Postgres URL for asyncpg and detect whether SSL should be enforced."""
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

    url = (raw_url or "").strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("sqlite:///"):
        url = url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)

    if "postgresql" not in url:
        return url, False

    parts = urlsplit(url)
    qs = dict(parse_qsl(parts.query, keep_blank_values=True))
    sslmode = str(qs.pop("sslmode", "") or "").lower()
    ssl_param = str(qs.pop("ssl", "") or "").lower()
    needs_ssl = (
        sslmode in ("require", "verify-ca", "verify-full", "prefer")
        or ssl_param in ("true", "1", "require")
        or "render.com" in (parts.hostname or "").lower()
    )
    clean_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))
    return clean_url, needs_ssl


def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL") or CFG.DATABASE_URL
    clean_url, _ = _normalize_pg_url_and_ssl(url)
    return clean_url


def get_pg_connect_args(url: str = None) -> dict:
    raw = url or os.environ.get("DATABASE_URL") or CFG.DATABASE_URL
    _, needs_ssl = _normalize_pg_url_and_ssl(raw)
    args = {
        "timeout": 10,
        "command_timeout": 30,
    }
    if needs_ssl:
        args["ssl"] = "require"
    return args


def create_engine_instance(url: str = None) -> AsyncEngine:
    raw_url = url or os.environ.get("DATABASE_URL") or CFG.DATABASE_URL
    db_url, needs_ssl = _normalize_pg_url_and_ssl(raw_url)
    is_sqlite = "sqlite" in db_url
    engine_kwargs = {
        "echo": CFG.DEBUG,
    }
    if not is_sqlite:
        cargs = {
            "timeout": 10,
            "command_timeout": 30,
        }
        if needs_ssl:
            cargs["ssl"] = "require"
        engine_kwargs.update({
            "pool_size": CFG.DATABASE_POOL_SIZE,
            "max_overflow": CFG.DATABASE_MAX_OVERFLOW,
            "pool_timeout": CFG.DATABASE_TIMEOUT,
            "pool_pre_ping": True,
            "pool_recycle": 1800,
            "connect_args": cargs,
        })
    return create_async_engine(db_url, **engine_kwargs)


async_engine = create_engine_instance()
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


def set_engine_url(url: str):
    """Dynamically re-bind the engine and sessionmaker (used for testing or dynamic switching)."""
    global async_engine, AsyncSessionLocal
    os.environ["DATABASE_URL"] = url
    async_engine = create_engine_instance(url)
    AsyncSessionLocal.configure(bind=async_engine)


async def init_models(engine: AsyncEngine = None):
    """Create all tables in database if they do not exist, and migrate new columns safely."""
    eng = engine or async_engine
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        migrations = [
            ("accounts", "zebu_client_id", "VARCHAR(100)"),
            ("accounts", "zebu_vc", "VARCHAR(100)"),
            ("accounts", "zebu_secret_key", "VARCHAR(200)"),
            ("accounts", "zebu_factor2_enc", "TEXT"),
            ("accounts", "zebu_password_enc", "TEXT"),
            ("accounts", "proxy_url", "VARCHAR(300)"),
            ("broker_sessions", "auth_mode", "VARCHAR(30) DEFAULT 'oauth'"),
            ("trades", "pnl", "NUMERIC(14, 2)"),
            ("trades", "realized_pnl", "NUMERIC(14, 2)"),
            ("trades", "unrealized_pnl", "NUMERIC(14, 2)"),
            ("trades", "net_pnl", "NUMERIC(14, 2)"),
            ("trades", "pnl_source", "VARCHAR(30)"),
            ("trades", "parent_trade_id", "TEXT"),
            ("trades", "entry_order_id", "TEXT"),
            ("trades", "exit_order_id", "TEXT"),
            ("trades", "is_finalized", "BOOLEAN DEFAULT FALSE"),
            ("trades", "finalized_at", "TIMESTAMP WITH TIME ZONE"),
            ("trades", "is_archived", "BOOLEAN DEFAULT FALSE"),
            ("trades", "archived_at", "TIMESTAMP WITH TIME ZONE"),
            ("trades", "token", "VARCHAR(50)"),
            ("positions", "raw_payload", "JSON"),
            ("account_sync_states", "is_stale", "BOOLEAN DEFAULT FALSE"),
            ("daily_sync_records", "finalized", "BOOLEAN DEFAULT FALSE"),
            ("daily_sync_records", "archived", "BOOLEAN DEFAULT FALSE"),
            ("trading_rules", "account_id", "VARCHAR(100)"),
            ("alerts", "order_id", "TEXT"),
            ("alerts", "trade_date", "VARCHAR(20)"),
            ("alerts", "trade_time", "VARCHAR(20)"),
            ("alerts", "configured_condition", "TEXT"),
            ("alerts", "observed_value", "TEXT"),
            ("alerts", "explanation", "TEXT"),
            ("alerts", "violation_status", "VARCHAR(30) DEFAULT 'ACTIVE'"),
        ]
        is_pg = eng.dialect.name == "postgresql"
        existing_cols: set = set()
        if is_pg:
            try:
                res = await conn.execute(
                    text(
                        "SELECT table_name, column_name FROM information_schema.columns "
                        "WHERE table_schema = current_schema()"
                    )
                )
                existing_cols = {(r[0], r[1]) for r in res.fetchall()}
            except Exception as e:
                log.warning(f"Could not inspect existing PostgreSQL columns: {e}")

        missing = [
            (tbl, col, col_type)
            for tbl, col, col_type in migrations
            if not is_pg or (tbl, col) not in existing_cols
        ]
        if is_pg and missing:
            try:
                await conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            except Exception:
                pass

        for tbl, col, col_type in missing:
            try:
                async with conn.begin_nested():
                    if is_pg:
                        await conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {col_type}"))
                    else:
                        await conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN {col} {col_type}"))
            except Exception as e:
                if is_pg:
                    log.warning(f"Skipped column migration {tbl}.{col}: {e}")

        # Widen variable-length order ID / rule text columns to TEXT so long multi-fill values never overflow VARCHAR
        if is_pg:
            widen_targets = [
                ("trades", "entry_order_id"),
                ("trades", "exit_order_id"),
                ("trades", "parent_trade_id"),
                ("position_lots", "order_id"),
                ("alerts", "order_id"),
                ("alerts", "configured_condition"),
                ("alerts", "observed_value"),
                ("alerts", "title"),
            ]
            try:
                res_w = await conn.execute(
                    text(
                        "SELECT table_name, column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = current_schema() "
                        "AND table_name IN ('trades', 'position_lots', 'alerts')"
                    )
                )
                col_types = {(r[0], r[1]): str(r[2]).lower() for r in res_w.fetchall()}
                for tbl_name, col_name in widen_targets:
                    dtype = col_types.get((tbl_name, col_name))
                    if dtype and dtype != "text":
                        async with conn.begin_nested():
                            await conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                            await conn.execute(text(f"ALTER TABLE {tbl_name} ALTER COLUMN {col_name} TYPE TEXT"))
            except Exception as e:
                log.warning(f"Skipped widening TEXT columns: {e}")
    log.info("Database tables verified/initialized.")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for obtaining an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Context manager for obtaining an async session outside FastAPI requests."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
