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


def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL") or CFG.DATABASE_URL
    # Ensure correct async driver scheme
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("sqlite:///"):
        url = url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
    return url


def create_engine_instance(url: str = None) -> AsyncEngine:
    db_url = url or get_db_url()
    is_sqlite = "sqlite" in db_url
    engine_kwargs = {
        "echo": CFG.DEBUG,
        "future": True,
    }
    if not is_sqlite:
        engine_kwargs.update({
            "pool_size": CFG.DATABASE_POOL_SIZE,
            "max_overflow": CFG.DATABASE_MAX_OVERFLOW,
            "pool_timeout": CFG.DATABASE_TIMEOUT,
            "pool_pre_ping": True,
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
            ("trades", "parent_trade_id", "VARCHAR(120)"),
            ("trades", "entry_order_id", "VARCHAR(100)"),
            ("trades", "exit_order_id", "VARCHAR(100)"),
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
            ("alerts", "order_id", "VARCHAR(100)"),
            ("alerts", "trade_date", "VARCHAR(20)"),
            ("alerts", "trade_time", "VARCHAR(20)"),
            ("alerts", "configured_condition", "VARCHAR(250)"),
            ("alerts", "observed_value", "VARCHAR(250)"),
            ("alerts", "explanation", "TEXT"),
            ("alerts", "violation_status", "VARCHAR(30) DEFAULT 'ACTIVE'"),
        ]
        for tbl, col, col_type in migrations:
            try:
                await conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {col_type}"))
            except Exception:
                try:
                    await conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN {col} {col_type}"))
                except Exception:
                    pass
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
