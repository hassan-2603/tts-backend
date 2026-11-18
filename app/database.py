"""Database engine and session management with PostgreSQL asyncpg and SQLite aiosqlite support."""
import logging
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


import os

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
    """Create all tables in database if they do not exist."""
    eng = engine or async_engine
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Graceful column migration for accounts and trades tables (PostgreSQL & SQLite)
        for tbl, col, col_type in [
            ("accounts", "zebu_client_id", "VARCHAR(100)"),
            ("accounts", "zebu_secret_key", "VARCHAR(200)"),
            ("accounts", "proxy_url", "VARCHAR(300)"),
            ("trades", "pnl", "NUMERIC(14, 2)"),
        ]:
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
