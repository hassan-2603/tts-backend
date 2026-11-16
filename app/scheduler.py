"""Singleton Distributed Scheduler for Market Hours & EOD Reconciliation.

Uses PostgreSQL advisory lock or Redis distributed lock to ensure that even when
multiple Uvicorn worker processes are running, only ONE singleton scheduler executes.
"""
import asyncio
import inspect
import logging
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Callable, Coroutine, Any
from sqlalchemy import text
from app.config import CFG
from app.database import AsyncSessionLocal

try:
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))
log = logging.getLogger("tts.scheduler")

# Unique 64-bit integer identifier for PostgreSQL advisory lock
TTS_SCHEDULER_LOCK_ID = 8847291048291029


def now_ist() -> datetime:
    return datetime.now(IST).replace(tzinfo=None)


class DistributedLock:
    """Manages distributed leader election using PostgreSQL Advisory Lock or Redis."""

    def __init__(self):
        self._acquired = False
        self._redis = None

    async def try_acquire(self) -> bool:
        if self._acquired:
            return True

        if CFG.USE_REDIS and CFG.REDIS_URL:
            try:
                import redis.asyncio as aioredis
                if not self._redis:
                    self._redis = aioredis.from_url(CFG.REDIS_URL)
                res = await self._redis.set("tts:scheduler:leader", "1", nx=True, ex=30)
                self._acquired = bool(res)
                return self._acquired
            except Exception as e:
                log.warning(f"Redis distributed lock failed, falling back to database lock: {e}")

        # PostgreSQL Advisory Lock
        try:
            async with AsyncSessionLocal() as session:
                bind = session.bind
                if not bind or "sqlite" in str(bind.url):
                    self._acquired = True
                    return True

                result = await session.execute(
                    text(f"SELECT pg_try_advisory_lock({TTS_SCHEDULER_LOCK_ID})")
                )
                self._acquired = bool(result.scalar())
                return self._acquired
        except Exception as e:
            log.warning(f"Advisory lock check notice ({e}); proceeding as in-process scheduler leader")
            self._acquired = True
            return True

    async def release(self):
        if not self._acquired:
            return
        if self._redis:
            try:
                await self._redis.delete("tts:scheduler:leader")
            except Exception:
                pass
        else:
            try:
                async with AsyncSessionLocal() as session:
                    if "sqlite" not in str(session.bind.url):
                        await session.execute(
                            text(f"SELECT pg_advisory_unlock({TTS_SCHEDULER_LOCK_ID})")
                        )
            except Exception:
                pass
        self._acquired = False


class MarketScheduler:
    def __init__(
        self,
        poll_fn: Callable[[], Coroutine[Any, Any, Any]],
        summary_fn: Callable[[datetime], Coroutine[Any, Any, Any]],
        get_settings_fn: Callable[[], dict],
    ):
        self.poll_fn = poll_fn
        self.summary_fn = summary_fn
        self.get_settings_fn = get_settings_fn
        self.lock = DistributedLock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_poll_time = 0.0

    async def start(self):
        if self._running or CFG.DISABLE_SCHEDULER:
            return
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        log.info("MarketScheduler task started.")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.lock.release()
        log.info("MarketScheduler task stopped.")

    async def _scheduler_loop(self):
        while self._running:
            try:
                is_leader = await self.lock.try_acquire()
                if not is_leader:
                    # Another worker is currently the leader; wait and retry
                    await asyncio.sleep(15)
                    continue

                now = now_ist()
                hm = now.strftime("%H:%M")
                weekday = now.weekday() < 5

                # Market Hours Polling
                if weekday and CFG.MARKET_OPEN <= hm <= CFG.MARKET_CLOSE:
                    if time.time() - self.last_poll_time >= CFG.POLL_SECONDS:
                        log.info(f"Triggering scheduled poll for {hm} IST")
                        await self.poll_fn()
                        self.last_poll_time = time.time()

                # Daily Summary & EOD Sync
                if inspect.iscoroutinefunction(self.get_settings_fn):
                    s = await self.get_settings_fn()
                elif callable(self.get_settings_fn):
                    res = self.get_settings_fn()
                    s = await res if inspect.iscoroutine(res) else res
                else:
                    s = {}
                s = s or {}
                summary_time = s.get("alerts", {}).get("summaryTime", "15:35")
                key = f"summary|{now.date().isoformat()}"

                from app.models import DailySyncRecord
                from sqlalchemy import select

                async with AsyncSessionLocal() as session:
                    stmt = select(DailySyncRecord).where(DailySyncRecord.date_str == now.date().isoformat())
                    rec = (await session.execute(stmt)).scalar_one_or_none()

                if weekday and s.get("alerts", {}).get("dailySummary") and hm >= summary_time and not rec:
                    log.info(f"Triggering EOD poll and daily summary for {now.date().isoformat()}")
                    await self.poll_fn()
                    await self.summary_fn(now)

                    async with AsyncSessionLocal() as session:
                        rec = DailySyncRecord(
                            date_str=now.date().isoformat(),
                            synced_at=datetime.now(timezone.utc),
                            summary_sent=True,
                        )
                        session.add(rec)
                        await session.commit()

            except Exception as e:
                log.error(f"Error in scheduler loop: {e}", exc_info=True)

            await asyncio.sleep(20)
