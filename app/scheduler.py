"""Singleton Distributed Scheduler for Market Hours, EOD Finalization & Midnight Archiving.

Uses PostgreSQL advisory lock or Redis distributed lock to ensure that even when
multiple Uvicorn worker processes are running, only ONE singleton scheduler executes.
Consistently uses IST (Asia/Kolkata) for all trading-day calculations and scheduling.
"""
import asyncio
import inspect
import logging
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Callable, Coroutine, Any
from sqlalchemy import text, select
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
                    if session.bind and "sqlite" not in str(session.bind.url):
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
        get_settings_fn: Callable[[], Any],
        finalize_eod_fn: Optional[Callable[[datetime], Coroutine[Any, Any, Any]]] = None,
        midnight_archive_fn: Optional[Callable[[datetime], Coroutine[Any, Any, Any]]] = None,
    ):
        self.poll_fn = poll_fn
        self.summary_fn = summary_fn
        self.get_settings_fn = get_settings_fn
        self.finalize_eod_fn = finalize_eod_fn
        self.midnight_archive_fn = midnight_archive_fn
        self.lock = DistributedLock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_poll_time = 0.0
        self.last_eod_retry_time = 0.0
        self.last_midnight_check_date: Optional[str] = None

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
                    await asyncio.sleep(15)
                    continue

                now = now_ist()
                hm = now.strftime("%H:%M")
                today_str = now.date().isoformat()
                weekday = now.weekday() < 5

                # 1. Midnight Cleanup & Daily Archiving (runs at/after 00:00 IST or on recovery after restart)
                if self.midnight_archive_fn and self.last_midnight_check_date != today_str:
                    log.info(f"Running midnight / recovery archive check for date < {today_str} IST")
                    await self.midnight_archive_fn(now)
                    self.last_midnight_check_date = today_str

                # 2. Live Market Hours Polling (09:15 to 15:30 IST on weekdays)
                market_open = CFG.MARKET_OPEN or "09:15"
                market_close = CFG.MARKET_CLOSE or "15:30"
                if weekday and market_open <= hm <= market_close:
                    if time.time() - self.last_poll_time >= CFG.POLL_SECONDS:
                        log.info(f"Triggering scheduled market-hours poll at {hm} IST")
                        await self.poll_fn()
                        self.last_poll_time = time.time()

                # 3. End-of-Day Finalization & Daily Summary (after 15:30 IST)
                if inspect.iscoroutinefunction(self.get_settings_fn):
                    s = await self.get_settings_fn()
                elif callable(self.get_settings_fn):
                    res = self.get_settings_fn()
                    s = await res if inspect.iscoroutine(res) else res
                else:
                    s = {}
                s = s or {}
                summary_time = s.get("alerts", {}).get("summaryTime", "15:35")
                eod_time = getattr(CFG, "EOD_FINALIZE_TIME", "15:31") or "15:31"

                from app.models import DailySyncRecord, DailyAccountReport

                async with AsyncSessionLocal() as session:
                    stmt = select(DailySyncRecord).where(DailySyncRecord.date_str == today_str)
                    rec = (await session.execute(stmt)).scalar_one_or_none()
                    pending_stmt = select(DailyAccountReport).where(
                        DailyAccountReport.trade_date == today_str,
                        DailyAccountReport.status == "PENDING_RECONCILIATION",
                    )
                    has_pending = (await session.execute(pending_stmt)).scalars().first() is not None

                if weekday and hm >= market_close:
                    # Trigger EOD finalization (also locks final snapshot at 21:00 IST with zero Zebu calls after 21:00 IST)
                    should_finalize = (not rec or not getattr(rec, "finalized", False) or has_pending)
                    if should_finalize and self.finalize_eod_fn and (time.time() - self.last_eod_retry_time >= 60):
                        log.info(f"Triggering EOD reconciliation and finalization for {today_str} at {hm} IST")
                        self.last_eod_retry_time = time.time()
                        eod_res = await self.finalize_eod_fn(now)
                        all_ok = not isinstance(eod_res, dict) or eod_res.get("pending_accounts", 0) == 0

                        async with AsyncSessionLocal() as session:
                            stmt = select(DailySyncRecord).where(DailySyncRecord.date_str == today_str)
                            cur_rec = (await session.execute(stmt)).scalar_one_or_none()
                            if not cur_rec:
                                cur_rec = DailySyncRecord(
                                    date_str=today_str,
                                    synced_at=datetime.now(timezone.utc),
                                    summary_sent=False,
                                    finalized=all_ok,
                                )
                                session.add(cur_rec)
                            else:
                                cur_rec.synced_at = datetime.now(timezone.utc)
                                cur_rec.finalized = all_ok
                            await session.commit()

                    # Daily summary alert
                    if s.get("alerts", {}).get("dailySummary") and hm >= summary_time:
                        async with AsyncSessionLocal() as session:
                            stmt = select(DailySyncRecord).where(DailySyncRecord.date_str == today_str)
                            cur_rec = (await session.execute(stmt)).scalar_one_or_none()
                            if not cur_rec or not cur_rec.summary_sent:
                                await self.summary_fn(now)
                                if not cur_rec:
                                    cur_rec = DailySyncRecord(
                                        date_str=today_str,
                                        synced_at=datetime.now(timezone.utc),
                                        summary_sent=True,
                                    )
                                    session.add(cur_rec)
                                else:
                                    cur_rec.summary_sent = True
                                await session.commit()

            except Exception as e:
                log.error(f"Error in scheduler loop: {e}", exc_info=True)

            await asyncio.sleep(15)
