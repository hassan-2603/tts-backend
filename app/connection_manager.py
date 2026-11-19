"""50-Account Concurrency Manager & Connection Supervisor.

Coordinates independent asynchronous event loops for each linked trading account.
Provides:
- Bounded concurrency with asyncio.Semaphore
- Independent per-account state machines & error boundaries
- Circuit breaker for failing accounts to protect broker API limits
- Event ingestion queues with backpressure
- Health and latency monitoring
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any, Callable, Coroutine
from app.config import CFG
from app.zebu import ZebuClient, SessionExpired, ZebuError
from app.zebu_ws import ZebuWebSocketClient

log = logging.getLogger("tts.manager")


class AccountConnectionState:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.status = "DISCONNECTED"  # REAL-TIME, DEGRADED, REAUTH_REQUIRED, ERROR, DISCONNECTED
        self.ws_client: Optional[ZebuWebSocketClient] = None
        self.last_sync_time: Optional[datetime] = None
        self.last_event_time: Optional[datetime] = None
        self.consecutive_errors = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.processing_task: Optional[asyncio.Task] = None

    def record_success(self):
        self.consecutive_errors = 0
        self.circuit_open = False
        self.circuit_open_until = None
        self.last_sync_time = datetime.now(timezone.utc)

    def record_failure(self, error: Exception):
        self.consecutive_errors += 1
        if self.consecutive_errors >= 5:
            self.circuit_open = True
            self.circuit_open_until = datetime.now(timezone.utc) + timedelta(minutes=5)
            log.warning(f"Circuit breaker OPEN for account {self.account_id} until {self.circuit_open_until}")

    def is_available(self) -> bool:
        if self.circuit_open:
            if datetime.now(timezone.utc) > self.circuit_open_until:
                log.info(f"Circuit breaker HALF-OPEN for account {self.account_id}")
                self.circuit_open = False
                return True
            return False
        return True


class ConnectionManager:
    """Manages active broker sessions and streams for up to 50+ concurrent accounts."""

    def __init__(self, zebu_client: ZebuClient):
        self.zebu = zebu_client
        self.accounts: Dict[str, AccountConnectionState] = {}
        self.semaphore = asyncio.Semaphore(CFG.ACCOUNT_SYNC_WORKERS)
        self._running = False
        self.on_fill_callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None

    async def start(self):
        self._running = True
        log.info(f"ConnectionManager started with concurrency limit {CFG.ACCOUNT_SYNC_WORKERS}")

    async def stop(self):
        self._running = False
        for aid, state in list(self.accounts.items()):
            if state.ws_client:
                await state.ws_client.stop()
            if state.processing_task:
                state.processing_task.cancel()
        self.accounts.clear()
        log.info("ConnectionManager stopped.")

    def get_account_state(self, account_id: str) -> AccountConnectionState:
        if account_id not in self.accounts:
            self.accounts[account_id] = AccountConnectionState(account_id)
        return self.accounts[account_id]

    async def register_account_stream(
        self,
        account_id: str,
        uid: str,
        actid: str,
        token: str,
        on_fill: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None,
        on_touchline: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None,
    ):
        """Attaches and starts a real-time WebSocket connection for an account."""
        state = self.get_account_state(account_id)

        # Stop existing client if any
        if state.ws_client:
            await state.ws_client.stop()

        async def _on_order_update(msg: Dict[str, Any]):
            state.last_event_time = datetime.now(timezone.utc)
            status = str(msg.get("status", "")).upper()
            rpt = str(msg.get("reporttype") or msg.get("rpt") or "").upper()
            flqty = float(msg.get("flqty") or msg.get("fillshares") or 0)
            if status in ("COMPLETE", "REJECTED", "CANCELED") or rpt in ("FILL", "REPLACED", "REJECTED", "CANCELED") or flqty > 0 or msg.get("flprc"):
                try:
                    state.event_queue.put_nowait(msg)
                except asyncio.QueueFull:
                    log.warning(f"Event queue full for {account_id}, dropping old event")
                    state.event_queue.get_nowait()
                    state.event_queue.put_nowait(msg)

                if on_fill:
                    try:
                        await on_fill(msg)
                    except Exception as e:
                        log.error(f"Error executing on_fill callback for {account_id}: {e}")

        async def _on_touchline_update(msg: Dict[str, Any]):
            state.last_event_time = datetime.now(timezone.utc)
            if on_touchline:
                try:
                    await on_touchline(msg)
                except Exception as e:
                    log.debug(f"Touchline callback error for {account_id}: {e}")

        async def _on_status_change(new_status: str, msg: str):
            state.status = new_status
            log.info(f"Account {account_id} WS status changed to {new_status}: {msg}")

        ws_client = ZebuWebSocketClient(
            ws_url=CFG.ZEBU_WS_URL,
            account_id=account_id,
            uid=uid,
            actid=actid,
            token=token,
            on_order_update=_on_order_update,
            on_touchline_update=_on_touchline_update,
            on_status_change=_on_status_change,
        )
        state.ws_client = ws_client
        await ws_client.start()

    async def subscribe_account_touchline(self, account_id: str, scriplist: list[str]):
        state = self.accounts.get(account_id)
        if state and state.ws_client and scriplist:
            await state.ws_client.subscribe_touchline(scriplist)

    async def unregister_account(self, account_id: str):
        if account_id in self.accounts:
            state = self.accounts[account_id]
            if state.ws_client:
                await state.ws_client.stop()
            del self.accounts[account_id]

    async def sync_account_bounded(self, account: Dict[str, Any], sync_coro_fn) -> Any:
        """Executes REST sync with semaphore concurrency limit and circuit breaking."""
        aid = account["id"]
        state = self.get_account_state(aid)

        if not state.is_available():
            return f"skipped (circuit breaker open until {state.circuit_open_until})"

        async with self.semaphore:
            try:
                res = await sync_coro_fn(account)
                state.record_success()
                return res
            except SessionExpired:
                state.status = "REAUTH_REQUIRED"
                state.record_failure(SessionExpired("Session expired"))
                return "expired"
            except Exception as e:
                state.record_failure(e)
                log.error(f"Error syncing account {aid}: {e}")
                return f"error: {str(e)[:200]}"
