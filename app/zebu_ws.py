"""Real-Time Zebu / Noren WebSocket Streaming Client.

Implements official Noren WebSocket protocol:
- Handshake: {"t": "c", "uid": uid, "actid": actid, "susertoken": token, "source": "API"}
- Ack: {"t": "ck", "s": "OK"}
- Order subscription: {"t": "o", "actid": actid"}
- Order messages: {"t": "om", ...}
- Touchline: {"t": "t", "k": "..."}
- Heartbeats: {"t": "h"}
- Automatic reconnection with exponential backoff & jitter
- Graceful degradation if WebSocket is not permitted
"""
import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from typing import Callable, Coroutine, Dict, Optional, Any, Tuple
import websockets
import websockets.asyncio.client as _ws_modern

log = logging.getLogger("tts.ws")

_GENUINE_WS_AUTH_PHRASES = (
    "invalid session",
    "session expired",
    "invalid token",
    "token expired",
    "invalid susertoken",
    "invalid key",
    "unauthorized",
)


def _is_genuine_ws_auth_failure(emsg: str) -> bool:
    low = (emsg or "").strip().lower()
    return any(p in low for p in _GENUINE_WS_AUTH_PHRASES)


class ZebuWebSocketClient:
    def __init__(
        self,
        ws_url: str,
        account_id: str,
        uid: str,
        actid: str,
        token: str,
        on_order_update: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None,
        on_touchline_update: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None,
        on_status_change: Optional[Callable[[str, str], Coroutine[Any, Any, None]]] = None,
        auth_mode: str = "oauth",
        on_auth_failure: Optional[Callable[[str, str], Coroutine[Any, Any, Optional[Tuple[str, str]]]]] = None,
    ):
        self.ws_url = ws_url
        self.account_id = account_id
        self.uid = uid
        self.actid = actid or uid
        self.token = token
        self.auth_mode = (auth_mode or "oauth").strip().lower()
        self.on_order_update = on_order_update
        self.on_touchline_update = on_touchline_update
        self.on_status_change = on_status_change
        self.on_auth_failure = on_auth_failure

        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._status = "DISCONNECTED"
        self._last_event_time: Optional[datetime] = None
        self._subscribed_scrips: set[str] = set()

    @property
    def status(self) -> str:
        return self._status

    @property
    def last_event_time(self) -> Optional[datetime]:
        return self._last_event_time

    async def subscribe_touchline(self, scriplist: list[str]):
        """Subscribe to real-time Touchline LTP ticks (e.g. ['NFO|41003', 'NSE|26000'])."""
        clean = [s.strip() for s in (scriplist or []) if s and "|" in str(s)]
        if not clean:
            return
        new_scrips = [s for s in clean if s not in self._subscribed_scrips]
        self._subscribed_scrips.update(clean)
        if self._ws and self._status == "REAL-TIME" and new_scrips:
            try:
                await self._ws.send(json.dumps({"t": "t", "k": "#".join(new_scrips)}))
            except Exception as e:
                log.debug(f"Touchline subscribe error for {self.account_id}: {e}")

    async def unsubscribe_touchline(self, scriplist: list[str]):
        """Unsubscribe from Touchline LTP ticks using official Zebu task 'u'."""
        clean = [s.strip() for s in (scriplist or []) if s and "|" in str(s)]
        if not clean:
            return
        to_remove = [s for s in clean if s in self._subscribed_scrips]
        for s in to_remove:
            self._subscribed_scrips.discard(s)
        if self._ws and self._status == "REAL-TIME" and to_remove:
            try:
                await self._ws.send(json.dumps({"t": "u", "k": "#".join(to_remove)}))
            except Exception as e:
                log.debug(f"Touchline unsubscribe error for {self.account_id}: {e}")

    async def set_touchline_subscriptions(self, scriplist: list[str]):
        """Reconciles Touchline subscriptions so only the exact open-position scrips are subscribed."""
        desired = {s.strip() for s in (scriplist or []) if s and "|" in str(s)}
        to_unsub = sorted(self._subscribed_scrips - desired)
        to_sub = sorted(desired - self._subscribed_scrips)
        if to_unsub:
            await self.unsubscribe_touchline(to_unsub)
        if to_sub:
            await self.subscribe_touchline(to_sub)

    async def start(self):
        """Start the persistent WebSocket connection in a background task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """Gracefully stop the WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._set_status("DISCONNECTED")

    async def _set_status(self, new_status: str, message: str = ""):
        self._status = new_status
        if self.on_status_change:
            try:
                await self.on_status_change(new_status, message)
            except Exception as e:
                log.error(f"Error in on_status_change callback for {self.account_id}: {e}")

    async def _run_loop(self):
        retry_delay = 1.0
        max_delay = 60.0

        while self._running:
            try:
                log.info(f"Connecting WebSocket for account {self.account_id} to {self.ws_url}")
                # Use modern websockets.asyncio.client API (websockets >= 13.0)
                async with _ws_modern.connect(
                    self.ws_url,
                    ping_interval=None,  # We manage Noren custom heartbeats manually
                    close_timeout=10,
                ) as ws:
                    self._ws = ws

                    # 1. Handshake (QuickAuth Noren TP task 'c' vs OAuth task 'a')
                    if self.auth_mode == "quickauth":
                        handshake = {
                            "t": "c",
                            "uid": self.uid,
                            "actid": self.actid or self.uid,
                            "susertoken": self.token,
                            "source": "API",
                        }
                    else:
                        handshake = {
                            "t": "a",
                            "uid": self.uid,
                            "actid": self.actid or self.uid,
                            "accesstoken": self.token,
                            "source": "API",
                        }
                    await ws.send(json.dumps(handshake))

                    # 2. Receive Handshake Ack (t='ak' or 'ck' with s='OK')
                    ack_raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
                    ack = json.loads(ack_raw) if isinstance(ack_raw, str) else json.loads(ack_raw.decode("utf-8"))

                    if ack.get("t") not in ("ak", "ck") or str(ack.get("s", "")).upper() != "OK":
                        emsg = str(ack.get("emsg") or ack.get("s") or "Handshake failed")
                        log.warning(f"WebSocket handshake failed for {self.account_id}: {emsg}")
                        if _is_genuine_ws_auth_failure(emsg):
                            if self.on_auth_failure:
                                try:
                                    recovered = await self.on_auth_failure(self.account_id, self.token)
                                    if recovered and recovered[0]:
                                        self.token, self.auth_mode = recovered[0], (recovered[1] or self.auth_mode).strip().lower()
                                        log.info(f"WebSocket recovered fresh token via SessionManager for {self.account_id}; reconnecting")
                                        retry_delay = 1.0
                                        continue
                                except Exception as re_err:
                                    log.warning(f"WebSocket centralized recovery failed for {self.account_id}: {re_err}")
                            await self._set_status("REAUTH_REQUIRED", f"WS auth failed: {emsg}")
                            self._running = False
                            return
                        # Transient / ambiguous handshake failure (including generic Not_Ok): back off & retry without false REAUTH_REQUIRED
                        await self._set_status("DEGRADED", f"Transient handshake failure: {emsg}")
                        jitter = random.uniform(0.5, 1.5)
                        await asyncio.sleep(min(retry_delay * jitter, max_delay))
                        retry_delay = min(retry_delay * 2, max_delay)
                        continue

                    # 3. Connection Established
                    await self._set_status("REAL-TIME")
                    retry_delay = 1.0  # Reset backoff on success

                    # 4. Subscribe to Order Updates & Active Touchline Scrips
                    order_sub = {"t": "o", "actid": self.actid}
                    await ws.send(json.dumps(order_sub))
                    if self._subscribed_scrips:
                        await ws.send(json.dumps({"t": "t", "k": "#".join(sorted(self._subscribed_scrips))}))

                    # 5. Heartbeat & Message Processing
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
                    try:
                        async for raw_msg in ws:
                            if not self._running:
                                break
                            self._last_event_time = datetime.now(timezone.utc)
                            msg = json.loads(raw_msg) if isinstance(raw_msg, str) else json.loads(raw_msg.decode("utf-8"))
                            await self._handle_message(msg)
                    finally:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

            except (websockets.exceptions.ConnectionClosed, ConnectionError, asyncio.TimeoutError) as e:
                log.warning(f"WebSocket disconnected for {self.account_id}: {e}")
                await self._set_status("DEGRADED", f"Disconnected: {e}")
            except Exception as e:
                log.error(f"Unexpected WebSocket error for {self.account_id}: {e}", exc_info=True)
                await self._set_status("DEGRADED", str(e))

            if self._running:
                jitter = random.uniform(0.5, 1.5)
                sleep_time = min(retry_delay * jitter, max_delay)
                log.info(f"Reconnecting {self.account_id} in {sleep_time:.2f}s...")
                await asyncio.sleep(sleep_time)
                retry_delay = min(retry_delay * 2, max_delay)

    async def _heartbeat_loop(self, ws):
        """Sends Noren {"t": "h"} heartbeat every 30 seconds."""
        while self._running:
            try:
                await asyncio.sleep(30)
                await ws.send(json.dumps({"t": "h"}))
            except Exception:
                break

    async def _handle_message(self, msg: Dict[str, Any]):
        """Dispatches incoming Noren/Zebu OAuth WebSocket messages."""
        t = msg.get("t")
        if t == "om":
            # Order update event
            if self.on_order_update:
                await self.on_order_update(msg)
        elif t in ("tk", "tf"):
            # Touchline ack ('tk') or live price feed ('tf')
            if self.on_touchline_update:
                await self.on_touchline_update(msg)
        elif t == "uk":
            # Touchline unsubscription acknowledgement
            log.debug(f"Touchline unsubscribe ack for {self.account_id}: {msg.get('e')}|{msg.get('tk')}")
        elif t in ("ak", "ck"):
            pass  # Connection/heartbeat acknowledgement
