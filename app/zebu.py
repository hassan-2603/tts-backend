"""Zebu / MYNT (Noren) API client — STRICTLY READ-ONLY.

Docs: https://zebu-apidoc.web.app/OAuth/
Every call: POST {BASE}/NorenWClientAPI/<Endpoint>
            Header  Authorization: Bearer <access_token>
            Content-Type: text/plain
            Body    jData=<json>

Order-placing endpoints (PlaceOrder, ModifyOrder, CancelOrder, GTT, ProductConversion …)
are deliberately NOT in the allow-list below. This software only reads trades.
"""
import hashlib
import json
import logging
import asyncio
from typing import Any, Dict, List, Optional, Union
import httpx

log = logging.getLogger("tts.zebu")

READ_ONLY_ENDPOINTS = {
    "GenAcsTok",      # exchange OAuth code for access token
    "ClientDetails",  # verify the linked account
    "TradeBook",      # every fill of the day
    "OrderBook",      # order list (status, remarks, ordersource)
    "PositionBook",   # open positions + LTP (for open / carried trades)
    "TPSeries",       # 1-min candles -> index spot at entry time
    "GetIndexList",   # find index tokens (Nifty Bank etc.)
    "GetQuotes",      # last price of a contract
    "SearchScrip",    # search contract master token
}


class ZebuError(Exception):
    pass


class SessionExpired(ZebuError):
    pass


class RateLimitError(ZebuError):
    pass


class ZebuClient:
    def __init__(self, base_url: str, client_id: str, secret_key: str, timeout: float = 20.0):
        self.base = base_url.rstrip("/")
        self.client_id = client_id
        self.secret_key = secret_key
        self.timeout = timeout
        self._async_http: Optional[httpx.AsyncClient] = None
        self._sync_http: Optional[httpx.Client] = None

    def _get_sync_http(self) -> httpx.Client:
        if self._sync_http is None or self._sync_http.is_closed:
            self._sync_http = httpx.Client(timeout=self.timeout)
        return self._sync_http

    def _get_async_http(self) -> httpx.AsyncClient:
        if self._async_http is None or self._async_http.is_closed:
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
            self._async_http = httpx.AsyncClient(timeout=self.timeout, limits=limits)
        return self._async_http

    async def aclose(self):
        if self._async_http and not self._async_http.is_closed:
            await self._async_http.aclose()
        if self._sync_http and not self._sync_http.is_closed:
            self._sync_http.close()

    # ---------- OAuth ----------
    def authorize_url(self, state: str = "", redirect_uri: str = "") -> str:
        url = f"{self.base}/OAuthlogin/authorize/oauth?client_id={self.client_id}"
        if redirect_uri:
            import urllib.parse
            url += f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        if state:
            url += f"&state={state}"
        return url

    def checksum(self, code: str) -> str:
        return hashlib.sha256((self.client_id + self.secret_key + code).encode()).hexdigest()

    async def exchange_code_async(self, code: str) -> dict:
        """Returns {access_token, refresh_token, expires_in, ...}"""
        errors = []
        keys = [self.client_id]
        if "_" in self.client_id:
            keys.append(self.client_id.split("_")[0])

        for k in keys:
            # 1. Format: AppKey + SecretKey + Code
            cs1 = hashlib.sha256(f"{k}{self.secret_key}{code}".encode("utf-8")).hexdigest()
            try:
                r = await self._post_async("GenAcsTok", {"code": code, "checksum": cs1}, token=None)
                tok = (r.get("access_token") or r.get("susertoken")) if isinstance(r, dict) else None
                if tok and r.get("stat") == "Ok":
                    r["access_token"] = tok
                    return r
                if isinstance(r, dict) and r.get("emsg"):
                    errors.append(r["emsg"])
            except Exception as e:
                errors.append(str(e))

            # 2. Format: AppKey + Code + SecretKey
            cs2 = hashlib.sha256(f"{k}{code}{self.secret_key}".encode("utf-8")).hexdigest()
            try:
                r = await self._post_async("GenAcsTok", {"code": code, "checksum": cs2}, token=None)
                tok = (r.get("access_token") or r.get("susertoken")) if isinstance(r, dict) else None
                if tok and r.get("stat") == "Ok":
                    r["access_token"] = tok
                    return r
                if isinstance(r, dict) and r.get("emsg"):
                    errors.append(r["emsg"])
            except Exception as e:
                errors.append(str(e))

        raise ZebuError("; ".join(dict.fromkeys(errors)) or "Token exchange failed")

    def exchange_code(self, code: str) -> dict:
        errors = []
        keys = [self.client_id]
        if "_" in self.client_id:
            keys.append(self.client_id.split("_")[0])

        for k in keys:
            cs1 = hashlib.sha256(f"{k}{self.secret_key}{code}".encode("utf-8")).hexdigest()
            try:
                r = self._post_sync("GenAcsTok", {"code": code, "checksum": cs1}, token=None)
                tok = (r.get("access_token") or r.get("susertoken")) if isinstance(r, dict) else None
                if tok and r.get("stat") == "Ok":
                    r["access_token"] = tok
                    return r
                if isinstance(r, dict) and r.get("emsg"):
                    errors.append(r["emsg"])
            except Exception as e:
                errors.append(str(e))

            cs2 = hashlib.sha256(f"{k}{code}{self.secret_key}".encode("utf-8")).hexdigest()
            try:
                r = self._post_sync("GenAcsTok", {"code": code, "checksum": cs2}, token=None)
                tok = (r.get("access_token") or r.get("susertoken")) if isinstance(r, dict) else None
                if tok and r.get("stat") == "Ok":
                    r["access_token"] = tok
                    return r
                if isinstance(r, dict) and r.get("emsg"):
                    errors.append(r["emsg"])
            except Exception as e:
                errors.append(str(e))

        raise ZebuError("; ".join(dict.fromkeys(errors)) or "Token exchange failed")

    # ---------- Low Level Async Request with Exponential Backoff ----------
    async def _post_async(self, endpoint: str, payload: dict, token: Optional[str] = None, retries: int = 2) -> Any:
        if endpoint not in READ_ONLY_ENDPOINTS:
            raise PermissionError(f"{endpoint} is not allowed — tracking software is strictly read-only")

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = "jData=" + json.dumps(payload, separators=(",", ":"))
        if token:
            body += f"&jKey={token}"
        client = self._get_async_http()

        last_ex = None
        for attempt in range(retries + 1):
            try:
                resp = await client.post(f"{self.base}/NorenWClientAPI/{endpoint}", content=body, headers=headers)
                if resp.status_code == 429:
                    raise RateLimitError("Zebu API Rate limit exceeded")
                try:
                    data = resp.json()
                except ValueError:
                    raise ZebuError(f"{endpoint}: HTTP {resp.status_code} non-JSON response")

                if isinstance(data, dict) and data.get("stat") == "Not_Ok":
                    emsg = data.get("emsg", "")
                    low = emsg.lower()
                    if "no data" in low or "no record" in low or "empty" in low or "not found" in low:
                        return []
                    if "session" in low or ("invalid" in low and "key" in low) or "token expired" in low:
                        raise SessionExpired(emsg)
                    raise ZebuError(f"{endpoint}: {emsg}")

                return data
            except (httpx.ConnectError, httpx.TimeoutException, RateLimitError) as e:
                last_ex = e
                if attempt < retries:
                    backoff = (2 ** attempt) * 0.5
                    await asyncio.sleep(backoff)
                else:
                    raise ZebuError(f"{endpoint} network error: {e}") from e

        if last_ex:
            raise ZebuError(f"{endpoint} failed: {last_ex}")

    # ---------- Low Level Sync Request (Preserved for compatibility) ----------
    def _post_sync(self, endpoint: str, payload: dict, token: Optional[str] = None) -> Any:
        if endpoint not in READ_ONLY_ENDPOINTS:
            raise PermissionError(f"{endpoint} is not allowed — tracking software is strictly read-only")

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = "jData=" + json.dumps(payload, separators=(",", ":"))
        if token:
            body += f"&jKey={token}"
        client = self._get_sync_http()

        resp = client.post(f"{self.base}/NorenWClientAPI/{endpoint}", content=body, headers=headers)
        try:
            data = resp.json()
        except ValueError:
            raise ZebuError(f"{endpoint}: HTTP {resp.status_code} non-JSON response")

        if isinstance(data, dict) and data.get("stat") == "Not_Ok":
            emsg = data.get("emsg", "")
            low = emsg.lower()
            if "no data" in low or "no record" in low or "empty" in low or "not found" in low:
                return []
            if "session" in low or ("invalid" in low and "key" in low):
                raise SessionExpired(emsg)
            raise ZebuError(f"{endpoint}: {emsg}")
        return data

    def _post(self, endpoint: str, payload: dict, token: Optional[str] = None) -> Any:
        return self._post_sync(endpoint, payload, token)

    # ---------- Async Read Calls ----------
    async def client_details_async(self, token: str, uid: str, actid: str) -> dict:
        return await self._post_async("ClientDetails", {"uid": uid, "actid": actid}, token)

    async def trade_book_async(self, token: str, uid: str, actid: str) -> List[dict]:
        r = await self._post_async("TradeBook", {"uid": uid, "actid": actid}, token)
        return r if isinstance(r, list) else []

    async def order_book_async(self, token: str, uid: str) -> List[dict]:
        r = await self._post_async("OrderBook", {"uid": uid}, token)
        return r if isinstance(r, list) else []

    async def position_book_async(self, token: str, uid: str, actid: str) -> List[dict]:
        r = await self._post_async("PositionBook", {"uid": uid, "actid": actid}, token)
        return r if isinstance(r, list) else []

    async def tp_series_async(self, token: str, uid: str, exch: str, sym_token: Union[str, int], st: int, et: int, intrv: str = "1") -> List[dict]:
        r = await self._post_async(
            "TPSeries",
            {"uid": uid, "exch": exch, "token": str(sym_token), "st": str(st), "et": str(et), "intrv": intrv},
            token,
        )
        return r if isinstance(r, list) else []

    async def index_list_async(self, token: str, uid: str, exch: str = "NSE") -> List[dict]:
        r = await self._post_async("GetIndexList", {"uid": uid, "exch": exch}, token)
        return (r or {}).get("values", []) if isinstance(r, dict) else []

    async def quote_async(self, token: str, uid: str, exch: str, sym_token: Union[str, int]) -> dict:
        return await self._post_async("GetQuotes", {"uid": uid, "exch": exch, "token": str(sym_token)}, token)

    # ---------- Sync Read Calls (Backward Compatibility) ----------
    def client_details(self, token: str, uid: str, actid: str):
        return self._post_sync("ClientDetails", {"uid": uid, "actid": actid}, token)

    def trade_book(self, token: str, uid: str, actid: str):
        r = self._post_sync("TradeBook", {"uid": uid, "actid": actid}, token)
        return r if isinstance(r, list) else []

    def order_book(self, token: str, uid: str):
        r = self._post_sync("OrderBook", {"uid": uid}, token)
        return r if isinstance(r, list) else []

    def position_book(self, token: str, uid: str, actid: str):
        r = self._post_sync("PositionBook", {"uid": uid, "actid": actid}, token)
        return r if isinstance(r, list) else []

    def tp_series(self, token: str, uid: str, exch: str, sym_token: Union[str, int], st: int, et: int, intrv: str = "1"):
        r = self._post_sync(
            "TPSeries",
            {"uid": uid, "exch": exch, "token": str(sym_token), "st": str(st), "et": str(et), "intrv": intrv},
            token,
        )
        return r if isinstance(r, list) else []

    def index_list(self, token: str, uid: str, exch: str = "NSE"):
        r = self._post_sync("GetIndexList", {"uid": uid, "exch": exch}, token)
        return (r or {}).get("values", []) if isinstance(r, dict) else []

    def quote(self, token: str, uid: str, exch: str, sym_token: Union[str, int]):
        return self._post_sync("GetQuotes", {"uid": uid, "exch": exch, "token": str(sym_token)}, token)
