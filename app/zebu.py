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
        self._proxy_async_clients: Dict[str, httpx.AsyncClient] = {}
        self._proxy_sync_clients: Dict[str, httpx.Client] = {}

    def _clean_proxy(self, proxy: Optional[str]) -> Optional[str]:
        if not proxy:
            return None
        p = proxy.strip()
        if not p:
            return None
        if ":PORT" in p.upper() or p.endswith(":"):
            raise ZebuError("Invalid proxy port: You entered the placeholder word 'PORT'. Please replace 'PORT' with the actual port number from your proxy provider (e.g. 8080, 7492, 3128).")
        if not (p.startswith("http://") or p.startswith("https://") or p.startswith("socks5://")):
            p = f"http://{p}"
        return p

    def _get_sync_http(self, proxy: Optional[str] = None) -> httpx.Client:
        proxy_clean = self._clean_proxy(proxy)
        if proxy_clean:
            cl = self._proxy_sync_clients.get(proxy_clean)
            if cl is None or cl.is_closed:
                cl = httpx.Client(timeout=self.timeout, proxy=proxy_clean)
                self._proxy_sync_clients[proxy_clean] = cl
            return cl
        if self._sync_http is None or self._sync_http.is_closed:
            self._sync_http = httpx.Client(timeout=self.timeout)
        return self._sync_http

    def _get_async_http(self, proxy: Optional[str] = None) -> httpx.AsyncClient:
        proxy_clean = self._clean_proxy(proxy)
        if proxy_clean:
            cl = self._proxy_async_clients.get(proxy_clean)
            if cl is None or cl.is_closed:
                proxy_timeout = httpx.Timeout(connect=15.0, read=20.0, write=20.0, pool=15.0)
                limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
                cl = httpx.AsyncClient(timeout=proxy_timeout, limits=limits, proxy=proxy_clean)
                self._proxy_async_clients[proxy_clean] = cl
            return cl
        if self._async_http is None or self._async_http.is_closed:
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
            self._async_http = httpx.AsyncClient(timeout=self.timeout, limits=limits)
        return self._async_http

    async def aclose(self):
        if self._async_http and not self._async_http.is_closed:
            await self._async_http.aclose()
        for cl in list(self._proxy_async_clients.values()):
            if not cl.is_closed:
                await cl.aclose()
        self._proxy_async_clients.clear()
        if self._sync_http and not self._sync_http.is_closed:
            self._sync_http.close()
        for cl in list(self._proxy_sync_clients.values()):
            if not cl.is_closed:
                cl.close()
        self._proxy_sync_clients.clear()

    # ---------- OAuth ----------
    def authorize_url(self, client_id: str = "", state: str = "", redirect_uri: str = "") -> str:
        cid = (client_id or self.client_id).strip()
        if cid and not cid.endswith("_U") and "_" not in cid:
            cid = f"{cid}_U"
        url = f"{self.base}/OAuthlogin/authorize/oauth?client_id={cid}&prompt=login"
        if redirect_uri:
            import urllib.parse
            url += f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        if state:
            url += f"&state={state}"
        return url

    def checksum(self, code: str, client_id: str = "", secret_key: str = "") -> str:
        cid = (client_id or self.client_id).strip()
        sec = (secret_key or self.secret_key).strip()
        return hashlib.sha256(f"{cid}{sec}{code}".encode()).hexdigest()

    async def exchange_code_async(
        self,
        code: str,
        uid: str = "",
        client_id: str = "",
        secret_key: str = "",
        proxy: Optional[str] = None,
    ) -> dict:
        """Returns {access_token, refresh_token, expires_in, ...} following official Zebu myntapi standard."""
        errors = []
        clean_uid = uid.strip().upper() if uid else ""
        cid = (client_id or self.client_id).strip()
        sec = (secret_key or self.secret_key).strip()

        # Format clean client_id (Zebu API app keys always end with _U)
        primary_cid = cid
        if not primary_cid.endswith("_U") and "_" not in primary_cid:
            primary_cid = f"{primary_cid}_U"

        # Candidate client IDs to test (primary_cid first, then fallback without _U if needed)
        cids = [primary_cid]
        if primary_cid.endswith("_U"):
            cids.append(primary_cid[:-2])
        if clean_uid and clean_uid not in cids:
            cids.append(f"{clean_uid}_U" if not clean_uid.endswith("_U") else clean_uid)
        cids = list(dict.fromkeys([k for k in cids if k]))

        # Candidate secret keys to test (account-level first, then server fallback)
        secs = [sec]
        if self.secret_key and self.secret_key != sec:
            secs.append(self.secret_key)
        secs = list(dict.fromkeys([s for s in secs if s]))

        for s in secs:
            for k in cids:
                # Official Zebu checksum: SHA256(client_id + secret_key + code)
                cs = hashlib.sha256(f"{k}{s}{code}".encode("utf-8")).hexdigest()
                payload = {"code": code, "checksum": cs}

                try:
                    r = await self._post_async("GenAcsTok", payload, token=None, retries=0, proxy=proxy)
                    tok = (r.get("access_token") or r.get("susertoken")) if isinstance(r, dict) else None
                    if tok and (r.get("stat", "").upper() == "OK" or "stat" not in r):
                        r["access_token"] = tok
                        return r
                    if isinstance(r, dict) and r.get("emsg"):
                        emsg = r["emsg"]
                        errors.append(f"{k}: {emsg}")
                        if "INVALID_IP" in emsg:
                            raise ZebuError(f"{k}: {emsg}")
                except Exception as e:
                    err_str = str(e)
                    errors.append(f"{k}: {err_str}")
                    if "INVALID_IP" in err_str or "Proxy Connection Error" in err_str:
                        raise ZebuError(err_str) from e

        raise ZebuError("; ".join(dict.fromkeys(errors)) or "Token exchange failed")

    def exchange_code(
        self,
        code: str,
        uid: str = "",
        client_id: str = "",
        secret_key: str = "",
    ) -> dict:
        errors = []
        clean_uid = uid.strip().upper() if uid else ""
        cid = (client_id or self.client_id).strip()
        sec = (secret_key or self.secret_key).strip()

        cids = [cid]
        if cid.endswith("_U"):
            cids.append(cid[:-2])
        else:
            cids.append(f"{cid}_U")
        if clean_uid and clean_uid not in cids:
            cids.append(clean_uid)
            cids.append(f"{clean_uid}_U")
        if self.client_id and self.client_id not in cids:
            cids.append(self.client_id)
        cids = list(dict.fromkeys([k for k in cids if k]))

        secs = [sec]
        if self.secret_key and self.secret_key != sec:
            secs.append(self.secret_key)
        secs = list(dict.fromkeys([s for s in secs if s]))

        for s in secs:
            for k in cids:
                cs = hashlib.sha256(f"{k}{s}{code}".encode("utf-8")).hexdigest()
                payloads = [
                    {"code": code, "checksum": cs},
                    {"code": code, "appkey": cs, "checksum": cs},
                ]
                if clean_uid:
                    payloads.append({"code": code, "checksum": cs, "uid": clean_uid})

                for p in payloads:
                    try:
                        r = self._post_sync("GenAcsTok", p, token=None)
                        tok = (r.get("access_token") or r.get("susertoken")) if isinstance(r, dict) else None
                        if tok and (r.get("stat", "").upper() == "OK" or "stat" not in r):
                            r["access_token"] = tok
                            return r
                        if isinstance(r, dict) and r.get("emsg"):
                            emsg = r["emsg"]
                            errors.append(f"{k}: {emsg}")
                            if "INVALID_IP" in emsg:
                                raise ZebuError(f"{k}: {emsg}")
                    except Exception as e:
                        err_str = str(e)
                        errors.append(f"{k}: {err_str}")
                        if "INVALID_IP" in err_str or "Proxy Connection Error" in err_str:
                            raise ZebuError(err_str) from e

        raise ZebuError("; ".join(dict.fromkeys(errors)) or "Token exchange failed")

    async def refresh_token_async(
        self,
        refresh_token: str,
        uid: str = "",
        client_id: str = "",
        secret_key: str = "",
        proxy: Optional[str] = None,
    ) -> Optional[dict]:
        """Attempts official OAuth refresh_token exchange via GenAcsTok if supported by Zebu."""
        if not refresh_token:
            return None
        try:
            r = await self.exchange_code_async(
                code=refresh_token,
                uid=uid,
                client_id=client_id,
                secret_key=secret_key,
                proxy=proxy,
            )
            if isinstance(r, dict) and r.get("access_token"):
                return r
        except Exception as e:
            log.debug(f"Token refresh attempt failed for {uid}: {e}")
        return None

    # ---------- Low Level Async Request with Exponential Backoff ----------
    async def _post_async(self, endpoint: str, payload: dict, token: Optional[str] = None, retries: int = 2, proxy: Optional[str] = None) -> Any:
        if endpoint not in READ_ONLY_ENDPOINTS:
            raise PermissionError(f"{endpoint} is not allowed — tracking software is strictly read-only")

        headers = {"Content-Type": "text/plain"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = "jData=" + json.dumps(payload, separators=(",", ":"))
        if token:
            body += f"&jKey={token}"
        client = self._get_async_http(proxy=proxy)

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
            except (httpx.ProxyError, httpx.ConnectError, httpx.TimeoutException, RateLimitError) as e:
                last_ex = e
                if proxy and (isinstance(e, httpx.ProxyError) or "proxy" in str(e).lower() or isinstance(e, (httpx.ConnectTimeout, httpx.ConnectError))):
                    raise ZebuError(f"Proxy Connection Error: Unable to reach proxy '{proxy}'. Please verify your proxy host, port, and authentication credentials.") from e
                if attempt < retries:
                    backoff = (2 ** attempt) * 0.5
                    await asyncio.sleep(backoff)
                else:
                    raise ZebuError(f"{endpoint} network error: {e}") from e

        if last_ex:
            raise ZebuError(f"{endpoint} failed: {last_ex}")

    # ---------- Low Level Sync Request (Preserved for compatibility) ----------
    def _post_sync(self, endpoint: str, payload: dict, token: Optional[str] = None, proxy: Optional[str] = None) -> Any:
        if endpoint not in READ_ONLY_ENDPOINTS:
            raise PermissionError(f"{endpoint} is not allowed — tracking software is strictly read-only")

        headers = {"Content-Type": "text/plain"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = "jData=" + json.dumps(payload, separators=(",", ":"))
        if token:
            body += f"&jKey={token}"
        client = self._get_sync_http(proxy=proxy)

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

    def _post(self, endpoint: str, payload: dict, token: Optional[str] = None, proxy: Optional[str] = None) -> Any:
        return self._post_sync(endpoint, payload, token, proxy=proxy)

    def _clean_uid(self, u: Optional[str]) -> str:
        s = (u or "").strip().upper()
        return s[:-2] if s.endswith("_U") else s

    # ---------- Async Read Calls ----------
    async def client_details_async(self, token: str, uid: str, actid: str, proxy: Optional[str] = None) -> dict:
        u = self._clean_uid(uid)
        a = self._clean_uid(actid) or u
        return await self._post_async("ClientDetails", {"uid": u, "actid": a}, token, proxy=proxy)

    async def trade_book_async(self, token: str, uid: str, actid: str, proxy: Optional[str] = None) -> List[dict]:
        u = self._clean_uid(uid)
        a = self._clean_uid(actid) or u
        r = await self._post_async("TradeBook", {"uid": u, "actid": a}, token, proxy=proxy)
        return r if isinstance(r, list) else []

    async def order_book_async(self, token: str, uid: str, proxy: Optional[str] = None) -> List[dict]:
        u = self._clean_uid(uid)
        r = await self._post_async("OrderBook", {"uid": u}, token, proxy=proxy)
        return r if isinstance(r, list) else []

    async def position_book_async(self, token: str, uid: str, actid: str, proxy: Optional[str] = None) -> List[dict]:
        u = self._clean_uid(uid)
        a = self._clean_uid(actid) or u
        r = await self._post_async("PositionBook", {"uid": u, "actid": a}, token, proxy=proxy)
        return r if isinstance(r, list) else []

    async def tp_series_async(self, token: str, uid: str, exch: str, sym_token: Union[str, int], st: int, et: int, intrv: str = "1", proxy: Optional[str] = None) -> List[dict]:
        u = self._clean_uid(uid)
        r = await self._post_async(
            "TPSeries",
            {"uid": u, "exch": exch, "token": str(sym_token), "st": str(st), "et": str(et), "intrv": intrv},
            token,
            proxy=proxy,
        )
        return r if isinstance(r, list) else []

    async def index_list_async(self, token: str, uid: str, exch: str = "NSE", proxy: Optional[str] = None) -> List[dict]:
        u = self._clean_uid(uid)
        r = await self._post_async("GetIndexList", {"uid": u, "exch": exch}, token, proxy=proxy)
        return (r or {}).get("values", []) if isinstance(r, dict) else []

    async def quote_async(self, token: str, uid: str, exch: str, sym_token: Union[str, int], proxy: Optional[str] = None) -> dict:
        u = self._clean_uid(uid)
        return await self._post_async("GetQuotes", {"uid": u, "exch": exch, "token": str(sym_token)}, token, proxy=proxy)

    # ---------- Sync Read Calls (Backward Compatibility) ----------
    def client_details(self, token: str, uid: str, actid: str):
        u = self._clean_uid(uid)
        a = self._clean_uid(actid) or u
        return self._post_sync("ClientDetails", {"uid": u, "actid": a}, token)

    def trade_book(self, token: str, uid: str, actid: str):
        u = self._clean_uid(uid)
        a = self._clean_uid(actid) or u
        r = self._post_sync("TradeBook", {"uid": u, "actid": a}, token)
        return r if isinstance(r, list) else []

    def order_book(self, token: str, uid: str):
        u = self._clean_uid(uid)
        r = self._post_sync("OrderBook", {"uid": u}, token)
        return r if isinstance(r, list) else []

    def position_book(self, token: str, uid: str, actid: str):
        u = self._clean_uid(uid)
        a = self._clean_uid(actid) or u
        r = self._post_sync("PositionBook", {"uid": u, "actid": a}, token)
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
