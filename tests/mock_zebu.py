"""Mock of the Zebu/MYNT Noren OAuth API and Noren WebSocket server for testing."""
import hashlib
import json
import os
import asyncio
from datetime import datetime, timedelta
from urllib.parse import unquote_plus
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, RedirectResponse

CID, SECRET = "TESTCID", "TESTSECRET"
CALLBACK = os.environ.get("MOCK_CALLBACK", "http://127.0.0.1:8000/auth/zebu/callback")
app = FastAPI()
CALLS, TELEGRAM = [], []
TODAY = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%m-%Y")
TSYM_CE, TSYM_CE2, TSYM_PE = "BANKNIFTY24NOV26C51700", "BANKNIFTY24NOV26C52000", "BANKNIFTY24NOV26P51900"


def fill(oid, flid, tsym, tok, side, qty, prc, hhmm, prd="I", src="WEB"):
    return {
        "stat": "Ok",
        "exch": "NFO",
        "tsym": tsym,
        "norenordno": oid,
        "prd": prd,
        "trantype": side,
        "prctyp": "MKT",
        "fillshares": str(qty),
        "qty": str(qty),
        "flqty": str(qty),
        "flprc": f"{prc:.2f}",
        "flid": flid,
        "fltm": f"{TODAY} {hhmm}:12",
        "exch_tm": f"{TODAY} {hhmm}:12",
        "ordersource": src,
        "remarks": "",
        "uid": "ZP1001",
        "actid": "ZP1001",
        "ls": "30",
        "ti": "0.05",
        "pp": "2",
        "token": tok,
        "ret": "DAY",
    }


FILLS = [
    fill("2611130001", "1", TSYM_CE, "41001", "B", 30, 300.0, "10:05", src="API"),
    fill("2611130002", "2", TSYM_CE, "41001", "S", 30, 340.0, "10:40", src="API"),
    fill("2611130003", "3", TSYM_CE2, "41002", "B", 60, 310.0, "10:42"),
    fill("2611130004", "4", TSYM_CE2, "41002", "S", 60, 295.0, "11:05"),
    fill("2611130005", "5", TSYM_PE, "41003", "B", 15, 280.0, "14:48", prd="M"),
    fill("2611130005", "6", TSYM_PE, "41003", "B", 15, 282.0, "14:48", prd="M"),
]


async def jdata(req: Request):
    raw = (await req.body()).decode()
    assert req.headers.get("content-type", "").startswith("text/plain"), "Content-Type must be text/plain"
    assert raw.startswith("jData="), "body must be jData=<json>"
    return json.loads(unquote_plus(raw[6:]) if "%7B" in raw[:10] else raw[6:])


def authed(req):
    return req.headers.get("authorization") == "Bearer ACCESS-OK"


@app.get("/OAuthlogin/authorize/oauth")
def authorize(client_id: str):
    assert client_id == CID
    return RedirectResponse(f"{CALLBACK}?code=AUTHCODE123")


@app.post("/NorenWClientAPI/{ep}")
async def noren(ep: str, req: Request):
    CALLS.append(ep)
    d = await jdata(req)
    if ep == "GenAcsTok":
        ok = d.get("checksum") == hashlib.sha256((CID + SECRET + d.get("code", "")).encode()).hexdigest()
        return {
            "stat": "Ok",
            "access_token": "ACCESS-OK",
            "refresh_token": "R",
            "expires_in": "3600",
            "request_time": "10:06:54 21-01-2023",
        } if ok else {"stat": "Not_Ok", "emsg": "Invalid checksum"}

    if not authed(req):
        return {"stat": "Not_Ok", "emsg": "Session Expired : Invalid Session Key"}

    if ep == "ClientDetails":
        return {"stat": "Ok", "actid": d["actid"], "cliname": "TEST CLIENT"}
    if ep == "TradeBook":
        return FILLS
    if ep == "PositionBook":
        return [{"stat": "Ok", "exch": "NFO", "tsym": TSYM_PE, "netqty": "30", "lp": "295.00", "prd": "M"}]
    if ep == "GetIndexList":
        return {"stat": "Ok", "values": [{"idxname": "NIFTY BANK", "token": "26009"}, {"idxname": "NIFTY 50", "token": "26000"}]} if d["exch"] == "NSE" else {"stat": "Not_Ok", "emsg": "no data"}
    if ep == "TPSeries":
        assert d["token"] == "26009" and d["intrv"] == "1"
        out, t = [], datetime.strptime(TODAY + " 09:15", "%d-%m-%Y %H:%M")
        while t.strftime("%H:%M") < "15:30":
            out.append({
                "stat": "Ok",
                "time": t.strftime("%d-%m-%Y %H:%M:%S"),
                "into": "51800.00",
                "inth": "51820",
                "intl": "51790",
                "intc": "51805.00",
                "intv": "0",
            })
            t += timedelta(minutes=1)
        return out[::-1]

    return JSONResponse({"stat": "Not_Ok", "emsg": f"mock: {ep} not implemented"}, 404)


@app.websocket("/NorenWSTP/")
@app.websocket("/NorenWSTP")
async def mock_ws(websocket: WebSocket):
    """Mock Noren WebSocket protocol."""
    await websocket.accept()
    CALLS.append("WebSocketConnect")
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            t = data.get("t")
            if t == "c":
                # Handshake
                CALLS.append("WebSocketHandshake")
                await websocket.send_text(json.dumps({"t": "ck", "s": "OK"}))
            elif t == "o":
                # Order updates sub
                CALLS.append("WebSocketOrderSub")
                # Send sample fill event
                await websocket.send_text(json.dumps({
                    "t": "om",
                    "norenordno": "2611139999",
                    "status": "COMPLETE",
                    "flqty": "30",
                    "flprc": "320.00",
                    "trantype": "B",
                    "tsym": TSYM_CE,
                    "prd": "I",
                    "remarks": "",
                    "ordersource": "API",
                }))
            elif t == "h":
                # Heartbeat
                await websocket.send_text(json.dumps({"t": "h"}))
    except WebSocketDisconnect:
        pass


@app.post("/bot{tok}/sendMessage")
async def tg(tok: str, req: Request):
    TELEGRAM.append(await req.json())
    return {"ok": True}


@app.get("/_calls")
def calls():
    return {"calls": CALLS, "telegram": TELEGRAM}
