"""High-performance relational repository layer replacing the flat key-value document store.

Maintains complete backward compatibility for existing callers while mapping
collections directly to structured relational tables and optimized queries.
"""
import json
import logging
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union
from sqlalchemy import select, delete as sa_delete, update as sa_update, insert as sa_insert
from sqlalchemy.orm import Session as SyncSession

from app.database import AsyncSessionLocal, async_engine, Base
from app.models import (
    User,
    Session as UserSession,
    LinkNonce,
    AuditLog,
    Trader,
    Account,
    BrokerSession,
    Group,
    Setting,
    Instrument,
    Order,
    Fill,
    Trade,
    Position,
    PositionLot,
    TradingRule,
    RuleLog,
    Alert,
    AlertDeliveryLog,
    SentFlag,
    AccountSyncState,
    ProcessedBrokerEvent,
    DailySyncRecord,
    DailyAccountReport,
)
from app.security import encrypt_token, decrypt_token

log = logging.getLogger("tts.store")

COLLECTIONS = [
    "traders",
    "accounts",
    "rules",
    "rule_log",
    "trades",
    "fills",
    "orders",
    "positions",
    "groups",
    "settings",
    "users",
    "sessions",
    "tokens",
    "link_nonces",
    "sent_flags",
    "status",
    "daily_reports",
    "alerts",
]


class Store:
    """Relational Store wrapping SQLAlchemy async engine with backward-compatible sync facade."""

    def __init__(self, db_path_or_url: Optional[str] = None):
        self._loop = None
        self._init_done = False

    def _run_async(self, coro):
        """Runs an async coroutine synchronously for legacy callers."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, coro).result()
        else:
            return asyncio.run(coro)

    # ---------------- Asynchronous Domain Methods ----------------

    async def get_user_async(self, username: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(User).where(User.user == username)
            res = (await session.execute(stmt)).scalar_one_or_none()
            if not res:
                return None
            return {
                "user": res.user,
                "role": res.role,
                "pw": res.pw,
                "trader_id": res.trader_id,
                "assigned_accounts": res.assigned_accounts or [],
            }

    async def get_all_users_async(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(User)
            res = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "user": u.user,
                    "role": u.role,
                    "pw": u.pw,
                    "trader_id": u.trader_id,
                    "assigned_accounts": u.assigned_accounts or [],
                }
                for u in res
            ]

    async def put_user_async(self, username: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(User).where(User.user == username)
            user = (await session.execute(stmt)).scalar_one_or_none()
            if user:
                user.role = doc.get("role", user.role)
                if doc.get("pw"):
                    user.pw = doc["pw"]
                user.trader_id = doc.get("trader_id", user.trader_id)
                user.assigned_accounts = doc.get("assigned_accounts", user.assigned_accounts)
            else:
                user = User(
                    user=username,
                    role=doc.get("role", "viewer"),
                    pw=doc.get("pw", ""),
                    trader_id=doc.get("trader_id"),
                    assigned_accounts=doc.get("assigned_accounts", []),
                )
                session.add(user)
            await session.commit()

    async def get_session_async(self, token: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(UserSession).where(UserSession.id == token)
            s = (await session.execute(stmt)).scalar_one_or_none()
            if not s:
                return None
            exp_dt = s.exp
            if exp_dt and exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            return {
                "user": s.user,
                "role": s.role,
                "exp": exp_dt.timestamp() if exp_dt else 0,
            }

    async def put_session_async(self, token: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            exp_dt = datetime.fromtimestamp(doc["exp"], tz=timezone.utc) if isinstance(doc.get("exp"), (int, float)) else datetime.now(timezone.utc)
            s = UserSession(id=token, user=doc["user"], role=doc["role"], exp=exp_dt)
            await session.merge(s)
            await session.commit()

    async def _delete_session(self, token: str):
        async with AsyncSessionLocal() as session:
            await session.execute(sa_delete(UserSession).where(UserSession.id == token))
            await session.commit()

    async def get_account_async(self, aid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Account).where(Account.id == aid)
            acc = (await session.execute(stmt)).scalar_one_or_none()
            if not acc:
                return None
            return {
                "id": acc.id,
                "name": acc.name,
                "broker": acc.broker,
                "trader": acc.trader_id,
                "zebu_uid": acc.zebu_uid,
                "zebu_actid": acc.zebu_actid,
                "zebu_client_id": getattr(acc, "zebu_client_id", None) or acc.zebu_uid,
                "zebu_secret_key": getattr(acc, "zebu_secret_key", None),
                "proxy_url": getattr(acc, "proxy_url", None),
                "group_id": acc.group_id,
            }

    async def get_all_accounts_async(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Account)
            res = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": a.id,
                    "name": a.name,
                    "broker": a.broker,
                    "trader": a.trader_id,
                    "zebu_uid": a.zebu_uid,
                    "zebu_actid": a.zebu_actid,
                    "zebu_client_id": getattr(a, "zebu_client_id", None) or a.zebu_uid,
                    "zebu_secret_key": getattr(a, "zebu_secret_key", None),
                    "proxy_url": getattr(a, "proxy_url", None),
                    "group_id": a.group_id,
                }
                for a in res
            ]

    async def put_account_async(self, aid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(Account).where(Account.id == aid)
            acc = (await session.execute(stmt)).scalar_one_or_none()
            if acc:
                acc.name = doc.get("name", acc.name)
                acc.broker = doc.get("broker", acc.broker)
                acc.trader_id = doc.get("trader", acc.trader_id)
                acc.zebu_uid = doc.get("zebu_uid", acc.zebu_uid)
                acc.zebu_actid = doc.get("zebu_actid", acc.zebu_actid)
                if "zebu_client_id" in doc:
                    acc.zebu_client_id = doc.get("zebu_client_id")
                if "zebu_secret_key" in doc and doc.get("zebu_secret_key"):
                    acc.zebu_secret_key = doc.get("zebu_secret_key")
                if "proxy_url" in doc:
                    acc.proxy_url = doc.get("proxy_url")
                acc.group_id = doc.get("group_id", acc.group_id)
            else:
                acc = Account(
                    id=aid,
                    name=doc.get("name", aid),
                    broker=doc.get("broker", "Zebu"),
                    trader_id=doc.get("trader"),
                    zebu_uid=doc.get("zebu_uid"),
                    zebu_actid=doc.get("zebu_actid"),
                    zebu_client_id=doc.get("zebu_client_id") or doc.get("zebu_uid"),
                    zebu_secret_key=doc.get("zebu_secret_key"),
                    proxy_url=doc.get("proxy_url"),
                    group_id=doc.get("group_id"),
                )
                session.add(acc)
            await session.commit()

    async def get_token_async(self, aid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(BrokerSession).where(BrokerSession.account_id == aid)
            bs = (await session.execute(stmt)).scalar_one_or_none()
            if not bs or not bs.access_token_enc:
                acc_stmt = select(Account).where((Account.id == aid) | (Account.zebu_uid == aid))
                cur_acc = (await session.execute(acc_stmt)).scalars().first()
                if cur_acc and cur_acc.zebu_uid:
                    other_stmt = select(BrokerSession).join(Account, Account.id == BrokerSession.account_id).where(
                        (Account.zebu_uid == cur_acc.zebu_uid) | (Account.id == cur_acc.id) | (BrokerSession.account_id == cur_acc.zebu_uid),
                        BrokerSession.access_token_enc.isnot(None),
                    )
                    bs = (await session.execute(other_stmt)).scalars().first()
            if not bs:
                return None
            exp_dt = bs.expires_at
            if exp_dt and exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            last_sync_dt = bs.last_sync_at
            if last_sync_dt and last_sync_dt.tzinfo is None:
                last_sync_dt = last_sync_dt.replace(tzinfo=timezone.utc)
            return {
                "access_token": decrypt_token(bs.access_token_enc),
                "refresh_token": decrypt_token(bs.refresh_token_enc),
                "expires_at": exp_dt.timestamp() if exp_dt else None,
                "linked_at": bs.linked_at.isoformat() if bs.linked_at else None,
                "cliname": bs.cliname,
                "status": bs.status or ("Linked" if bs.access_token_enc else "Not linked"),
                "ws_status": bs.ws_status,
                "last_sync_at": last_sync_dt.isoformat() if last_sync_dt else None,
                "sync_message": bs.sync_message,
            }

    async def put_token_async(self, aid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(BrokerSession).where(BrokerSession.account_id == aid)
            bs = (await session.execute(stmt)).scalar_one_or_none()
            exp_dt = None
            if doc.get("expires_at"):
                exp_dt = datetime.fromtimestamp(doc["expires_at"], tz=timezone.utc) if isinstance(doc["expires_at"], (int, float)) else None

            linked_dt = None
            if doc.get("linked_at"):
                try:
                    linked_dt = datetime.fromisoformat(str(doc["linked_at"])).replace(tzinfo=timezone.utc)
                except Exception:
                    linked_dt = datetime.now(timezone.utc)

            last_sync_dt = None
            if doc.get("last_sync_at"):
                if isinstance(doc["last_sync_at"], datetime):
                    last_sync_dt = doc["last_sync_at"]
                else:
                    try:
                        last_sync_dt = datetime.fromisoformat(str(doc["last_sync_at"]))
                    except Exception:
                        last_sync_dt = datetime.now(timezone.utc)

            acc_token_enc = encrypt_token(doc["access_token"]) if "access_token" in doc and doc["access_token"] is not None else None
            ref_token_enc = encrypt_token(doc["refresh_token"]) if "refresh_token" in doc and doc["refresh_token"] is not None else None

            if bs:
                if "access_token" in doc:
                    bs.access_token_enc = acc_token_enc
                if "refresh_token" in doc:
                    bs.refresh_token_enc = ref_token_enc
                if exp_dt is not None:
                    bs.expires_at = exp_dt
                if linked_dt is not None:
                    bs.linked_at = linked_dt
                if doc.get("cliname") is not None:
                    bs.cliname = doc["cliname"]
                if doc.get("status"):
                    bs.status = doc["status"]
                if doc.get("ws_status"):
                    bs.ws_status = doc["ws_status"]
                if last_sync_dt is not None:
                    bs.last_sync_at = last_sync_dt
                if "sync_message" in doc:
                    bs.sync_message = doc["sync_message"]
            else:
                bs = BrokerSession(
                    account_id=aid,
                    access_token_enc=acc_token_enc,
                    refresh_token_enc=ref_token_enc,
                    expires_at=exp_dt,
                    linked_at=linked_dt,
                    cliname=doc.get("cliname"),
                    status=doc.get("status", "Linked" if acc_token_enc else "Not linked"),
                    ws_status=doc.get("ws_status", "DISCONNECTED"),
                    last_sync_at=last_sync_dt,
                    sync_message=doc.get("sync_message"),
                )
                session.add(bs)
            await session.commit()

    async def delete_token_async(self, aid: str):
        async with AsyncSessionLocal() as session:
            acc_stmt = select(Account).where((Account.id == aid) | (Account.zebu_uid == aid))
            cur_acc = (await session.execute(acc_stmt)).scalars().first()
            target_ids = {aid}
            if cur_acc:
                target_ids.add(cur_acc.id)
                if cur_acc.zebu_uid:
                    target_ids.add(cur_acc.zebu_uid)
                    same_uid_stmt = select(Account.id).where(Account.zebu_uid == cur_acc.zebu_uid)
                    for row_id in (await session.execute(same_uid_stmt)).scalars().all():
                        target_ids.add(row_id)
            await session.execute(sa_delete(BrokerSession).where(BrokerSession.account_id.in_(list(target_ids))))
            await session.commit()

    # ---------------- Account Sync State & Daily Reports ----------------

    async def get_all_sync_states_async(self) -> Dict[str, Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(AccountSyncState)
            rows = (await session.execute(stmt)).scalars().all()
            return {
                st.account_id: {
                    "account_id": st.account_id,
                    "status": st.status,
                    "is_stale": bool(getattr(st, "is_stale", False)),
                    "last_sync_start": st.last_sync_start.isoformat() if st.last_sync_start else None,
                    "last_sync_success": st.last_sync_success.isoformat() if st.last_sync_success else None,
                    "last_error": st.last_error,
                    "error_count": st.error_count,
                    "consecutive_failures": st.consecutive_failures,
                    "circuit_open": st.circuit_open,
                    "total_fills_today": st.total_fills_today,
                    "total_trades_today": st.total_trades_today,
                }
                for st in rows
            }

    async def get_sync_state_async(self, aid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(AccountSyncState).where(AccountSyncState.account_id == aid)
            st = (await session.execute(stmt)).scalar_one_or_none()
            if not st:
                return None
            return {
                "account_id": st.account_id,
                "status": st.status,
                "is_stale": bool(getattr(st, "is_stale", False)),
                "last_sync_start": st.last_sync_start.isoformat() if st.last_sync_start else None,
                "last_sync_success": st.last_sync_success.isoformat() if st.last_sync_success else None,
                "last_error": st.last_error,
                "error_count": st.error_count,
                "consecutive_failures": st.consecutive_failures,
                "circuit_open": st.circuit_open,
                "total_fills_today": st.total_fills_today,
                "total_trades_today": st.total_trades_today,
            }

    async def put_sync_state_async(self, aid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(AccountSyncState).where(AccountSyncState.account_id == aid)
            st = (await session.execute(stmt)).scalar_one_or_none()
            if not st:
                st = AccountSyncState(account_id=aid)
                session.add(st)
            if "status" in doc:
                st.status = doc["status"]
            if "is_stale" in doc:
                st.is_stale = bool(doc["is_stale"])
            if "last_sync_start" in doc and doc["last_sync_start"]:
                st.last_sync_start = doc["last_sync_start"]
            if "last_sync_success" in doc and doc["last_sync_success"]:
                st.last_sync_success = doc["last_sync_success"]
            if "last_error" in doc:
                st.last_error = doc["last_error"]
            if "error_count" in doc:
                st.error_count = int(doc["error_count"])
            if "consecutive_failures" in doc:
                st.consecutive_failures = int(doc["consecutive_failures"])
            if "circuit_open" in doc:
                st.circuit_open = bool(doc["circuit_open"])
            if "total_fills_today" in doc:
                st.total_fills_today = int(doc["total_fills_today"])
            if "total_trades_today" in doc:
                st.total_trades_today = int(doc["total_trades_today"])
            await session.commit()

    async def get_daily_reports_async(self, account_id: Optional[str] = None, from_date: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(DailyAccountReport)
            if account_id:
                stmt = stmt.where((DailyAccountReport.account_id == account_id) | (DailyAccountReport.trader_id == account_id))
            if from_date:
                stmt = stmt.where(DailyAccountReport.trade_date >= from_date)
            stmt = stmt.order_by(DailyAccountReport.trade_date.asc())
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": r.id,
                    "account": r.account_id,
                    "trader": r.trader_id,
                    "date": r.trade_date,
                    "status": r.status,
                    "realized_pnl": float(r.realized_pnl or 0),
                    "unrealized_pnl": float(r.unrealized_pnl or 0),
                    "charges": float(r.charges or 0),
                    "net_pnl": float(r.net_pnl or 0),
                    "total_trades": r.total_trades,
                    "closed_trades": r.closed_trades,
                    "open_trades": r.open_trades,
                    "partially_closed_trades": r.partially_closed_trades,
                    "winning_trades": r.winning_trades,
                    "losing_trades": r.losing_trades,
                    "finalized_at": r.finalized_at.isoformat() if r.finalized_at else None,
                    "archived_at": r.archived_at.isoformat() if r.archived_at else None,
                    "source_info": r.source_info,
                    "reconciliation_notes": r.reconciliation_notes,
                    "trade_details": r.trade_details or [],
                }
                for r in rows
            ]

    async def put_daily_report_async(self, report_id: str, doc: Dict[str, Any], allow_overwrite_finalized: bool = False):
        async with AsyncSessionLocal() as session:
            stmt = select(DailyAccountReport).where(DailyAccountReport.id == report_id)
            r = (await session.execute(stmt)).scalar_one_or_none()
            if r and r.status in ("FINALIZED", "ARCHIVED") and not allow_overwrite_finalized:
                # Only allow transitioning FINALIZED -> ARCHIVED without altering financial figures
                if doc.get("status") == "ARCHIVED" and r.status == "FINALIZED":
                    r.status = "ARCHIVED"
                    r.archived_at = doc.get("archived_at") or datetime.now(timezone.utc)
                    await session.commit()
                return
            if not r:
                r = DailyAccountReport(id=report_id)
                session.add(r)
            r.account_id = doc.get("account") or r.account_id
            r.trader_id = doc.get("trader") or r.trader_id
            r.trade_date = doc.get("date") or r.trade_date
            r.status = doc.get("status") or r.status or "LIVE"
            r.realized_pnl = doc.get("realized_pnl", r.realized_pnl or 0)
            r.unrealized_pnl = doc.get("unrealized_pnl", r.unrealized_pnl or 0)
            r.charges = doc.get("charges", r.charges or 0)
            r.net_pnl = doc.get("net_pnl", r.net_pnl or 0)
            r.total_trades = int(doc.get("total_trades", r.total_trades or 0))
            r.closed_trades = int(doc.get("closed_trades", r.closed_trades or 0))
            r.open_trades = int(doc.get("open_trades", r.open_trades or 0))
            r.partially_closed_trades = int(doc.get("partially_closed_trades", r.partially_closed_trades or 0))
            r.winning_trades = int(doc.get("winning_trades", r.winning_trades or 0))
            r.losing_trades = int(doc.get("losing_trades", r.losing_trades or 0))
            if doc.get("finalized_at"):
                r.finalized_at = doc["finalized_at"] if isinstance(doc["finalized_at"], datetime) else datetime.now(timezone.utc)
            if doc.get("archived_at"):
                r.archived_at = doc["archived_at"] if isinstance(doc["archived_at"], datetime) else datetime.now(timezone.utc)
            if "source_info" in doc:
                r.source_info = doc["source_info"]
            if "reconciliation_notes" in doc:
                r.reconciliation_notes = doc["reconciliation_notes"]
            if "trade_details" in doc:
                r.trade_details = doc["trade_details"]
            await session.commit()

    # ---------------- Orders & Positions Persistence ----------------

    async def put_many_orders_async(self, account_id: str, orders: List[Dict[str, Any]]):
        from app.engine import parse_ts
        async with AsyncSessionLocal() as session:
            for o in orders:
                ord_no = str(o.get("norenordno") or "")
                if not ord_no:
                    continue
                oid = f"{account_id}|{ord_no}"
                stmt = select(Order).where(Order.id == oid)
                row = (await session.execute(stmt)).scalar_one_or_none()
                ot = parse_ts(o.get("norentm"), o.get("exch_tm"), o.get("ordenttm")) or datetime.now(timezone.utc)
                side_raw = str(o.get("trantype", "B")).upper()
                side = "BUY" if side_raw in ("B", "BUY") else "SELL"
                qty = float(o.get("qty") or 0)
                filled_qty = float(o.get("fillshares") or o.get("flqty") or 0)
                avg_prc = float(o.get("avgprc") or o.get("prc") or 0)
                status = str(o.get("status") or "OPEN").upper()

                remarks_str = o.get("remarks") or o.get("rejreason") or ""
                if o.get("rejreason") and o.get("remarks") and o["rejreason"] not in o["remarks"]:
                    remarks_str = f"{o['remarks']} ({o['rejreason']})"

                if not row:
                    row = Order(
                        id=oid,
                        account_id=account_id,
                        norenordno=ord_no,
                        exch=o.get("exch", "NFO"),
                        tsym=str(o.get("tsym", "")),
                        side=side,
                        product=o.get("prd", "MIS"),
                        price_type=o.get("prctyp", "MKT"),
                        status=status,
                        qty=qty,
                        filled_qty=filled_qty,
                        avg_price=avg_prc,
                        order_source=o.get("ordersource", ""),
                        remarks=remarks_str,
                        order_time=ot,
                        raw_payload=o,
                    )
                    session.add(row)
                else:
                    row.status = status
                    row.qty = qty
                    row.filled_qty = filled_qty
                    row.avg_price = avg_prc
                    if remarks_str:
                        row.remarks = remarks_str
                    row.raw_payload = o
            await session.commit()

    async def get_orders_async(self, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Order)
            if account_id:
                stmt = stmt.where(Order.account_id == account_id)
            stmt = stmt.order_by(Order.order_time.desc())
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": r.id,
                    "account": r.account_id,
                    "norenordno": r.norenordno,
                    "exch": r.exch,
                    "tsym": r.tsym,
                    "side": r.side,
                    "product": r.product,
                    "price_type": r.price_type,
                    "status": r.status,
                    "qty": float(r.qty or 0),
                    "filled_qty": float(r.filled_qty or 0),
                    "avg_price": float(r.avg_price or 0),
                    "order_source": r.order_source,
                    "remarks": r.remarks,
                    "order_time": r.order_time.isoformat() if r.order_time else None,
                }
                for r in rows
            ]

    async def put_many_positions_async(self, account_id: str, positions: List[Dict[str, Any]]):
        async with AsyncSessionLocal() as session:
            for p in positions:
                tsym = str(p.get("tsym") or "")
                if not tsym:
                    continue
                prd = str(p.get("prd") or "MIS")
                pid = f"{account_id}|{tsym}|{prd}"
                stmt = select(Position).where(Position.id == pid)
                row = (await session.execute(stmt)).scalar_one_or_none()
                net_qty = float(p.get("netqty") or 0)
                buy_qty = float(p.get("daybuyqty") or p.get("totbuyqty") or 0)
                sell_qty = float(p.get("daysellqty") or p.get("totsellqty") or 0)
                buy_avg = float(p.get("daybuyavgprc") or p.get("totbuyavgprc") or p.get("netavgprc") or 0)
                sell_avg = float(p.get("daysellavgprc") or p.get("totsellavgprc") or 0)
                lp_raw = p.get("lp")
                ltp_val = float(lp_raw) if lp_raw is not None and lp_raw != "" else None
                rpnl = float(p.get("rpnl") or 0)
                urmtom = float(p.get("urmtom") or 0)

                if not row:
                    row = Position(
                        id=pid,
                        account_id=account_id,
                        tsym=tsym,
                        product=prd,
                        exch=p.get("exch", "NFO"),
                        net_qty=net_qty,
                        buy_qty=buy_qty,
                        sell_qty=sell_qty,
                        buy_avg=buy_avg,
                        sell_avg=sell_avg,
                        ltp=ltp_val,
                        realized_pnl=rpnl,
                        unrealized_pnl=urmtom,
                        raw_payload=p,
                    )
                    session.add(row)
                else:
                    row.net_qty = net_qty
                    row.buy_qty = buy_qty
                    row.sell_qty = sell_qty
                    row.buy_avg = buy_avg
                    row.sell_avg = sell_avg
                    if ltp_val is not None:
                        row.ltp = ltp_val
                    row.realized_pnl = rpnl
                    row.unrealized_pnl = urmtom
                    row.raw_payload = p
                    row.updated_at = datetime.now(timezone.utc)
            await session.commit()

    async def get_positions_async(self, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Position)
            if account_id:
                stmt = stmt.where(Position.account_id == account_id)
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": r.id,
                    "account": r.account_id,
                    "tsym": r.tsym,
                    "product": r.product,
                    "exch": r.exch,
                    "net_qty": float(r.net_qty or 0),
                    "buy_qty": float(r.buy_qty or 0),
                    "sell_qty": float(r.sell_qty or 0),
                    "buy_avg": float(r.buy_avg or 0),
                    "sell_avg": float(r.sell_avg or 0),
                    "ltp": float(r.ltp) if r.ltp is not None else None,
                    "realized_pnl": float(r.realized_pnl or 0),
                    "unrealized_pnl": float(r.unrealized_pnl or 0),
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
                for r in rows
            ]

    # ---------------- Persistent Rule Violations (Alerts) ----------------

    async def upsert_rule_violation_async(self, dedup_key: str, doc: Dict[str, Any]) -> bool:
        """Persists a structured rule violation in the alerts table idempotently. Returns True if newly created."""
        async with AsyncSessionLocal() as session:
            stmt = select(Alert).where(Alert.dedup_key == dedup_key)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if existing:
                # Update status if changed (e.g. ACTIVE -> HISTORICAL or RESOLVED)
                if doc.get("violation_status") and existing.violation_status != doc["violation_status"]:
                    existing.violation_status = doc["violation_status"]
                    await session.commit()
                return False

            import uuid
            alert_id = doc.get("id") or f"FLG-{uuid.uuid4().hex[:12]}"
            alert = Alert(
                id=alert_id,
                dedup_key=dedup_key,
                account_id=doc.get("account_id"),
                trader_id=doc.get("trader_id"),
                trade_id=doc.get("trade_id"),
                order_id=doc.get("order_id"),
                trade_date=doc.get("trade_date"),
                trade_time=doc.get("trade_time"),
                rule_name=doc.get("rule_name"),
                configured_condition=doc.get("configured_condition"),
                observed_value=doc.get("observed_value"),
                explanation=doc.get("explanation"),
                violation_status=doc.get("violation_status", "ACTIVE"),
                severity=doc.get("severity", "High"),
                title=doc.get("title", doc.get("rule_name", "Rule violation")),
                message=doc.get("message", doc.get("explanation", "")),
                event_time=datetime.now(timezone.utc),
            )
            session.add(alert)
            await session.commit()
            return True

    async def clear_rule_violations_async(self, account_id: str):
        """Remove persisted rule violation alerts for an account/trader before re-evaluating updated/deleted rules."""
        async with AsyncSessionLocal() as session:
            await session.execute(
                sa_delete(Alert).where(
                    Alert.rule_name.isnot(None),
                    (Alert.account_id == account_id) | (Alert.trader_id == account_id),
                )
            )
            await session.commit()

    async def get_rule_violations_async(self, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Alert).where(Alert.rule_name.isnot(None))
            if account_id:
                stmt = stmt.where((Alert.account_id == account_id) | (Alert.trader_id == account_id))
            stmt = stmt.order_by(Alert.created_at.desc())
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": r.id,
                    "dedup_key": r.dedup_key,
                    "account": r.account_id,
                    "trader": r.trader_id,
                    "trade_id": r.trade_id,
                    "order_id": r.order_id,
                    "date": r.trade_date,
                    "time": r.trade_time,
                    "rule": r.rule_name,
                    "condition": r.configured_condition,
                    "observed": r.observed_value,
                    "did": r.explanation or r.message,
                    "status": getattr(r, "violation_status", "ACTIVE") or "ACTIVE",
                    "level": r.severity,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in rows
            ]

    # ---------------- Generic Asynchronous Operations ----------------

    async def all_async(self, collection: str) -> List[Dict[str, Any]]:
        """Retrieve all records for a collection asynchronously."""
        if collection == "traders":
            return await self._all_traders()
        elif collection == "accounts":
            return await self.get_all_accounts_async()
        elif collection == "users":
            return await self.get_all_users_async()
        elif collection == "groups":
            return await self._all_groups()
        elif collection == "rules":
            return await self._all_rules()
        elif collection == "rule_log":
            return await self._all_rule_logs()
        elif collection == "trades":
            return await self._all_trades()
        elif collection == "fills":
            return await self._all_fills()
        elif collection == "orders":
            return await self.get_orders_async()
        elif collection == "positions":
            return await self.get_positions_async()
        elif collection == "daily_reports":
            return await self.get_daily_reports_async()
        elif collection == "alerts":
            return await self.get_rule_violations_async()
        elif collection == "settings":
            return await self._all_settings()
        elif collection == "sessions":
            return await self._all_sessions()
        elif collection == "tokens":
            return await self._all_tokens()
        elif collection == "sent_flags":
            return await self._all_sent_flags()
        elif collection == "status":
            return await self._all_status()
        return []

    async def get_async(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single record asynchronously."""
        if collection == "users":
            return await self.get_user_async(item_id)
        elif collection == "sessions":
            return await self.get_session_async(item_id)
        elif collection == "accounts":
            return await self.get_account_async(item_id)
        elif collection == "tokens":
            return await self.get_token_async(item_id)
        elif collection == "traders":
            return await self._get_trader(item_id)
        elif collection == "rules":
            return await self._get_rule(item_id)
        elif collection == "groups":
            return await self._get_group(item_id)
        elif collection == "trades":
            return await self._get_trade(item_id)
        elif collection == "settings":
            return await self._get_setting(item_id)
        elif collection == "link_nonces":
            return await self._get_link_nonce(item_id)
        elif collection == "sent_flags":
            return await self._get_sent_flag(item_id)
        elif collection == "status":
            return await self._get_status(item_id)
        return None

    async def put_async(self, collection: str, item_id: str, doc: Dict[str, Any]):
        """Upsert a single record asynchronously."""
        if collection == "users":
            return await self.put_user_async(item_id, doc)
        elif collection == "sessions":
            return await self.put_session_async(item_id, doc)
        elif collection == "accounts":
            return await self.put_account_async(item_id, doc)
        elif collection == "tokens":
            return await self.put_token_async(item_id, doc)
        elif collection == "traders":
            return await self._put_trader(item_id, doc)
        elif collection == "rules":
            return await self._put_rule(item_id, doc)
        elif collection == "rule_log":
            return await self._put_rule_log(item_id, doc)
        elif collection == "groups":
            return await self._put_group(item_id, doc)
        elif collection == "trades":
            return await self._put_trade(item_id, doc)
        elif collection == "settings":
            return await self._put_setting(item_id, doc)
        elif collection == "link_nonces":
            return await self._put_link_nonce(item_id, doc)
        elif collection == "sent_flags":
            return await self._put_sent_flag(item_id, doc)
        elif collection == "status":
            return await self._put_status(item_id, doc)

    async def put_many_async(self, collection: str, items: List[Tuple[str, Dict[str, Any]]]):
        """Batch insert/update asynchronously."""
        if collection == "trades":
            return await self._put_many_trades(items)
        elif collection == "fills":
            return await self._put_many_fills(items)
        for item_id, doc in items:
            await self.put_async(collection, item_id, doc)

    async def delete_async(self, collection: str, item_id: str):
        """Delete an item asynchronously."""
        if collection == "tokens":
            return await self.delete_token_async(item_id)
        elif collection == "trades":
            return await self._delete_trade(item_id)
        elif collection == "groups":
            return await self._delete_group(item_id)
        elif collection == "link_nonces":
            return await self._delete_link_nonce(item_id)
        elif collection == "sessions":
            return await self._delete_session(item_id)
        elif collection == "accounts":
            return await self.delete_account_permanent_async(item_id)
        elif collection == "traders":
            return await self.delete_trader_permanent_async(item_id)

    async def delete_account_permanent_async(self, aid: str):
        """Permanently delete a broker account and all its associated sessions, sync states, orders, fills, positions, and trades."""
        async with AsyncSessionLocal() as session:
            stmt = select(Account).where(Account.id == aid)
            acc = (await session.execute(stmt)).scalar_one_or_none()
            z_uid = acc.zebu_uid if acc else None

            # Delete child delivery logs for any alerts belonging to this account first
            alert_ids = (await session.execute(select(Alert.id).where(Alert.account_id == aid))).scalars().all()
            if alert_ids:
                await session.execute(sa_delete(AlertDeliveryLog).where(AlertDeliveryLog.alert_id.in_(alert_ids)))

            await session.execute(sa_delete(BrokerSession).where(BrokerSession.account_id == aid))
            await session.execute(sa_delete(AccountSyncState).where(AccountSyncState.account_id == aid))
            await session.execute(sa_delete(ProcessedBrokerEvent).where(ProcessedBrokerEvent.account_id == aid))
            await session.execute(sa_delete(DailyAccountReport).where(DailyAccountReport.account_id == aid))
            await session.execute(sa_delete(Order).where(Order.account_id == aid))
            await session.execute(sa_delete(Position).where(Position.account_id == aid))
            await session.execute(sa_delete(PositionLot).where(PositionLot.account_id == aid))
            await session.execute(sa_delete(Fill).where(Fill.account_id == aid))
            await session.execute(sa_delete(Trade).where(Trade.account_id == aid))
            await session.execute(sa_delete(TradingRule).where((TradingRule.trader_id == aid) | (TradingRule.account_id == aid)))
            await session.execute(sa_delete(RuleLog).where(RuleLog.trader_id == aid))
            await session.execute(sa_delete(Alert).where(Alert.account_id == aid))
            await session.execute(sa_delete(LinkNonce).where(LinkNonce.account_id == aid))
            await session.execute(sa_delete(Account).where(Account.id == aid))

            # Unassign any other accounts pointing to aid as trader_id before removing auto-created Trader row
            await session.execute(sa_update(Account).where(Account.trader_id == aid).values(trader_id=None))
            await session.execute(sa_delete(Trader).where(Trader.id == aid))
            if z_uid and z_uid != aid:
                rem_acc = (await session.execute(select(Account).where((Account.zebu_uid == z_uid) | (Account.trader_id == z_uid)))).scalars().first()
                if not rem_acc:
                    await session.execute(sa_delete(Trader).where(Trader.id == z_uid))
                    await session.execute(sa_delete(TradingRule).where(TradingRule.trader_id == z_uid))

            # Remove account ID from any groups
            groups = (await session.execute(select(Group))).scalars().all()
            for g in groups:
                if g.traders and aid in g.traders:
                    g.traders = [x for x in g.traders if x != aid]

            await session.commit()

    async def delete_trader_permanent_async(self, tid: str):
        """Permanently delete a trader profile and any matching account/trades/rules."""
        async with AsyncSessionLocal() as session:
            # Find any broker accounts whose ID or zebu_uid matches this trader ID
            matching_accs = (
                await session.execute(
                    select(Account).where((Account.id == tid) | (Account.zebu_uid == tid))
                )
            ).scalars().all()
            matching_aids = [a.id for a in matching_accs]

            # 1. Unassign any other broker accounts that pointed to this trader BEFORE deleting Trader
            await session.execute(sa_update(Account).where(Account.trader_id == tid).values(trader_id=None))

            # 2. Clean up matching broker accounts and their child records
            for aid in matching_aids:
                a_alert_ids = (await session.execute(select(Alert.id).where(Alert.account_id == aid))).scalars().all()
                if a_alert_ids:
                    await session.execute(sa_delete(AlertDeliveryLog).where(AlertDeliveryLog.alert_id.in_(a_alert_ids)))
                await session.execute(sa_delete(BrokerSession).where(BrokerSession.account_id == aid))
                await session.execute(sa_delete(AccountSyncState).where(AccountSyncState.account_id == aid))
                await session.execute(sa_delete(ProcessedBrokerEvent).where(ProcessedBrokerEvent.account_id == aid))
                await session.execute(sa_delete(DailyAccountReport).where(DailyAccountReport.account_id == aid))
                await session.execute(sa_delete(Order).where(Order.account_id == aid))
                await session.execute(sa_delete(Position).where(Position.account_id == aid))
                await session.execute(sa_delete(PositionLot).where(PositionLot.account_id == aid))
                await session.execute(sa_delete(Fill).where(Fill.account_id == aid))
                await session.execute(sa_delete(Trade).where(Trade.account_id == aid))
                await session.execute(sa_delete(TradingRule).where((TradingRule.trader_id == aid) | (TradingRule.account_id == aid)))
                await session.execute(sa_delete(Alert).where(Alert.account_id == aid))
                await session.execute(sa_delete(LinkNonce).where(LinkNonce.account_id == aid))
                await session.execute(sa_delete(Account).where(Account.id == aid))

            # 3. Delete trader's child records and finally the Trader row itself
            t_alert_ids = (await session.execute(select(Alert.id).where((Alert.trader_id == tid) | (Alert.account_id == tid)))).scalars().all()
            if t_alert_ids:
                await session.execute(sa_delete(AlertDeliveryLog).where(AlertDeliveryLog.alert_id.in_(t_alert_ids)))
            await session.execute(sa_delete(TradingRule).where((TradingRule.trader_id == tid) | (TradingRule.account_id == tid)))
            await session.execute(sa_delete(RuleLog).where(RuleLog.trader_id == tid))
            await session.execute(sa_delete(Trade).where((Trade.trader_id == tid) | (Trade.account_id == tid)))
            await session.execute(sa_delete(DailyAccountReport).where((DailyAccountReport.trader_id == tid) | (DailyAccountReport.account_id == tid)))
            await session.execute(sa_delete(Alert).where((Alert.trader_id == tid) | (Alert.account_id == tid)))
            await session.execute(sa_delete(Trader).where(Trader.id == tid))

            await session.commit()

    async def delete_where_prefix_async(self, collection: str, prefix: str):
        """Delete items by prefix asynchronously."""
        if collection == "trades":
            await self._delete_trades_by_prefix(prefix)

    # ---------------- Backward Compatible Synchronous Methods ----------------

    def all(self, collection: str) -> List[Dict[str, Any]]:
        """Retrieve all records for a collection, mapped to dictionary."""
        return self._run_async(self.all_async(collection))

    def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single record by collection and ID."""
        return self._run_async(self.get_async(collection, item_id))

    def put(self, collection: str, item_id: str, doc: Dict[str, Any]):
        """Upsert a single record."""
        return self._run_async(self.put_async(collection, item_id, doc))

    def put_many(self, collection: str, items: List[Tuple[str, Dict[str, Any]]]):
        """Batch insert/update for efficiency."""
        return self._run_async(self.put_many_async(collection, items))

    def delete(self, collection: str, item_id: str):
        """Delete an item by collection and ID."""
        return self._run_async(self.delete_async(collection, item_id))

    def delete_where_prefix(self, collection: str, prefix: str):
        """Delete rows matching prefix."""
        return self._run_async(self.delete_where_prefix_async(collection, prefix))

    # ---------------- Internal Async Implementations ----------------

    async def _all_traders(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Trader)
            res = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "id": t.id,
                    "name": t.name,
                    "ra": t.ra,
                    "style": t.style,
                    "holding": t.holding,
                    "segment": t.segment,
                    "testCapital": float(t.test_capital or 0),
                    "status": t.status,
                    "start": t.start,
                }
                for t in res
            ]

    async def _get_trader(self, tid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Trader).where(Trader.id == tid)
            t = (await session.execute(stmt)).scalar_one_or_none()
            if not t:
                return None
            return {
                "id": t.id,
                "name": t.name,
                "ra": t.ra,
                "style": t.style,
                "holding": t.holding,
                "segment": t.segment,
                "testCapital": float(t.test_capital or 0),
                "status": t.status,
                "start": t.start,
            }

    async def _put_trader(self, tid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(Trader).where(Trader.id == tid)
            t = (await session.execute(stmt)).scalar_one_or_none()
            if t:
                t.name = doc.get("name", t.name)
                t.ra = doc.get("ra", t.ra)
                t.style = doc.get("style", t.style)
                t.holding = doc.get("holding", t.holding)
                t.segment = doc.get("segment", t.segment)
                t.test_capital = doc.get("testCapital", t.test_capital)
                t.status = doc.get("status", t.status)
                t.start = doc.get("start", t.start)
            else:
                t = Trader(
                    id=tid,
                    name=doc.get("name", tid),
                    ra=doc.get("ra"),
                    style=doc.get("style"),
                    holding=doc.get("holding"),
                    segment=doc.get("segment"),
                    test_capital=doc.get("testCapital", 0),
                    status=doc.get("status", "Testing"),
                    start=doc.get("start"),
                )
                session.add(t)
            await session.commit()

    async def _all_rules(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(TradingRule)
            res = (await session.execute(stmt)).scalars().all()
            return [{"trader": r.trader_id, "account": getattr(r, "account_id", None) or r.trader_id, "rules": r.rules} for r in res]

    async def _get_rule(self, tid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(TradingRule).where(TradingRule.trader_id == tid)
            r = (await session.execute(stmt)).scalar_one_or_none()
            if not r:
                return None
            return {"trader": r.trader_id, "account": getattr(r, "account_id", None) or r.trader_id, "rules": r.rules}

    async def _put_rule(self, tid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(TradingRule).where(TradingRule.trader_id == tid)
            r = (await session.execute(stmt)).scalar_one_or_none()
            rules_data = doc.get("rules", doc)
            if r:
                r.rules = rules_data
                if doc.get("account"):
                    r.account_id = doc["account"]
            else:
                r = TradingRule(trader_id=tid, account_id=doc.get("account") or tid, rules=rules_data)
                session.add(r)
            await session.commit()

    async def _all_rule_logs(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(RuleLog).order_by(RuleLog.at.asc())
            res = (await session.execute(stmt)).scalars().all()
            return [
                {"id": rl.id, "trader": rl.trader_id, "at": rl.at, "by": rl.by, "change": rl.change}
                for rl in res
            ]

    async def _put_rule_log(self, rlid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            rl = RuleLog(
                id=rlid,
                trader_id=doc["trader"],
                at=doc["at"],
                by=doc["by"],
                change=doc.get("change", ""),
            )
            await session.merge(rl)
            await session.commit()

    async def _all_groups(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Group)
            res = (await session.execute(stmt)).scalars().all()
            return [
                {"id": g.id, "name": g.name, "description": g.description, "traders": g.traders or [], "accounts": g.traders or []}
                for g in res
            ]

    async def _get_group(self, gid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Group).where(Group.id == gid)
            g = (await session.execute(stmt)).scalar_one_or_none()
            if not g:
                return None
            return {"id": g.id, "name": g.name, "description": g.description, "traders": g.traders or [], "accounts": g.traders or []}

    async def _put_group(self, gid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(Group).where(Group.id == gid)
            g = (await session.execute(stmt)).scalar_one_or_none()
            members = doc.get("accounts") if "accounts" in doc else doc.get("traders", [])
            if g:
                g.name = doc.get("name", g.name)
                g.description = doc.get("description", g.description)
                g.traders = members if members is not None else g.traders
            else:
                g = Group(id=gid, name=doc.get("name", gid), description=doc.get("description"), traders=members or [])
                session.add(g)
            await session.commit()

    async def _delete_group(self, gid: str):
        async with AsyncSessionLocal() as session:
            await session.execute(sa_delete(Group).where(Group.id == gid))
            await session.commit()

    async def _all_trades(self, from_date: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Trade)
            if from_date:
                # Include open positions regardless of entry date, or trades whose trade_date or exit_date >= from_date
                stmt = stmt.where(
                    (Trade.is_open == True) | (Trade.trade_date >= from_date) | (Trade.exit_date >= from_date)
                )
            stmt = stmt.order_by(Trade.trade_date.desc(), Trade.trade_time.desc())
            res = (await session.execute(stmt)).scalars().all()
            return [self._trade_to_dict(t) for t in res]

    async def _get_trade(self, tid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Trade).where(Trade.id == tid)
            t = (await session.execute(stmt)).scalar_one_or_none()
            return self._trade_to_dict(t) if t else None

    async def _put_trade(self, tid: str, doc: Dict[str, Any], allow_overwrite_finalized: bool = False):
        async with AsyncSessionLocal() as session:
            stmt = select(Trade).where(Trade.id == tid)
            t = (await session.execute(stmt)).scalar_one_or_none()
            if t:
                if getattr(t, "is_finalized", False) and not allow_overwrite_finalized:
                    # Only allow setting is_archived on a finalized trade; never mutate financial values
                    if doc.get("is_archived") and not getattr(t, "is_archived", False):
                        t.is_archived = True
                        t.archived_at = doc.get("archived_at") or datetime.now(timezone.utc)
                        await session.commit()
                    return
                self._update_trade_model(t, doc)
            else:
                t = Trade(id=tid)
                self._update_trade_model(t, doc)
                session.add(t)
            await session.commit()

    async def _put_many_trades(self, items: List[Tuple[str, Dict[str, Any]]], allow_overwrite_finalized: bool = False):
        async with AsyncSessionLocal() as session:
            for tid, doc in items:
                stmt = select(Trade).where(Trade.id == tid)
                t = (await session.execute(stmt)).scalar_one_or_none()
                if t:
                    if getattr(t, "is_finalized", False) and not allow_overwrite_finalized:
                        if doc.get("is_archived") and not getattr(t, "is_archived", False):
                            t.is_archived = True
                            t.archived_at = doc.get("archived_at") or datetime.now(timezone.utc)
                        continue
                    self._update_trade_model(t, doc)
                else:
                    t = Trade(id=tid)
                    self._update_trade_model(t, doc)
                    session.add(t)
            await session.commit()

    async def _delete_trade(self, tid: str, allow_delete_finalized: bool = False):
        async with AsyncSessionLocal() as session:
            if not allow_delete_finalized:
                stmt = select(Trade).where(Trade.id == tid)
                t = (await session.execute(stmt)).scalar_one_or_none()
                if t and getattr(t, "is_finalized", False):
                    log.warning(f"Prevented deletion of finalized historical trade {tid}")
                    return
            await session.execute(sa_delete(Trade).where(Trade.id == tid))
            await session.commit()

    async def _delete_trades_by_prefix(self, prefix: str):
        async with AsyncSessionLocal() as session:
            await session.execute(
                sa_delete(Trade).where(Trade.id.like(f"{prefix}%"), Trade.is_finalized == False)
            )
            await session.commit()

    def _update_trade_model(self, t: Trade, doc: Dict[str, Any]):
        t.origin = doc.get("origin") or t.origin or "import"
        t.account_id = doc.get("account") or t.account_id
        t.trader_id = doc.get("trader") or t.trader_id
        t.trade_date = doc.get("date") or t.trade_date
        t.trade_time = doc.get("time") or t.trade_time or "09:15"
        t.symbol = doc.get("symbol") or t.symbol or "UNKNOWN"
        t.strike = doc.get("strike", t.strike)
        t.opt = doc.get("opt", t.opt)
        t.expiry = doc.get("expiry", t.expiry)
        t.expiry_type = doc.get("expiryType", t.expiry_type)
        t.tsym = doc.get("tsym") or doc.get("symbol") or t.tsym or "UNKNOWN"
        t.exch = doc.get("exch", t.exch or "NFO")
        t.side = (doc.get("side") or t.side or "BUY").upper()
        lots = doc.get("lots") if doc.get("lots") is not None else (t.lots or 1)
        t.lots = Decimal(str(lots)) if lots is not None else Decimal("1")
        lot_size = doc.get("lotSize") or doc.get("lot_size") or t.lot_size or 1
        t.lot_size = int(lot_size)
        qty = doc.get("qty")
        if qty is None:
            qty = float(t.lots) * int(t.lot_size)
        t.qty = Decimal(str(qty))
        entry_val = doc.get("entry") if doc.get("entry") is not None else (t.entry_price if t.entry_price is not None else 0.0)
        t.entry_price = Decimal(str(entry_val))
        if "exit" in doc:
            t.exit_price = Decimal(str(doc["exit"])) if doc["exit"] is not None else None
        elif t.exit_price is None:
            t.exit_price = t.entry_price

        is_open = bool(doc.get("open", False))
        t.is_open = is_open

        if "pnl" in doc:
            t.pnl = Decimal(str(doc["pnl"])) if doc["pnl"] is not None else None
        if "realized_pnl" in doc:
            t.realized_pnl = Decimal(str(doc["realized_pnl"])) if doc["realized_pnl"] is not None else None
        elif not is_open and t.pnl is not None and t.realized_pnl is None:
            t.realized_pnl = t.pnl
        if "unrealized_pnl" in doc:
            t.unrealized_pnl = Decimal(str(doc["unrealized_pnl"])) if doc["unrealized_pnl"] is not None else None
        elif is_open and t.pnl is not None:
            t.unrealized_pnl = t.pnl

        charges_val = doc.get("charges") if doc.get("charges") is not None else (t.charges if t.charges is not None else 0.0)
        t.charges = Decimal(str(charges_val))
        t.charges_estimated = doc.get("chargesEstimated", True)

        if "net_pnl" in doc and doc["net_pnl"] is not None:
            t.net_pnl = Decimal(str(doc["net_pnl"]))
        elif t.pnl is not None:
            t.net_pnl = Decimal(str(t.pnl)) - Decimal(str(t.charges or 0))

        if "pnl_source" in doc:
            t.pnl_source = doc["pnl_source"]

        if is_open:
            t.exit_date = doc.get("exitDate")  # Should be None for open positions
            t.exit_time = doc.get("exitTime", "OPEN")
        else:
            t.exit_date = doc.get("exitDate") or t.exit_date or t.trade_date
            t.exit_time = doc.get("exitTime") or t.exit_time or t.trade_time

        t.spot = doc.get("spot", t.spot)
        t.product = doc.get("product", t.product or "MIS")
        t.source = doc.get("source", t.source or "Manual")
        t.hedged = bool(doc.get("hedged", False))

        if "parent_trade_id" in doc:
            t.parent_trade_id = doc["parent_trade_id"]
        if "entry_order_id" in doc:
            t.entry_order_id = doc["entry_order_id"]
        if "exit_order_id" in doc:
            t.exit_order_id = doc["exit_order_id"]
        if "is_finalized" in doc:
            t.is_finalized = bool(doc["is_finalized"])
        if "finalized_at" in doc and doc["finalized_at"]:
            t.finalized_at = doc["finalized_at"] if isinstance(doc["finalized_at"], datetime) else datetime.now(timezone.utc)
        if "is_archived" in doc:
            t.is_archived = bool(doc["is_archived"])
        if "archived_at" in doc and doc["archived_at"]:
            t.archived_at = doc["archived_at"] if isinstance(doc["archived_at"], datetime) else datetime.now(timezone.utc)

    def _trade_to_dict(self, t: Trade) -> Dict[str, Any]:
        lots = float(t.lots) if t.lots is not None else 1.0
        lot_size = int(t.lot_size) if t.lot_size is not None else 1
        qty = float(t.qty) if t.qty is not None else (lots * lot_size)
        entry_f = float(t.entry_price) if t.entry_price is not None else 0.0
        exit_f = float(t.exit_price) if t.exit_price is not None else entry_f
        pnl_f = float(t.pnl) if getattr(t, "pnl", None) is not None else None
        realized_f = float(t.realized_pnl) if getattr(t, "realized_pnl", None) is not None else (pnl_f if not t.is_open else 0.0)
        unrealized_f = float(t.unrealized_pnl) if getattr(t, "unrealized_pnl", None) is not None else (pnl_f if t.is_open else 0.0)
        charges_f = float(t.charges) if t.charges is not None else 0.0
        net_f = float(t.net_pnl) if getattr(t, "net_pnl", None) is not None else ((pnl_f - charges_f) if pnl_f is not None else None)
        return {
            "id": t.id,
            "parent_trade_id": getattr(t, "parent_trade_id", None) or t.id,
            "entry_order_id": getattr(t, "entry_order_id", None),
            "exit_order_id": getattr(t, "exit_order_id", None),
            "origin": t.origin,
            "date": t.trade_date,
            "time": t.trade_time,
            "trader": t.trader_id,
            "account": t.account_id,
            "symbol": t.symbol,
            "strike": float(t.strike) if t.strike is not None else None,
            "opt": t.opt,
            "expiry": t.expiry,
            "expiryType": t.expiry_type,
            "tsym": t.tsym,
            "exch": t.exch,
            "side": t.side,
            "lots": lots,
            "lotSize": lot_size,
            "qty": qty,
            "entry": entry_f,
            "exit": exit_f,
            "pnl": pnl_f,
            "realized_pnl": realized_f,
            "unrealized_pnl": unrealized_f,
            "net_pnl": net_f,
            "pnl_source": getattr(t, "pnl_source", "calculated"),
            "open": t.is_open,
            "exitDate": t.exit_date,
            "exitTime": t.exit_time,
            "spot": float(t.spot) if t.spot is not None else None,
            "product": t.product,
            "source": t.source,
            "charges": charges_f,
            "chargesEstimated": t.charges_estimated,
            "hedged": t.hedged,
            "is_finalized": bool(getattr(t, "is_finalized", False)),
            "finalized_at": t.finalized_at.isoformat() if getattr(t, "finalized_at", None) else None,
            "is_archived": bool(getattr(t, "is_archived", False)),
            "archived_at": t.archived_at.isoformat() if getattr(t, "archived_at", None) else None,
        }

    async def purge_synthetic_fills_and_pb_trades_async(self, aid: str, today_str: str, unlock_today: bool = False) -> int:
        """Remove any legacy PARTIAL-* synthetic fills and -PB- synthetic trades that could double-count quantities."""
        removed = 0
        async with AsyncSessionLocal() as session:
            r1 = await session.execute(
                sa_delete(Fill).where(Fill.account_id == aid, Fill.flid.like("PARTIAL-%"))
            )
            r2 = await session.execute(
                sa_delete(Trade).where(Trade.account_id == aid, Trade.id.like("%-PB-%"))
            )
            removed = (r1.rowcount or 0) + (r2.rowcount or 0)
            if removed > 0 or unlock_today:
                # Unlock today's trades and today's daily report for this account so they rebuild cleanly from real fills
                await session.execute(
                    sa_update(Trade)
                    .where(Trade.account_id == aid, (Trade.trade_date == today_str) | (Trade.exit_date == today_str))
                    .values(is_finalized=False)
                )
                await session.execute(
                    sa_delete(DailyAccountReport).where(
                        DailyAccountReport.account_id == aid,
                        DailyAccountReport.trade_date == today_str,
                    )
                )
            await session.commit()
        return removed

    async def _all_fills(self, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Fill)
            if account_id:
                stmt = stmt.where(Fill.account_id == account_id)
            res = (await session.execute(stmt)).scalars().all()
            return [
                {
                    **(f.raw_payload or {}),
                    "_acc": f.account_id,
                    "_day": f.trade_day,
                    "norenordno": f.order_no,
                    "flid": f.flid,
                    "tsym": f.tsym,
                    "exch": f.exch,
                    "trantype": f.side,
                    "prd": f.product,
                    "flqty": str(f.qty),
                    "flprc": str(f.price),
                    "ls": str(f.lot_size),
                    "fltm": f.fill_time.strftime("%d-%m-%Y %H:%M:%S") if f.fill_time else "",
                    "ordersource": f.order_source,
                    "remarks": f.remarks,
                }
                for f in res
            ]

    async def _put_many_fills(self, items: List[Tuple[str, Dict[str, Any]]]):
        from app.engine import parse_ts
        async with AsyncSessionLocal() as session:
            for fid, f in items:
                stmt = select(Fill).where(Fill.id == fid)
                row = (await session.execute(stmt)).scalar_one_or_none()
                ft = parse_ts(f.get("fltm"), f.get("exch_tm"), f.get("norentm"))
                if not ft:
                    # Fallback if only trade_day is known
                    day_str = f.get("_day") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    ft = datetime.strptime(f"{day_str} 09:15:00", "%Y-%m-%d %H:%M:%S")
                actual_trade_day = ft.strftime("%Y-%m-%d")
                if not row:
                    row = Fill(
                        id=fid,
                        account_id=f.get("_acc") or f.get("actid", ""),
                        order_no=str(f.get("norenordno", "")),
                        flid=str(f.get("flid", "")),
                        tsym=str(f.get("tsym", "")),
                        exch=f.get("exch", "NFO"),
                        side=f.get("trantype", "B"),
                        product=f.get("prd", "MIS"),
                        qty=Decimal(str(f.get("flqty") or f.get("fillshares") or 0)),
                        price=Decimal(str(f.get("flprc") or f.get("avgprc") or 0)),
                        lot_size=int(float(f.get("ls") or 1)),
                        fill_time=ft,
                        trade_day=actual_trade_day,
                        order_source=f.get("ordersource", ""),
                        remarks=f.get("remarks", ""),
                        token=f.get("token"),
                        raw_payload=f,
                    )
                    session.add(row)
                else:
                    row.raw_payload = f
            await session.commit()

    async def _all_settings(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Setting)
            res = (await session.execute(stmt)).scalars().all()
            return [s.data for s in res]

    async def _get_setting(self, key: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Setting).where(Setting.key == key)
            s = (await session.execute(stmt)).scalar_one_or_none()
            return s.data if s else None

    async def _put_setting(self, key: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(Setting).where(Setting.key == key)
            s = (await session.execute(stmt)).scalar_one_or_none()
            if s:
                s.data = doc
            else:
                s = Setting(key=key, data=doc)
                session.add(s)
            await session.commit()

    async def _get_link_nonce(self, nonce: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(LinkNonce).where(LinkNonce.nonce == nonce)
            ln = (await session.execute(stmt)).scalar_one_or_none()
            if not ln:
                return None
            exp_val = ln.exp
            if exp_val and exp_val.tzinfo is None:
                exp_val = exp_val.replace(tzinfo=timezone.utc)
            return {"account": ln.account_id, "exp": exp_val.timestamp()}

    async def _put_link_nonce(self, nonce: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            exp_dt = datetime.fromtimestamp(doc["exp"], tz=timezone.utc)
            ln = LinkNonce(nonce=nonce, account_id=doc["account"], exp=exp_dt)
            await session.merge(ln)
            await session.commit()

    async def _delete_link_nonce(self, nonce: str):
        async with AsyncSessionLocal() as session:
            await session.execute(sa_delete(LinkNonce).where(LinkNonce.nonce == nonce))
            await session.commit()

    async def _get_sent_flag(self, key: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(SentFlag).where(SentFlag.key == key)
            sf = (await session.execute(stmt)).scalar_one_or_none()
            return {"at": sf.sent_at.isoformat()} if sf else None

    async def _put_sent_flag(self, key: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            sf = SentFlag(key=key, sent_at=datetime.now(timezone.utc))
            await session.merge(sf)
            await session.commit()

    async def _get_status(self, key: str) -> Optional[Dict[str, Any]]:
        return await self._get_setting(f"status_{key}")

    async def _put_status(self, key: str, doc: Dict[str, Any]):
        return await self._put_setting(f"status_{key}", doc)

    async def _all_status(self) -> List[Dict[str, Any]]:
        return []

    async def _all_sessions(self) -> List[Dict[str, Any]]:
        return []

    async def _all_tokens(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(BrokerSession)
            res = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "account": bs.account_id,
                    "access_token": decrypt_token(bs.access_token_enc),
                    "refresh_token": decrypt_token(bs.refresh_token_enc),
                    "expires_at": bs.expires_at.timestamp() if bs.expires_at else None,
                    "linked_at": bs.linked_at.isoformat() if bs.linked_at else None,
                    "last_sync_at": bs.last_sync_at.isoformat() if getattr(bs, "last_sync_at", None) else None,
                    "cliname": bs.cliname,
                    "status": bs.status,
                    "ws_status": getattr(bs, "ws_status", None),
                    "sync_message": getattr(bs, "sync_message", None),
                }
                for bs in res
            ]

    async def _all_sent_flags(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(SentFlag)
            res = (await session.execute(stmt)).scalars().all()
            return [{"key": sf.key, "at": sf.sent_at.isoformat()} for sf in res]
