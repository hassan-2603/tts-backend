"""High-performance relational repository layer replacing the flat key-value document store.

Maintains complete backward compatibility for existing callers while mapping
collections directly to structured relational tables and optimized queries.
"""
import json
import logging
import asyncio
from datetime import datetime, timezone
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
    "groups",
    "settings",
    "users",
    "sessions",
    "tokens",
    "link_nonces",
    "sent_flags",
    "status",
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
            if not bs:
                acc_stmt = select(Account).where((Account.id == aid) | (Account.zebu_uid == aid))
                cur_acc = (await session.execute(acc_stmt)).scalars().first()
                if cur_acc and cur_acc.zebu_uid:
                    other_stmt = select(BrokerSession).join(Account, Account.id == BrokerSession.account_id).where(
                        (Account.zebu_uid == cur_acc.zebu_uid) | (Account.id == cur_acc.id)
                    )
                    bs = (await session.execute(other_stmt)).scalars().first()
            if not bs:
                return None
            exp_dt = bs.expires_at
            if exp_dt and exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            return {
                "access_token": decrypt_token(bs.access_token_enc),
                "refresh_token": decrypt_token(bs.refresh_token_enc),
                "expires_at": exp_dt.timestamp() if exp_dt else None,
                "linked_at": bs.linked_at.isoformat() if bs.linked_at else None,
                "cliname": bs.cliname,
                "status": bs.status,
                "ws_status": bs.ws_status,
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

            acc_token_enc = encrypt_token(doc.get("access_token"))
            ref_token_enc = encrypt_token(doc.get("refresh_token"))

            if bs:
                if acc_token_enc is not None:
                    bs.access_token_enc = acc_token_enc
                if ref_token_enc is not None:
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
            else:
                bs = BrokerSession(
                    account_id=aid,
                    access_token_enc=acc_token_enc,
                    refresh_token_enc=ref_token_enc,
                    expires_at=exp_dt,
                    linked_at=linked_dt,
                    cliname=doc.get("cliname"),
                    status=doc.get("status", "Linked"),
                    ws_status=doc.get("ws_status", "DISCONNECTED"),
                )
                session.add(bs)
            await session.commit()

    async def delete_token_async(self, aid: str):
        async with AsyncSessionLocal() as session:
            await session.execute(sa_delete(BrokerSession).where(BrokerSession.account_id == aid))
            await session.commit()

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
            return [{"trader": r.trader_id, "rules": r.rules} for r in res]

    async def _get_rule(self, tid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(TradingRule).where(TradingRule.trader_id == tid)
            r = (await session.execute(stmt)).scalar_one_or_none()
            if not r:
                return None
            return {"trader": r.trader_id, "rules": r.rules}

    async def _put_rule(self, tid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(TradingRule).where(TradingRule.trader_id == tid)
            r = (await session.execute(stmt)).scalar_one_or_none()
            rules_data = doc.get("rules", doc)
            if r:
                r.rules = rules_data
            else:
                r = TradingRule(trader_id=tid, rules=rules_data)
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
                {"id": g.id, "name": g.name, "description": g.description, "traders": g.traders or []}
                for g in res
            ]

    async def _get_group(self, gid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Group).where(Group.id == gid)
            g = (await session.execute(stmt)).scalar_one_or_none()
            if not g:
                return None
            return {"id": g.id, "name": g.name, "description": g.description, "traders": g.traders or []}

    async def _put_group(self, gid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(Group).where(Group.id == gid)
            g = (await session.execute(stmt)).scalar_one_or_none()
            if g:
                g.name = doc.get("name", g.name)
                g.description = doc.get("description", g.description)
                g.traders = doc.get("traders", g.traders)
            else:
                g = Group(id=gid, name=doc.get("name", gid), description=doc.get("description"), traders=doc.get("traders", []))
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
                stmt = stmt.where(Trade.trade_date >= from_date)
            stmt = stmt.order_by(Trade.trade_date.desc(), Trade.trade_time.desc())
            res = (await session.execute(stmt)).scalars().all()
            return [self._trade_to_dict(t) for t in res]

    async def _get_trade(self, tid: str) -> Optional[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Trade).where(Trade.id == tid)
            t = (await session.execute(stmt)).scalar_one_or_none()
            return self._trade_to_dict(t) if t else None

    async def _put_trade(self, tid: str, doc: Dict[str, Any]):
        async with AsyncSessionLocal() as session:
            stmt = select(Trade).where(Trade.id == tid)
            t = (await session.execute(stmt)).scalar_one_or_none()
            if t:
                self._update_trade_model(t, doc)
            else:
                t = Trade(id=tid)
                self._update_trade_model(t, doc)
                session.add(t)
            await session.commit()

    async def _put_many_trades(self, items: List[Tuple[str, Dict[str, Any]]]):
        async with AsyncSessionLocal() as session:
            for tid, doc in items:
                stmt = select(Trade).where(Trade.id == tid)
                t = (await session.execute(stmt)).scalar_one_or_none()
                if t:
                    self._update_trade_model(t, doc)
                else:
                    t = Trade(id=tid)
                    self._update_trade_model(t, doc)
                    session.add(t)
            await session.commit()

    async def _delete_trade(self, tid: str):
        async with AsyncSessionLocal() as session:
            await session.execute(sa_delete(Trade).where(Trade.id == tid))
            await session.commit()

    async def _delete_trades_by_prefix(self, prefix: str):
        async with AsyncSessionLocal() as session:
            await session.execute(sa_delete(Trade).where(Trade.id.like(f"{prefix}%")))
            await session.commit()

    def _update_trade_model(self, t: Trade, doc: Dict[str, Any]):
        t.origin = doc.get("origin") or t.origin or "import"
        t.account_id = doc.get("account") or t.account_id or "AC-01"
        t.trader_id = doc.get("trader") or t.trader_id
        t.trade_date = doc.get("date") or t.trade_date
        t.trade_time = doc.get("time") or t.trade_time or "10:00"
        t.symbol = doc.get("symbol") or t.symbol or "BANKNIFTY"
        t.strike = doc.get("strike", t.strike)
        t.opt = doc.get("opt", t.opt)
        t.expiry = doc.get("expiry", t.expiry)
        t.expiry_type = doc.get("expiryType", t.expiry_type)
        t.tsym = doc.get("tsym") or doc.get("symbol") or t.tsym or "UNKNOWN"
        t.exch = doc.get("exch", t.exch or "NFO")
        t.side = (doc.get("side") or t.side or "BUY").upper()
        lots = doc.get("lots") if doc.get("lots") is not None else (t.lots or 1)
        t.lots = float(lots) if lots is not None else 1.0
        lot_size = doc.get("lotSize") or doc.get("lot_size") or t.lot_size or 1
        t.lot_size = int(lot_size)
        qty = doc.get("qty")
        if qty is None:
            qty = float(t.lots) * int(t.lot_size)
        t.qty = float(qty)
        t.entry_price = float(doc.get("entry") if doc.get("entry") is not None else (t.entry_price if t.entry_price is not None else 0.0))
        t.exit_price = float(doc.get("exit") if doc.get("exit") is not None else (t.exit_price if t.exit_price is not None else 0.0))
        t.is_open = bool(doc.get("open", False))
        t.exit_date = doc.get("exitDate", t.exit_date or t.trade_date)
        t.exit_time = doc.get("exitTime", t.exit_time or t.trade_time)
        t.spot = doc.get("spot", t.spot)
        t.product = doc.get("product", t.product or "MIS")
        t.source = doc.get("source", t.source or "Manual")
        t.charges = float(doc.get("charges") if doc.get("charges") is not None else (t.charges if t.charges is not None else 0.0))
        t.charges_estimated = doc.get("chargesEstimated", True)
        t.hedged = bool(doc.get("hedged", False))

    def _trade_to_dict(self, t: Trade) -> Dict[str, Any]:
        lots = float(t.lots) if t.lots is not None else 1.0
        lot_size = int(t.lot_size) if t.lot_size is not None else 1
        qty = float(t.qty) if t.qty is not None else (lots * lot_size)
        return {
            "id": t.id,
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
            "entry": float(t.entry_price) if t.entry_price is not None else 0.0,
            "exit": float(t.exit_price) if t.exit_price is not None else 0.0,
            "open": t.is_open,
            "exitDate": t.exit_date,
            "exitTime": t.exit_time,
            "spot": float(t.spot) if t.spot is not None else None,
            "product": t.product,
            "source": t.source,
            "charges": float(t.charges) if t.charges is not None else 0.0,
            "chargesEstimated": t.charges_estimated,
            "hedged": t.hedged,
        }

    async def _all_fills(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(Fill)
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
                ft = parse_ts(f.get("fltm"), f.get("exch_tm"), f.get("norentm")) or datetime.now(timezone.utc)
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
                        qty=float(f.get("flqty") or f.get("fillshares") or 0),
                        price=float(f.get("flprc") or f.get("avgprc") or 0),
                        lot_size=int(float(f.get("ls") or 1)),
                        fill_time=ft,
                        trade_day=f.get("_day") or ft.strftime("%Y-%m-%d"),
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
        # Map status to AccountSyncState or Setting
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
                    "cliname": bs.cliname,
                    "status": bs.status,
                }
                for bs in res
            ]

    async def _all_sent_flags(self) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as session:
            stmt = select(SentFlag)
            res = (await session.execute(stmt)).scalars().all()
            return [{"key": sf.key, "at": sf.sent_at.isoformat()} for sf in res]
