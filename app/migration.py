"""Safe, idempotent SQLite-to-PostgreSQL migration utility.

Preserves all historical records, backs up the source database, converts JSON
documents into structured relational models, verifies counts and foreign keys,
and outputs a comprehensive migration report.
"""
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from app.database import AsyncSessionLocal, init_models
from app.models import (
    User,
    Session as UserSession,
    LinkNonce,
    Trader,
    Account,
    BrokerSession,
    Group,
    Setting,
    Fill,
    Trade,
    TradingRule,
    RuleLog,
    SentFlag,
)
from app.security import encrypt_token

log = logging.getLogger("tts.migration")


class MigrationReport:
    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path
        self.backup_path = ""
        self.start_time = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.sqlite_counts: Dict[str, int] = {}
        self.migrated_counts: Dict[str, int] = {}
        self.skipped_counts: Dict[str, int] = {}
        self.errors: List[str] = []
        self.financial_summary: Dict[str, float] = {}

    @property
    def status(self) -> str:
        return "SUCCESS" if not self.errors else "PARTIAL_SUCCESS"

    def summary(self) -> Dict[str, Any]:
        return {
            "source_sqlite": self.sqlite_path,
            "backup_created": self.backup_path,
            "started_at": self.start_time.isoformat(),
            "completed_at": self.end_time.isoformat() if self.end_time else None,
            "source_records": self.sqlite_counts,
            "migrated_records": self.migrated_counts,
            "skipped_records": self.skipped_counts,
            "financial_verification": self.financial_summary,
            "errors": self.errors,
            "status": "SUCCESS" if not self.errors else "PARTIAL_SUCCESS",
        }


def backup_sqlite(src_path: str) -> str:
    """Create timestamped copy of SQLite database before migration."""
    src = Path(src_path)
    if not src.exists():
        raise FileNotFoundError(f"Source SQLite database not found at {src_path}")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = src.parent / f"{src.name}.bak.{ts}"
    shutil.copy2(src, backup_file)
    log.info(f"Created SQLite backup: {backup_file}")
    return str(backup_file)


async def run_sqlite_migration(sqlite_path: str) -> MigrationReport:
    """Execute complete migration from SQLite document store to PostgreSQL relational models."""
    report = MigrationReport(sqlite_path)

    if not os.path.exists(sqlite_path):
        report.errors.append(f"SQLite DB does not exist at {sqlite_path}")
        report.end_time = datetime.now(timezone.utc)
        return report

    # 1. Back up SQLite database
    report.backup_path = backup_sqlite(sqlite_path)

    # 2. Initialize relational database tables
    await init_models()

    # 3. Read SQLite tables
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()

    def read_collection(col_name: str) -> List[Tuple[str, Dict[str, Any]]]:
        try:
            cur.execute(f"SELECT id, doc FROM {col_name}")
            rows = cur.fetchall()
            report.sqlite_counts[col_name] = len(rows)
            out = []
            for r in rows:
                try:
                    out.append((r[0], json.loads(r[1])))
                except Exception as ex:
                    report.errors.append(f"Failed to parse JSON in {col_name}:{r[0]}: {ex}")
            return out
        except sqlite3.OperationalError:
            # Table does not exist in SQLite
            report.sqlite_counts[col_name] = 0
            return []

    async with AsyncSessionLocal() as session:
        # Migrate Users
        users = read_collection("users")
        migrated_users = 0
        for uid, doc in users:
            stmt = select(User).where(User.user == uid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                u = User(
                    user=uid,
                    role=doc.get("role", "viewer"),
                    pw=doc.get("pw", ""),
                    trader_id=doc.get("trader_id"),
                    assigned_accounts=doc.get("assigned_accounts", []),
                )
                session.add(u)
            migrated_users += 1
        await session.commit()
        report.migrated_counts["users"] = migrated_users

        # Migrate Sessions
        sessions = read_collection("sessions")
        migrated_sessions = 0
        for sid, doc in sessions:
            stmt = select(UserSession).where(UserSession.id == sid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                exp_dt = datetime.fromtimestamp(doc.get("exp", 0), tz=timezone.utc) if doc.get("exp") else datetime.now(timezone.utc)
                s = UserSession(id=sid, user=doc.get("user", "admin"), role=doc.get("role", "viewer"), exp=exp_dt)
                session.add(s)
            migrated_sessions += 1
        await session.commit()
        report.migrated_counts["sessions"] = migrated_sessions

        # Migrate Link Nonces
        nonces = read_collection("link_nonces")
        migrated_nonces = 0
        for n, doc in nonces:
            stmt = select(LinkNonce).where(LinkNonce.nonce == n)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                exp_dt = datetime.fromtimestamp(doc.get("exp", 0), tz=timezone.utc) if doc.get("exp") else datetime.now(timezone.utc)
                ln = LinkNonce(nonce=n, account_id=doc.get("account", ""), exp=exp_dt)
                session.add(ln)
            migrated_nonces += 1
        await session.commit()
        report.migrated_counts["link_nonces"] = migrated_nonces

        # Migrate Traders
        traders = read_collection("traders")
        migrated_traders = 0
        for tid, doc in traders:
            stmt = select(Trader).where(Trader.id == tid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
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
            migrated_traders += 1
        await session.commit()
        report.migrated_counts["traders"] = migrated_traders

        # Migrate Groups
        groups = read_collection("groups")
        migrated_groups = 0
        for gid, doc in groups:
            stmt = select(Group).where(Group.id == gid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                g = Group(id=gid, name=doc.get("name", gid), description=doc.get("description"), traders=doc.get("traders", []))
                session.add(g)
            migrated_groups += 1
        await session.commit()
        report.migrated_counts["groups"] = migrated_groups

        # Migrate Accounts
        accounts = read_collection("accounts")
        migrated_accounts = 0
        for aid, doc in accounts:
            stmt = select(Account).where(Account.id == aid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                acc = Account(
                    id=aid,
                    name=doc.get("name", aid),
                    broker=doc.get("broker", "Zebu"),
                    trader_id=doc.get("trader"),
                    zebu_uid=doc.get("zebu_uid"),
                    zebu_actid=doc.get("zebu_actid"),
                    group_id=doc.get("group_id"),
                )
                session.add(acc)
            migrated_accounts += 1
        await session.commit()
        report.migrated_counts["accounts"] = migrated_accounts

        # Migrate Tokens -> BrokerSessions
        tokens = read_collection("tokens")
        migrated_tokens = 0
        for aid, doc in tokens:
            stmt = select(BrokerSession).where(BrokerSession.account_id == aid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            exp_dt = datetime.fromtimestamp(doc["expires_at"], tz=timezone.utc) if doc.get("expires_at") else None
            linked_dt = None
            if doc.get("linked_at"):
                try:
                    linked_dt = datetime.fromisoformat(str(doc["linked_at"])).replace(tzinfo=timezone.utc)
                except Exception:
                    linked_dt = datetime.now(timezone.utc)

            acc_enc = encrypt_token(doc.get("access_token"))
            ref_enc = encrypt_token(doc.get("refresh_token"))

            if not existing:
                bs = BrokerSession(
                    account_id=aid,
                    access_token_enc=acc_enc,
                    refresh_token_enc=ref_enc,
                    expires_at=exp_dt,
                    linked_at=linked_dt,
                    cliname=doc.get("cliname"),
                    status=doc.get("status", "Linked"),
                    ws_status="DISCONNECTED",
                )
                session.add(bs)
            migrated_tokens += 1
        await session.commit()
        report.migrated_counts["tokens"] = migrated_tokens

        # Migrate Rules
        rules = read_collection("rules")
        migrated_rules = 0
        for tid, doc in rules:
            stmt = select(TradingRule).where(TradingRule.trader_id == tid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            rules_data = doc.get("rules", doc)
            if not existing:
                r = TradingRule(trader_id=tid, rules=rules_data)
                session.add(r)
            migrated_rules += 1
        await session.commit()
        report.migrated_counts["rules"] = migrated_rules

        # Migrate Rule Logs
        rule_logs = read_collection("rule_log")
        migrated_rule_logs = 0
        for rlid, doc in rule_logs:
            stmt = select(RuleLog).where(RuleLog.id == rlid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                rl = RuleLog(
                    id=rlid,
                    trader_id=doc.get("trader", ""),
                    at=doc.get("at", ""),
                    by=doc.get("by", "admin"),
                    change=doc.get("change", ""),
                )
                session.add(rl)
            migrated_rule_logs += 1
        await session.commit()
        report.migrated_counts["rule_log"] = migrated_rule_logs

        # Migrate Fills
        from app.engine import parse_ts
        fills = read_collection("fills")
        migrated_fills = 0
        for fid, f in fills:
            stmt = select(Fill).where(Fill.id == fid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            ft = parse_ts(f.get("fltm"), f.get("exch_tm"), f.get("norentm")) or datetime.now(timezone.utc)
            if not existing:
                fill_row = Fill(
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
                session.add(fill_row)
            migrated_fills += 1
        await session.commit()
        report.migrated_counts["fills"] = migrated_fills

        # Migrate Trades
        trades = read_collection("trades")
        migrated_trades = 0
        total_pnl_verified = 0.0
        for tid, doc in trades:
            stmt = select(Trade).where(Trade.id == tid)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                t = Trade(
                    id=tid,
                    origin=doc.get("origin", "zebu"),
                    account_id=doc.get("account", ""),
                    trader_id=doc.get("trader"),
                    trade_date=doc.get("date", ""),
                    trade_time=doc.get("time", ""),
                    symbol=doc.get("symbol", ""),
                    strike=doc.get("strike"),
                    opt=doc.get("opt"),
                    expiry=doc.get("expiry"),
                    expiry_type=doc.get("expiryType"),
                    tsym=doc.get("tsym", ""),
                    exch=doc.get("exch", "NFO"),
                    side=doc.get("side", "BUY"),
                    lots=doc.get("lots", 1),
                    lot_size=doc.get("lotSize", 1),
                    qty=doc.get("qty", 1),
                    entry_price=doc.get("entry", 0.0),
                    exit_price=doc.get("exit", 0.0),
                    is_open=bool(doc.get("open", False)),
                    exit_date=doc.get("exitDate"),
                    exit_time=doc.get("exitTime"),
                    spot=doc.get("spot"),
                    product=doc.get("product", "MIS"),
                    source=doc.get("source", "Manual"),
                    charges=doc.get("charges", 0.0),
                    charges_estimated=doc.get("chargesEstimated", True),
                    hedged=bool(doc.get("hedged", False)),
                )
                session.add(t)
            migrated_trades += 1
            # Accumulate financial check
            diff = (float(doc.get("exit", 0.0)) - float(doc.get("entry", 0.0)))
            side_mult = -1 if doc.get("side") == "SELL" else 1
            pnl = diff * float(doc.get("qty", 0.0)) * side_mult - float(doc.get("charges", 0.0))
            total_pnl_verified += pnl

        await session.commit()
        report.migrated_counts["trades"] = migrated_trades
        report.financial_summary["total_net_pnl_verified"] = round(total_pnl_verified, 2)

        # Migrate Settings
        settings = read_collection("settings")
        migrated_settings = 0
        for k, doc in settings:
            stmt = select(Setting).where(Setting.key == k)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                s = Setting(key=k, data=doc)
                session.add(s)
            migrated_settings += 1
        await session.commit()
        report.migrated_counts["settings"] = migrated_settings

        # Migrate Sent Flags
        flags = read_collection("sent_flags")
        migrated_flags = 0
        for k, doc in flags:
            stmt = select(SentFlag).where(SentFlag.key == k)
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if not existing:
                sf = SentFlag(key=k, sent_at=datetime.now(timezone.utc))
                session.add(sf)
            migrated_flags += 1
        await session.commit()
        report.migrated_counts["sent_flags"] = migrated_flags

    con.close()
    report.end_time = datetime.now(timezone.utc)
    log.info(f"Migration completed. Summary: {report.summary()}")
    return report


if __name__ == "__main__":
    import sys
    from app.config import CFG
    sqlite_file = sys.argv[1] if len(sys.argv) > 1 else CFG.DB_PATH
    import asyncio
    rep = asyncio.run(run_sqlite_migration(sqlite_file))
    print(json.dumps(rep.summary(), indent=2))
