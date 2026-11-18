"""Account synchronization states, processed event deduplication, and daily finalized reports."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    Numeric,
    Index,
    JSON,
    Integer,
    Boolean,
)
from app.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class AccountSyncState(Base):
    """Tracks concurrency, health, and sync state for each linked trading account."""
    __tablename__ = "account_sync_states"

    account_id = Column(String(100), primary_key=True)
    status = Column(String(50), nullable=False, default="IDLE")  # IDLE, SYNCING, LIVE, STREAMING, STALE, ERROR, DEGRADED, REAUTH_REQUIRED, DISCONNECTED
    is_stale = Column(Boolean, nullable=False, default=False)
    last_sync_start = Column(DateTime(timezone=True), nullable=True)
    last_sync_success = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    error_count = Column(Integer, nullable=False, default=0)
    consecutive_failures = Column(Integer, nullable=False, default=0)
    circuit_open = Column(Boolean, nullable=False, default=False)
    circuit_open_until = Column(DateTime(timezone=True), nullable=True)
    total_fills_today = Column(Integer, nullable=False, default=0)
    total_trades_today = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class ProcessedBrokerEvent(Base):
    """Guarantees idempotency and out-of-order event handling for broker WebSocket feeds."""
    __tablename__ = "processed_broker_events"

    event_id = Column(String(150), primary_key=True)  # account_id|event_type|broker_id
    account_id = Column(String(100), nullable=False, index=True)
    event_type = Column(String(50), nullable=False)  # order, fill, position
    received_at = Column(DateTime(timezone=True), default=utc_now)


class DailySyncRecord(Base):
    """Records completion of EOD sync and daily trader summaries."""
    __tablename__ = "daily_sync_records"

    date_str = Column(String(20), primary_key=True)  # YYYY-MM-DD
    synced_at = Column(DateTime(timezone=True), default=utc_now)
    summary_sent = Column(Boolean, nullable=False, default=False)
    summary_text = Column(Text, nullable=True)
    finalized = Column(Boolean, nullable=False, default=False)
    archived = Column(Boolean, nullable=False, default=False)


class DailyAccountReport(Base):
    """Permanent finalized daily trading summary per account and trading date."""
    __tablename__ = "daily_account_reports"

    id = Column(String(150), primary_key=True)  # account_id|trade_date
    account_id = Column(String(100), nullable=False, index=True)
    trader_id = Column(String(100), nullable=True, index=True)
    trade_date = Column(String(20), nullable=False, index=True)  # YYYY-MM-DD
    status = Column(String(40), nullable=False, default="LIVE")  # LIVE, PENDING_RECONCILIATION, FINALIZED, ARCHIVED
    realized_pnl = Column(Numeric(14, 2), nullable=False, default=0)
    unrealized_pnl = Column(Numeric(14, 2), nullable=False, default=0)
    charges = Column(Numeric(12, 2), nullable=False, default=0)
    net_pnl = Column(Numeric(14, 2), nullable=False, default=0)
    total_trades = Column(Integer, nullable=False, default=0)
    closed_trades = Column(Integer, nullable=False, default=0)
    open_trades = Column(Integer, nullable=False, default=0)
    partially_closed_trades = Column(Integer, nullable=False, default=0)
    winning_trades = Column(Integer, nullable=False, default=0)
    losing_trades = Column(Integer, nullable=False, default=0)
    finalized_at = Column(DateTime(timezone=True), nullable=True)
    archived_at = Column(DateTime(timezone=True), nullable=True)
    source_info = Column(String(100), nullable=True, default="zebu_eod_reconciliation")
    reconciliation_notes = Column(Text, nullable=True)
    trade_details = Column(JSON, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    __table_args__ = (
        Index("ix_daily_rep_acc_date", "account_id", "trade_date"),
        Index("ix_daily_rep_trader_date", "trader_id", "trade_date"),
    )
