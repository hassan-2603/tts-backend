"""Account synchronization states, processed event deduplication, and daily sync logs."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
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
    status = Column(String(50), nullable=False, default="IDLE")  # IDLE, SYNCING, STREAMING, ERROR, DEGRADED
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
