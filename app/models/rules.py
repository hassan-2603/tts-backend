"""Trading rules and rule modification logs."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    JSON,
    Index,
)
from app.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class TradingRule(Base):
    __tablename__ = "trading_rules"

    trader_id = Column(String(100), primary_key=True)  # trader_id or account_id
    account_id = Column(String(100), nullable=True, index=True)
    rules = Column(JSON, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class RuleLog(Base):
    __tablename__ = "rule_logs"

    id = Column(String(64), primary_key=True)
    trader_id = Column(String(100), nullable=False, index=True)  # trader_id or account_id
    at = Column(String(50), nullable=False, index=True)  # YYYY-MM-DD HH:MM
    by = Column(String(100), nullable=False)
    change = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now)
