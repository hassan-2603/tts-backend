"""Trading rules and rule modification logs."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    ForeignKey,
    JSON,
    Index,
)
from sqlalchemy.orm import relationship
from app.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class TradingRule(Base):
    __tablename__ = "trading_rules"

    trader_id = Column(String(100), ForeignKey("traders.id", ondelete="CASCADE"), primary_key=True)
    rules = Column(JSON, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    trader_rel = relationship("Trader", back_populates="rules")


class RuleLog(Base):
    __tablename__ = "rule_logs"

    id = Column(String(64), primary_key=True)
    trader_id = Column(String(100), ForeignKey("traders.id", ondelete="CASCADE"), nullable=False, index=True)
    at = Column(String(50), nullable=False, index=True)  # YYYY-MM-DD HH:MM
    by = Column(String(100), nullable=False)
    change = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now)
