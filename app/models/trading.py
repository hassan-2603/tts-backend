"""Trading entities: Traders, Accounts, Broker Sessions, Groups, Settings."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    Numeric,
    ForeignKey,
    Index,
    JSON,
    Boolean,
)
from sqlalchemy.orm import relationship
from app.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class Trader(Base):
    __tablename__ = "traders"

    id = Column(String(100), primary_key=True)
    name = Column(String(200), nullable=False)
    ra = Column(String(100), nullable=True)  # Risk analyst / advisor
    style = Column(String(50), nullable=True)  # Manual / Algo / Hybrid
    holding = Column(String(50), nullable=True)  # Intraday / Positional
    segment = Column(String(100), nullable=True)
    test_capital = Column(Numeric(18, 2), nullable=True, default=0)
    status = Column(String(50), nullable=False, default="Testing")
    start = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    accounts = relationship("Account", back_populates="trader_rel")
    rules = relationship("TradingRule", back_populates="trader_rel", uselist=False)


class Account(Base):
    __tablename__ = "accounts"

    id = Column(String(100), primary_key=True)  # e.g. AC-01
    name = Column(String(200), nullable=False)
    broker = Column(String(50), nullable=False, default="Zebu")
    trader_id = Column(String(100), ForeignKey("traders.id", ondelete="SET NULL"), nullable=True, index=True)
    zebu_uid = Column(String(100), nullable=True, index=True)
    zebu_actid = Column(String(100), nullable=True)
    zebu_client_id = Column(String(100), nullable=True)
    zebu_secret_key = Column(String(200), nullable=True)
    proxy_url = Column(String(300), nullable=True)
    group_id = Column(String(100), ForeignKey("groups.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    trader_rel = relationship("Trader", back_populates="accounts")
    broker_session = relationship("BrokerSession", back_populates="account_rel", uselist=False, cascade="all, delete-orphan")
    group_rel = relationship("Group", back_populates="accounts")
    trades = relationship("Trade", back_populates="account_rel")
    fills = relationship("Fill", back_populates="account_rel")
    positions = relationship("Position", back_populates="account_rel")


class BrokerSession(Base):
    __tablename__ = "broker_sessions"

    account_id = Column(String(100), ForeignKey("accounts.id", ondelete="CASCADE"), primary_key=True)
    access_token_enc = Column(Text, nullable=True)  # Encrypted at rest
    refresh_token_enc = Column(Text, nullable=True)  # Encrypted at rest
    expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    issued_at = Column(DateTime(timezone=True), nullable=True)
    linked_at = Column(DateTime(timezone=True), nullable=True)
    cliname = Column(String(200), nullable=True)
    status = Column(String(50), default="Not linked", index=True)  # Linked, Login expired, Not linked, REAUTH_REQUIRED
    ws_status = Column(String(50), default="DISCONNECTED")  # REAL-TIME, DEGRADED, DISCONNECTED
    last_ws_event_at = Column(DateTime(timezone=True), nullable=True)
    last_sync_at = Column(DateTime(timezone=True), nullable=True)
    sync_message = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    account_rel = relationship("Account", back_populates="broker_session")


class Group(Base):
    __tablename__ = "groups"

    id = Column(String(100), primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    traders = Column(JSON, nullable=True, default=list)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    accounts = relationship("Account", back_populates="group_rel")


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(100), primary_key=True, default="main")
    data = Column(JSON, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
