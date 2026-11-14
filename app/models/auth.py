"""Authentication, sessions, link nonces, and audit logs."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    ForeignKey,
    Index,
    JSON,
)
from sqlalchemy.orm import relationship
from app.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    user = Column(String(100), primary_key=True)  # username/user id
    role = Column(String(50), nullable=False, default="viewer")  # admin, trader, viewer
    pw = Column(String(255), nullable=False)
    trader_id = Column(String(100), nullable=True)  # link user to specific trader if role=trader
    assigned_accounts = Column(JSON, nullable=True, default=list)  # list of account IDs viewer/trader can see
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    sessions = relationship("Session", back_populates="user_rel", cascade="all, delete-orphan")


class Session(Base):
    __tablename__ = "sessions"

    id = Column(String(128), primary_key=True)  # bearer token
    user = Column(String(100), ForeignKey("users.user", ondelete="CASCADE"), nullable=False, index=True)
    role = Column(String(50), nullable=False)
    exp = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)

    user_rel = relationship("User", back_populates="sessions")


class LinkNonce(Base):
    __tablename__ = "link_nonces"

    nonce = Column(String(64), primary_key=True)
    account_id = Column(String(100), nullable=False, index=True)
    exp = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(String(64), primary_key=True)
    actor = Column(String(100), nullable=False, index=True)
    action = Column(String(100), nullable=False)
    target = Column(String(200), nullable=True)
    details = Column(JSON, nullable=True)
    ip_address = Column(String(45), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, index=True)
