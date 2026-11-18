"""Market & Ledger entities: Instruments, Fills, Orders, Trades, Positions, Position Lots."""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    Date,
    Numeric,
    Integer,
    Boolean,
    ForeignKey,
    Index,
    JSON,
)
from sqlalchemy.orm import relationship
from app.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class Instrument(Base):
    __tablename__ = "instruments"

    symbol = Column(String(100), primary_key=True)  # e.g. BANKNIFTY24NOV26C51700
    token = Column(String(50), nullable=True, index=True)
    exch = Column(String(20), nullable=False, default="NFO")
    underlying = Column(String(50), nullable=False, index=True)
    instrument_type = Column(String(30), nullable=False)  # OPTIDX, OPTSTK, FUTIDX, FUTSTK, EQ
    strike = Column(Numeric(12, 2), nullable=True)
    opt_type = Column(String(10), nullable=True)  # CE / PE
    expiry_date = Column(Date, nullable=True, index=True)
    expiry_type = Column(String(20), nullable=True)  # Monthly / Weekly
    lot_size = Column(Integer, nullable=False, default=1)
    tick_size = Column(Numeric(8, 4), nullable=True, default=0.05)
    updated_at = Column(DateTime(timezone=True), default=utc_now)


class Order(Base):
    __tablename__ = "orders"

    id = Column(String(100), primary_key=True)  # AccountID|norenordno
    account_id = Column(String(100), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    norenordno = Column(String(100), nullable=False, index=True)
    exch = Column(String(20), nullable=True)
    tsym = Column(String(100), nullable=False, index=True)
    side = Column(String(10), nullable=False)  # B / S
    product = Column(String(20), nullable=False, default="MIS")
    price_type = Column(String(20), nullable=True, default="MKT")
    status = Column(String(50), nullable=False, default="COMPLETE")
    qty = Column(Numeric(12, 2), nullable=False, default=0)
    filled_qty = Column(Numeric(12, 2), nullable=False, default=0)
    avg_price = Column(Numeric(12, 4), nullable=False, default=0)
    order_source = Column(String(50), nullable=True)
    remarks = Column(Text, nullable=True)
    order_time = Column(DateTime(timezone=True), nullable=False, index=True)
    raw_payload = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)


class Fill(Base):
    __tablename__ = "fills"

    id = Column(String(150), primary_key=True)  # AccountID|norenordno|flid
    account_id = Column(String(100), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    order_no = Column(String(100), nullable=False, index=True)
    flid = Column(String(100), nullable=True)
    tsym = Column(String(100), nullable=False, index=True)
    exch = Column(String(20), nullable=True, default="NFO")
    side = Column(String(10), nullable=False)  # B / S
    product = Column(String(20), nullable=False, default="MIS")
    qty = Column(Numeric(12, 2), nullable=False)
    price = Column(Numeric(12, 4), nullable=False)
    lot_size = Column(Integer, nullable=False, default=1)
    fill_time = Column(DateTime(timezone=True), nullable=False, index=True)
    trade_day = Column(String(20), nullable=False, index=True)  # YYYY-MM-DD
    order_source = Column(String(50), nullable=True)
    remarks = Column(Text, nullable=True)
    token = Column(String(50), nullable=True)
    raw_payload = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now)

    account_rel = relationship("Account", back_populates="fills")

    __table_args__ = (
        Index("ix_fills_account_day", "account_id", "trade_day"),
        Index("ix_fills_account_tsym", "account_id", "tsym"),
    )


class Trade(Base):
    """Aggregated trade row matching static/index.html schema."""
    __tablename__ = "trades"

    id = Column(String(120), primary_key=True)  # Z-account-oid or I-rand
    origin = Column(String(50), nullable=False, default="zebu")  # zebu / import
    account_id = Column(String(100), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    trader_id = Column(String(100), ForeignKey("traders.id", ondelete="CASCADE"), nullable=True, index=True)
    trade_date = Column(String(20), nullable=False, index=True)  # YYYY-MM-DD
    trade_time = Column(String(20), nullable=False)  # HH:MM
    symbol = Column(String(50), nullable=False, index=True)  # Underlying symbol e.g. BANKNIFTY
    strike = Column(Numeric(12, 2), nullable=True)
    opt = Column(String(10), nullable=True)  # CE / PE
    expiry = Column(String(30), nullable=True)
    expiry_type = Column(String(20), nullable=True)  # Monthly / Weekly
    tsym = Column(String(100), nullable=False, index=True)
    exch = Column(String(20), nullable=True, default="NFO")
    side = Column(String(10), nullable=False)  # BUY / SELL
    lots = Column(Numeric(10, 2), nullable=False)
    lot_size = Column(Integer, nullable=False, default=1)
    qty = Column(Numeric(12, 2), nullable=False)
    entry_price = Column(Numeric(12, 4), nullable=False)
    exit_price = Column(Numeric(12, 4), nullable=False)
    pnl = Column(Numeric(14, 2), nullable=True)
    is_open = Column(Boolean, nullable=False, default=False, index=True)
    exit_date = Column(String(20), nullable=True)
    exit_time = Column(String(20), nullable=True)
    spot = Column(Numeric(12, 2), nullable=True)
    product = Column(String(20), nullable=False, default="MIS")  # MIS / NRML
    source = Column(String(20), nullable=False, default="Manual")  # Manual / Algo
    charges = Column(Numeric(12, 2), nullable=False, default=0.0)
    charges_estimated = Column(Boolean, nullable=False, default=True)
    hedged = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    account_rel = relationship("Account", back_populates="trades")

    __table_args__ = (
        Index("ix_trades_acc_date", "account_id", "trade_date"),
        Index("ix_trades_trader_date", "trader_id", "trade_date"),
        Index("ix_trades_date_sym", "trade_date", "symbol"),
    )


class Position(Base):
    """Live open position record."""
    __tablename__ = "positions"

    id = Column(String(120), primary_key=True)  # account_id|tsym|product
    account_id = Column(String(100), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    tsym = Column(String(100), nullable=False, index=True)
    product = Column(String(20), nullable=False, default="MIS")
    exch = Column(String(20), nullable=False, default="NFO")
    net_qty = Column(Numeric(12, 2), nullable=False, default=0)
    buy_qty = Column(Numeric(12, 2), nullable=False, default=0)
    sell_qty = Column(Numeric(12, 2), nullable=False, default=0)
    buy_avg = Column(Numeric(12, 4), nullable=False, default=0)
    sell_avg = Column(Numeric(12, 4), nullable=False, default=0)
    ltp = Column(Numeric(12, 4), nullable=True, default=0)
    realized_pnl = Column(Numeric(14, 2), nullable=False, default=0)
    unrealized_pnl = Column(Numeric(14, 2), nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), default=utc_now)

    account_rel = relationship("Account", back_populates="positions")


class PositionLot(Base):
    """Persistent FIFO open lot tracking across days (supports > 10 days carry-forward)."""
    __tablename__ = "position_lots"

    id = Column(String(120), primary_key=True)  # lot UUID
    account_id = Column(String(100), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    tsym = Column(String(100), nullable=False, index=True)
    side = Column(String(10), nullable=False)  # B / S
    product = Column(String(20), nullable=False, default="MIS")
    original_qty = Column(Numeric(12, 2), nullable=False)
    remaining_qty = Column(Numeric(12, 2), nullable=False, index=True)
    entry_price = Column(Numeric(12, 4), nullable=False)
    entry_time = Column(DateTime(timezone=True), nullable=False, index=True)
    order_id = Column(String(100), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utc_now)

    __table_args__ = (
        Index("ix_lots_acc_tsym_rem", "account_id", "tsym", "remaining_qty"),
    )
