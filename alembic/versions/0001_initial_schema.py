"""Initial schema for TTS PostgreSQL database.

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-11-14 03:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Users & Auth
    op.create_table(
        "users",
        sa.Column("user", sa.String(100), primary_key=True),
        sa.Column("role", sa.String(50), nullable=False, server_default="viewer"),
        sa.Column("pw", sa.String(255), nullable=False),
        sa.Column("trader_id", sa.String(100), nullable=True),
        sa.Column("assigned_accounts", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "sessions",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("user", sa.String(100), sa.ForeignKey("users.user", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("role", sa.String(50), nullable=False),
        sa.Column("exp", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "link_nonces",
        sa.Column("nonce", sa.String(64), primary_key=True),
        sa.Column("account_id", sa.String(100), nullable=False, index=True),
        sa.Column("exp", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "audit_logs",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("actor", sa.String(100), nullable=False, index=True),
        sa.Column("action", sa.String(100), nullable=False),
        sa.Column("target", sa.String(200), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("ip_address", sa.String(45), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True, index=True),
    )

    # Traders, Groups, Accounts
    op.create_table(
        "traders",
        sa.Column("id", sa.String(100), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("ra", sa.String(100), nullable=True),
        sa.Column("style", sa.String(50), nullable=True),
        sa.Column("holding", sa.String(50), nullable=True),
        sa.Column("segment", sa.String(100), nullable=True),
        sa.Column("test_capital", sa.Numeric(18, 2), nullable=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="Testing"),
        sa.Column("start", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "groups",
        sa.Column("id", sa.String(100), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("traders", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "accounts",
        sa.Column("id", sa.String(100), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("broker", sa.String(50), nullable=False, server_default="Zebu"),
        sa.Column("trader_id", sa.String(100), sa.ForeignKey("traders.id", ondelete="SET NULL"), nullable=True, index=True),
        sa.Column("zebu_uid", sa.String(100), nullable=True, index=True),
        sa.Column("zebu_actid", sa.String(100), nullable=True),
        sa.Column("group_id", sa.String(100), sa.ForeignKey("groups.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "broker_sessions",
        sa.Column("account_id", sa.String(100), sa.ForeignKey("accounts.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("access_token_enc", sa.Text(), nullable=True),
        sa.Column("refresh_token_enc", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True, index=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("linked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cliname", sa.String(200), nullable=True),
        sa.Column("status", sa.String(50), nullable=True, server_default="Not linked", index=True),
        sa.Column("ws_status", sa.String(50), nullable=True, server_default="DISCONNECTED"),
        sa.Column("last_ws_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sync_message", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "settings",
        sa.Column("key", sa.String(100), primary_key=True),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Market, Instruments, Fills, Trades
    op.create_table(
        "instruments",
        sa.Column("symbol", sa.String(100), primary_key=True),
        sa.Column("token", sa.String(50), nullable=True, index=True),
        sa.Column("exch", sa.String(20), nullable=False, server_default="NFO"),
        sa.Column("underlying", sa.String(50), nullable=False, index=True),
        sa.Column("instrument_type", sa.String(30), nullable=False),
        sa.Column("strike", sa.Numeric(12, 2), nullable=True),
        sa.Column("opt_type", sa.String(10), nullable=True),
        sa.Column("expiry_date", sa.Date(), nullable=True, index=True),
        sa.Column("expiry_type", sa.String(20), nullable=True),
        sa.Column("lot_size", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("tick_size", sa.Numeric(8, 4), nullable=True, server_default="0.05"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "orders",
        sa.Column("id", sa.String(100), primary_key=True),
        sa.Column("account_id", sa.String(100), sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("norenordno", sa.String(100), nullable=False, index=True),
        sa.Column("exch", sa.String(20), nullable=True),
        sa.Column("tsym", sa.String(100), nullable=False, index=True),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("product", sa.String(20), nullable=False, server_default="MIS"),
        sa.Column("price_type", sa.String(20), nullable=True, server_default="MKT"),
        sa.Column("status", sa.String(50), nullable=False, server_default="COMPLETE"),
        sa.Column("qty", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column("filled_qty", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column("avg_price", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("order_source", sa.String(50), nullable=True),
        sa.Column("remarks", sa.Text(), nullable=True),
        sa.Column("order_time", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "fills",
        sa.Column("id", sa.String(150), primary_key=True),
        sa.Column("account_id", sa.String(100), sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("order_no", sa.String(100), nullable=False, index=True),
        sa.Column("flid", sa.String(100), nullable=True),
        sa.Column("tsym", sa.String(100), nullable=False, index=True),
        sa.Column("exch", sa.String(20), nullable=True, server_default="NFO"),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("product", sa.String(20), nullable=False, server_default="MIS"),
        sa.Column("qty", sa.Numeric(12, 2), nullable=False),
        sa.Column("price", sa.Numeric(12, 4), nullable=False),
        sa.Column("lot_size", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("fill_time", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("trade_day", sa.String(20), nullable=False, index=True),
        sa.Column("order_source", sa.String(50), nullable=True),
        sa.Column("remarks", sa.Text(), nullable=True),
        sa.Column("token", sa.String(50), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_fills_account_day", "fills", ["account_id", "trade_day"])
    op.create_index("ix_fills_account_tsym", "fills", ["account_id", "tsym"])

    op.create_table(
        "trades",
        sa.Column("id", sa.String(120), primary_key=True),
        sa.Column("origin", sa.String(50), nullable=False, server_default="zebu"),
        sa.Column("account_id", sa.String(100), sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("trader_id", sa.String(100), sa.ForeignKey("traders.id", ondelete="CASCADE"), nullable=True, index=True),
        sa.Column("trade_date", sa.String(20), nullable=False, index=True),
        sa.Column("trade_time", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(50), nullable=False, index=True),
        sa.Column("strike", sa.Numeric(12, 2), nullable=True),
        sa.Column("opt", sa.String(10), nullable=True),
        sa.Column("expiry", sa.String(30), nullable=True),
        sa.Column("expiry_type", sa.String(20), nullable=True),
        sa.Column("tsym", sa.String(100), nullable=False, index=True),
        sa.Column("exch", sa.String(20), nullable=True, server_default="NFO"),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("lots", sa.Numeric(10, 2), nullable=False),
        sa.Column("lot_size", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("qty", sa.Numeric(12, 2), nullable=False),
        sa.Column("entry_price", sa.Numeric(12, 4), nullable=False),
        sa.Column("exit_price", sa.Numeric(12, 4), nullable=False),
        sa.Column("is_open", sa.Boolean(), nullable=False, server_default="false", index=True),
        sa.Column("exit_date", sa.String(20), nullable=True),
        sa.Column("exit_time", sa.String(20), nullable=True),
        sa.Column("spot", sa.Numeric(12, 2), nullable=True),
        sa.Column("product", sa.String(20), nullable=False, server_default="MIS"),
        sa.Column("source", sa.String(20), nullable=False, server_default="Manual"),
        sa.Column("charges", sa.Numeric(12, 2), nullable=False, server_default="0.0"),
        sa.Column("charges_estimated", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("hedged", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_trades_acc_date", "trades", ["account_id", "trade_date"])
    op.create_index("ix_trades_trader_date", "trades", ["trader_id", "trade_date"])
    op.create_index("ix_trades_date_sym", "trades", ["trade_date", "symbol"])

    op.create_table(
        "positions",
        sa.Column("id", sa.String(120), primary_key=True),
        sa.Column("account_id", sa.String(100), sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("tsym", sa.String(100), nullable=False, index=True),
        sa.Column("product", sa.String(20), nullable=False, server_default="MIS"),
        sa.Column("exch", sa.String(20), nullable=False, server_default="NFO"),
        sa.Column("net_qty", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column("buy_qty", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column("sell_qty", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column("buy_avg", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("sell_avg", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("ltp", sa.Numeric(12, 4), nullable=True, server_default="0"),
        sa.Column("realized_pnl", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("unrealized_pnl", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "position_lots",
        sa.Column("id", sa.String(120), primary_key=True),
        sa.Column("account_id", sa.String(100), sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("tsym", sa.String(100), nullable=False, index=True),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("product", sa.String(20), nullable=False, server_default="MIS"),
        sa.Column("original_qty", sa.Numeric(12, 2), nullable=False),
        sa.Column("remaining_qty", sa.Numeric(12, 2), nullable=False, index=True),
        sa.Column("entry_price", sa.Numeric(12, 4), nullable=False),
        sa.Column("entry_time", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column("order_id", sa.String(100), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_lots_acc_tsym_rem", "position_lots", ["account_id", "tsym", "remaining_qty"])

    # Rules & Rule Logs
    op.create_table(
        "trading_rules",
        sa.Column("trader_id", sa.String(100), sa.ForeignKey("traders.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("rules", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "rule_logs",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("trader_id", sa.String(100), sa.ForeignKey("traders.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("at", sa.String(50), nullable=False, index=True),
        sa.Column("by", sa.String(100), nullable=False),
        sa.Column("change", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Alerts & Delivery Logs
    op.create_table(
        "alerts",
        sa.Column("id", sa.String(120), primary_key=True),
        sa.Column("dedup_key", sa.String(200), nullable=False, unique=True, index=True),
        sa.Column("account_id", sa.String(100), nullable=True, index=True),
        sa.Column("trader_id", sa.String(100), nullable=True, index=True),
        sa.Column("trade_id", sa.String(120), nullable=True, index=True),
        sa.Column("rule_name", sa.String(100), nullable=True),
        sa.Column("severity", sa.String(20), nullable=False, server_default="High"),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True, index=True),
    )

    op.create_table(
        "alert_delivery_logs",
        sa.Column("id", sa.String(120), primary_key=True),
        sa.Column("alert_id", sa.String(120), sa.ForeignKey("alerts.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("channel", sa.String(30), nullable=False),
        sa.Column("recipient", sa.String(200), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="PENDING"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_delivery_channel_status", "alert_delivery_logs", ["channel", "status"])

    op.create_table(
        "sent_flags",
        sa.Column("key", sa.String(200), primary_key=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False),
    )

    # Sync state & Broker events
    op.create_table(
        "account_sync_states",
        sa.Column("account_id", sa.String(100), primary_key=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="IDLE"),
        sa.Column("last_sync_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sync_success", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("circuit_open", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("circuit_open_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("total_fills_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_trades_today", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "processed_broker_events",
        sa.Column("event_id", sa.String(150), primary_key=True),
        sa.Column("account_id", sa.String(100), nullable=False, index=True),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "daily_sync_records",
        sa.Column("date_str", sa.String(20), primary_key=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("summary_sent", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("summary_text", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("daily_sync_records")
    op.drop_table("processed_broker_events")
    op.drop_table("account_sync_states")
    op.drop_table("sent_flags")
    op.drop_table("alert_delivery_logs")
    op.drop_table("alerts")
    op.drop_table("rule_logs")
    op.drop_table("trading_rules")
    op.drop_table("position_lots")
    op.drop_table("positions")
    op.drop_table("trades")
    op.drop_table("fills")
    op.drop_table("orders")
    op.drop_table("instruments")
    op.drop_table("settings")
    op.drop_table("broker_sessions")
    op.drop_table("accounts")
    op.drop_table("groups")
    op.drop_table("traders")
    op.drop_table("audit_logs")
    op.drop_table("link_nonces")
    op.drop_table("sessions")
    op.drop_table("users")
