"""Configuration management for Trader Tracking System (TTS)."""
import os
import secrets
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


ROOT_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = ROOT_DIR.parent if (ROOT_DIR.name in ("backend", "tts-server")) else ROOT_DIR


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=[
            str(ROOT_DIR / ".env"),
            str(PROJECT_ROOT / ".env"),
            str(Path(".env")),
        ],
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    ENV: str = Field(default="development")
    DEBUG: bool = Field(default=False)
    PUBLIC_URL: str = Field(default="http://localhost:8000")
    FRONTEND_URL: str = Field(default="https://tts-frontend-liart.vercel.app")
    API_V1_STR: str = Field(default="/api")
    SECRET_KEY: str = Field(default_factory=lambda: secrets.token_hex(32))
    TOKEN_ENCRYPTION_KEY: str = Field(
        default_factory=lambda: "TTS_ENCRYPTION_KEY_32_BYTES_HEX1234567890abcdef"[:32]
    )
    CORS_ORIGINS: str = Field(default="*")
    RATE_LIMIT_LOGIN_PER_MINUTE: int = Field(default=15)

    # Service Separation Flags
    RUN_WORKER_IN_PROCESS: bool = Field(default=True)
    RUN_SCHEDULER_IN_PROCESS: bool = Field(default=True)
    RUN_NOTIFICATION_WORKER_IN_PROCESS: bool = Field(default=True)

    # Database
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://tts:tts_pass@localhost:5432/tts_db"
    )
    DATABASE_POOL_SIZE: int = Field(default=25)
    DATABASE_MAX_OVERFLOW: int = Field(default=25)
    DATABASE_TIMEOUT: int = Field(default=20)
    DB_PATH: str = Field(default=str(ROOT_DIR / "tts.db"))  # SQLite fallback / migration source

    # Redis (Optional distributed lock/cache)
    REDIS_URL: str = Field(default="")
    USE_REDIS: bool = Field(default=False)

    # Admin User
    ADMIN_USER: str = Field(default="admin")
    ADMIN_PASSWORD: str = Field(default="change-me")

    # Zebu / MYNT API
    ZEBU_BASE_URL: str = Field(default="https://go.mynt.in")
    ZEBU_WS_URL: str = Field(default="wss://go.mynt.in/NorenWSTP/")
    ZEBU_CLIENT_ID: str = Field(default="")
    ZEBU_SECRET_KEY: str = Field(default="")
    ZEBU_TIMEOUT: float = Field(default=20.0)

    # Concurrency & Sync Timing
    MAX_CONCURRENT_ACCOUNTS: int = Field(default=50)
    ACCOUNT_SYNC_WORKERS: int = Field(default=10)
    POLL_SECONDS: int = Field(default=45)
    MARKET_OPEN: str = Field(default="09:15")
    MARKET_CLOSE: str = Field(default="15:30")
    EOD_FINALIZE_TIME: str = Field(default="15:45")
    EOD_LATE_RECONCILE_TIME: str = Field(default="16:00")
    POST_MARKET_HARD_CUTOFF_HOUR: int = Field(default=21)
    REBUILD_LOOKBACK_DAYS: int = Field(default=30)
    DISABLE_SCHEDULER: bool = Field(default=False)

    # Algo identification
    ALGO_ORDERSOURCES: str = Field(default="API")
    ALGO_REMARK_PREFIX: str = Field(default="")

    # Product map & Charges
    PRODUCT_MAP: str = Field(
        default='{"I":"MIS","M":"NRML","C":"CNC","B":"MIS","H":"MIS"}'
    )
    CHG_BROKERAGE_PER_ORDER: float = Field(default=20.0)
    CHG_STT_SELL_PCT: float = Field(default=0.1)
    CHG_TXN_PCT: float = Field(default=0.03503)
    CHG_STAMP_BUY_PCT: float = Field(default=0.003)

    # Index Tokens & Fallbacks
    INDEX_TOKENS: str = Field(
        default="BANKNIFTY=NSE:26009,NIFTY=NSE:26000,FINNIFTY=NSE:26037,MIDCPNIFTY=NSE:26074,SENSEX=BSE:1"
    )

    # Alerts (Legacy Telegram/SMTP + Resend Email + Meta WhatsApp Cloud API)
    TELEGRAM_BOT_TOKEN: str = Field(default="")
    TELEGRAM_CHAT_ID: str = Field(default="")
    TELEGRAM_API: str = Field(default="https://api.telegram.org")
    WHATSAPP_WEBHOOK_URL: str = Field(default="")
    WHATSAPP_WEBHOOK_TOKEN: str = Field(default="")
    SMTP_HOST: str = Field(default="")
    SMTP_PORT: int = Field(default=587)
    SMTP_USER: str = Field(default="")
    SMTP_PASSWORD: str = Field(default="")
    SMTP_FROM: str = Field(default="")
    SMTP_TLS: bool = Field(default=True)

    # Resend Transactional Email
    RESEND_API_KEY: str = Field(default="")
    RESEND_FROM_EMAIL: str = Field(default="")

    # Meta WhatsApp Cloud API
    WHATSAPP_ACCESS_TOKEN: str = Field(default="")
    WHATSAPP_PHONE_NUMBER_ID: str = Field(default="")
    WHATSAPP_BUSINESS_ACCOUNT_ID: str = Field(default="")
    WHATSAPP_APP_SECRET: str = Field(default="")
    WHATSAPP_VERIFY_TOKEN: str = Field(default="")
    WHATSAPP_API_VERSION: str = Field(default="v21.0")
    WHATSAPP_ALERT_TEMPLATE: str = Field(default="tts_high_red_flag_alert")
    WHATSAPP_SUMMARY_TEMPLATE: str = Field(default="tts_daily_trading_summary")
    WHATSAPP_TEMPLATE_LANG: str = Field(default="en_US")
    WHATSAPP_DEFAULT_COUNTRY_CODE: str = Field(default="91")

    # Notification Worker Tuning
    NOTIFICATION_POLL_SECONDS: int = Field(default=5)
    NOTIFICATION_MAX_ATTEMPTS: int = Field(default=5)
    NOTIFICATION_STALE_LOCK_SECONDS: int = Field(default=300)
    DAILY_REPORT_GRACE_MINUTES: int = Field(default=15)

    @field_validator("ADMIN_PASSWORD")
    @classmethod
    def check_admin_password(cls, v: str, info) -> str:
        env = os.environ.get("ENV", "development").lower()
        if env == "production" and v in ("change-me", "admin", "password", "123456"):
            raise ValueError("ADMIN_PASSWORD must be changed to a secure password in production environment")
        return v

    def get_cors_origins(self) -> List[str]:
        if self.CORS_ORIGINS.strip() == "*":
            return ["*"]
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]


def get_settings() -> Settings:
    return Settings()


CFG = get_settings()
