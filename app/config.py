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

    # Database
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://tts:tts_pass@localhost:5432/tts_db"
    )
    DATABASE_POOL_SIZE: int = Field(default=20)
    DATABASE_MAX_OVERFLOW: int = Field(default=30)
    DATABASE_TIMEOUT: int = Field(default=30)
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
    POLL_SECONDS: int = Field(default=120)
    MARKET_OPEN: str = Field(default="09:10")
    MARKET_CLOSE: str = Field(default="15:40")
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

    # Alerts
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
