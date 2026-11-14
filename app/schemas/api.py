"""Pydantic validation schemas for API requests and data transfer."""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    user: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1)


class UserUpdateRequest(BaseModel):
    password: Optional[str] = Field(default=None)
    role: Optional[str] = Field(default="viewer")
    trader_id: Optional[str] = Field(default=None)
    assigned_accounts: Optional[List[str]] = Field(default_factory=list)


class TraderCreateUpdateRequest(BaseModel):
    id: Optional[str] = Field(default=None)
    name: str = Field(..., min_length=1, max_length=200)
    ra: Optional[str] = Field(default=None)
    style: Optional[str] = Field(default="Manual")
    holding: Optional[str] = Field(default="Intraday")
    segment: Optional[str] = Field(default="Bank Nifty options")
    testCapital: Optional[float] = Field(default=300000.0)
    status: Optional[str] = Field(default="Testing")
    start: Optional[str] = Field(default=None)


class AccountCreateUpdateRequest(BaseModel):
    id: Optional[str] = Field(default=None)
    name: str = Field(..., min_length=1, max_length=200)
    broker: str = Field(default="Zebu")
    trader: Optional[str] = Field(default=None)
    zebu_uid: Optional[str] = Field(default=None)
    zebu_actid: Optional[str] = Field(default=None)
    group_id: Optional[str] = Field(default=None)


class RuleUpdateRequest(BaseModel):
    rules: Dict[str, Any]
    change: Optional[str] = Field(default="Rules updated")


class GroupCreateUpdateRequest(BaseModel):
    id: Optional[str] = Field(default=None)
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(default=None)
    traders: Optional[List[str]] = Field(default_factory=list)


class TradeImportRow(BaseModel):
    id: Optional[str] = Field(default=None)
    origin: Optional[str] = Field(default="import")
    date: str
    time: str
    trader: Optional[str] = Field(default=None)
    account: str
    symbol: str
    strike: Optional[float] = Field(default=None)
    opt: Optional[str] = Field(default=None)
    expiry: Optional[str] = Field(default=None)
    expiryType: Optional[str] = Field(default=None)
    tsym: str
    exch: Optional[str] = Field(default="NFO")
    side: str
    lots: float
    lotSize: Optional[int] = Field(default=1)
    qty: float
    entry: float
    exit: float
    open: Optional[bool] = Field(default=False)
    exitDate: Optional[str] = Field(default=None)
    exitTime: Optional[str] = Field(default=None)
    spot: Optional[float] = Field(default=None)
    product: Optional[str] = Field(default="MIS")
    source: Optional[str] = Field(default="Manual")
    charges: Optional[float] = Field(default=0.0)
    chargesEstimated: Optional[bool] = Field(default=True)
    hedged: Optional[bool] = Field(default=False)
