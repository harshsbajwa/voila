from datetime import datetime, date
from typing import Optional

from pydantic import BaseModel, Field


class OHLCVRecord(BaseModel):
    """Input OHLCV record for data processing"""

    Date: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Adj_Close: float = Field(..., alias="Adj Close")
    Volume: int
    Dividends: Optional[float] = 0.0
    Stock_Splits: Optional[float] = Field(0.0, alias="Stock Splits")
    Ticker: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True


class OHLCVResponseRecord(BaseModel):
    """Basic OHLCV data for API responses"""

    Date: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: int

    class Config:
        from_attributes = True


class CompanyRecord(BaseModel):
    """Company information record"""

    ticker: str
    name: str
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    class Config:
        from_attributes = True


class CompletedMarketRecord(BaseModel):
    """
    COMPLETE data record matching original spec requirements:
    'company ticker, company name, geocoded location, plaintext address,
    most recent OHLCV data with the possibility of getting more OHLCV data'
    """

    # Company Info
    ticker: str
    company_name: str
    address: Optional[str] = None

    # Geocoded Location
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Most Recent OHLCV Data
    latest_date: date
    latest_open: float
    latest_high: float
    latest_low: float
    latest_close: float
    latest_volume: int

    # Additional Helpful Data
    price_change_24h: Optional[float] = None
    price_change_pct_24h: Optional[float] = None
    avg_volume_30d: Optional[int] = None
    volatility_30d: Optional[float] = None

    class Config:
        from_attributes = True


class CompletedMarketRecordWithHistory(CompletedMarketRecord):
    """Complete market record with historical OHLCV data"""

    historical_data: list[OHLCVResponseRecord] = Field(default_factory=list)

    class Config:
        from_attributes = True
