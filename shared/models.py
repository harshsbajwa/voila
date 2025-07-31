from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class OHLCVRecord(BaseModel):
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
    """
    Defines the data structure for API responses.
    """
    Date: datetime
    Open: float
    High: float
    Low: float
    Close: float
    Volume: int

    class Config:
        from_attributes = True