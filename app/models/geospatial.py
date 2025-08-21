from datetime import date, datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class GeoPoint(BaseModel):
    """Geographic point (latitude, longitude)"""

    latitude: float = Field(
        ..., ge=-90, le=90, description="Latitude in decimal degrees"
    )
    longitude: float = Field(
        ..., ge=-180, le=180, description="Longitude in decimal degrees"
    )


class CompanyLocation(BaseModel):
    """Company with location information"""

    ticker: str
    name: str
    address: Optional[str] = None
    latitude: float
    longitude: float
    distance_km: Optional[float] = None
    # Optional market data fields (populated when include_market_data=True)
    latest_price: Optional[float] = None
    latest_volume: Optional[int] = None
    last_updated: Optional[datetime] = None


class SpatialQueryRequest(BaseModel):
    """Base class for spatial queries"""

    limit: int = Field(
        100, ge=1, le=1000, description="Maximum number of results"
    )
    include_market_data: bool = Field(
        False, description="Include latest OHLCV data"
    )


class CircleQueryRequest(SpatialQueryRequest):
    """Query companies within a circular area"""

    center: GeoPoint
    radius_km: float = Field(
        ..., gt=0, le=1000, description="Radius in kilometers"
    )


class PolygonQueryRequest(SpatialQueryRequest):
    """Query companies within a polygon"""

    coordinates: List[GeoPoint] = Field(..., min_items=3, max_items=100)

    @field_validator("coordinates")
    def validate_polygon(cls, v):
        if len(v) < 3:
            raise ValueError("Polygon must have at least 3 coordinates")
        return v


class StateQueryRequest(SpatialQueryRequest):
    """Query companies within a US state"""

    state: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="US state code (e.g., 'CA', 'NY')",
    )

    @field_validator("state")
    def validate_state_code(cls, v):
        return v.upper()


class RegionStatsRequest(BaseModel):
    """Request for regional market statistics"""

    region_type: str = Field(..., pattern="^(circle|polygon|state)$")
    region_params: Dict[str, Any] = Field(
        ..., description="Region-specific parameters"
    )
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    metrics: List[str] = Field(
        default=["avg_price", "total_volume", "volatility"],
        description="Metrics to calculate",
    )


class CompanyMarketData(BaseModel):
    """Company with market data"""

    ticker: str
    name: str
    latitude: float
    longitude: float
    distance_km: Optional[float] = None
    latest_price: Optional[float] = None
    price_change_pct: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None


class SpatialQueryResponse(BaseModel):
    """Response for spatial queries"""

    companies: List[CompanyLocation]
    total_found: int
    query_params: Dict[str, Any]
    execution_time_ms: float


class RegionalStats(BaseModel):
    """Regional market statistics"""

    region_description: str
    company_count: int
    avg_price: Optional[float] = None
    median_price: Optional[float] = None
    total_volume: Optional[int] = None
    volatility: Optional[float] = None
    price_range: Optional[Dict[str, float]] = None
    top_companies: Optional[List[Dict[str, Any]]] = None


class RegionStatsResponse(BaseModel):
    """Response for regional statistics"""

    stats: RegionalStats
    period: Dict[str, date]
    execution_time_ms: float


class NearbyCompaniesRequest(BaseModel):
    """Request for companies near a specific ticker"""

    ticker: str
    radius_km: float = Field(
        50.0, gt=0, le=500, description="Search radius in kilometers"
    )
    limit: int = Field(
        20, ge=1, le=100, description="Maximum number of nearby companies"
    )
    include_market_data: bool = Field(
        True, description="Include market data comparison"
    )


class MarketDataWithLocation(BaseModel):
    """Market data point with location"""

    ticker: str
    name: str
    latitude: float
    longitude: float
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    distance_km: Optional[float] = None


class BulkSpatialDataRequest(BaseModel):
    """Request for bulk market data with spatial filtering"""

    spatial_filter: Dict[str, Any] = Field(
        ..., description="Spatial filter parameters"
    )
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    tickers: Optional[List[str]] = Field(
        None, max_items=50, description="Optional ticker filter"
    )
    limit_per_ticker: int = Field(
        100, ge=1, le=1000, description="Records per ticker"
    )


class SpatialAggregationRequest(BaseModel):
    """Request for spatial aggregation analysis"""

    region_type: str = Field(..., pattern="^(circle|polygon|state|grid)$")
    region_params: Dict[str, Any]
    aggregation_type: str = Field(..., pattern="^(daily|weekly|monthly)$")
    start_date: date
    end_date: date
    metrics: List[str] = Field(default=["avg_price", "total_volume"])


class GridCell(BaseModel):
    """Grid cell for spatial aggregation"""

    cell_id: str
    bounds: Dict[
        str, float
    ]  # {"north": lat, "south": lat, "east": lng, "west": lng}
    company_count: int
    avg_price: Optional[float] = None
    total_volume: Optional[int] = None


class SpatialAggregationResponse(BaseModel):
    """Response for spatial aggregation"""

    grid_cells: List[GridCell]
    aggregation_metadata: Dict[str, Any]
    execution_time_ms: float
