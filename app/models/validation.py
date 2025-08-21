import re
from datetime import date, datetime
from typing import List, Optional
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict,
)


# Base validation patterns and utilities
TICKER_PATTERN = re.compile(r"^[A-Z0-9]{1,10}$")
DANGEROUS_SQL_PATTERNS = [
    re.compile(r";\s*--", re.IGNORECASE),  # SQL comments
    re.compile(r";\s*\/\*", re.IGNORECASE),  # SQL block comments
    re.compile(r"\bunion\b", re.IGNORECASE),  # UNION statements
    re.compile(r"\bselect\b.*\bfrom\b", re.IGNORECASE),  # SELECT statements
    re.compile(r"\bdrop\b", re.IGNORECASE),  # DROP statements
    re.compile(r"\bdelete\b", re.IGNORECASE),  # DELETE statements
    re.compile(r"\binsert\b", re.IGNORECASE),  # INSERT statements
    re.compile(r"\bupdate\b", re.IGNORECASE),  # UPDATE statements
    re.compile(r"\bexec\b", re.IGNORECASE),  # EXEC statements
    re.compile(r"\bxp_\w+", re.IGNORECASE),  # SQL Server extended procedures
]

MIN_DATE = date(2020, 1, 1)
MAX_DATE = date(2030, 12, 31)


def validate_ticker_format(ticker: str) -> str:
    """Validate and normalize a single ticker symbol"""
    if not ticker or not isinstance(ticker, str):
        raise ValueError("Ticker must be a non-empty string")

    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("Ticker cannot be empty after trimming")

    if not TICKER_PATTERN.match(ticker):
        raise ValueError(
            f"Invalid ticker format: '{ticker}'. Must be 1-10 alphanumeric characters"
        )

    return ticker


def validate_search_query(query: str) -> str:
    """Validate search query against SQL injection patterns"""
    if not query or not isinstance(query, str):
        raise ValueError("Search query must be a non-empty string")

    query = query.strip()
    if len(query) < 1:
        raise ValueError("Search query must be at least 1 character")
    if len(query) > 50:
        raise ValueError("Search query must be 50 characters or less")

    # Check for dangerous SQL patterns
    for pattern in DANGEROUS_SQL_PATTERNS:
        if pattern.search(query):
            raise ValueError(
                "Search query contains invalid characters or SQL patterns"
            )

    return query


def validate_coordinate(
    value: float, coord_type: str, min_val: float, max_val: float
) -> float:
    """Validate geographic coordinate"""
    if value is None:
        return value

    try:
        coord = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{coord_type} must be a valid number")

    if not min_val <= coord <= max_val:
        raise ValueError(
            f"{coord_type} must be between {min_val} and {max_val}"
        )

    return coord


def validate_date_range(
    start_date: Optional[date], end_date: Optional[date]
) -> None:
    """Validate date range logic"""
    if start_date and end_date and start_date > end_date:
        raise ValueError("Start date must be before or equal to end date")


class ValidatedBulkTickersRequest(BaseModel):
    """Bulk tickers request with comprehensive validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    tickers: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Comma-separated ticker list (max 100 tickers)",
    )
    include_analytics: bool = True
    limit: int = Field(50, ge=1, le=100)

    @field_validator("tickers")
    @classmethod
    def validate_tickers_string(cls, v: str) -> str:
        """Parse and validate ticker list"""
        # Split and clean ticker list
        ticker_list = [t.strip().upper() for t in v.split(",") if t.strip()]

        if len(ticker_list) == 0:
            raise ValueError("At least one ticker must be provided")
        if len(ticker_list) > 100:
            raise ValueError("Maximum 100 tickers allowed")

        # Validate each ticker format
        validated_tickers = []
        for ticker in ticker_list:
            validated_ticker = validate_ticker_format(ticker)
            validated_tickers.append(validated_ticker)

        return ",".join(validated_tickers)


class ValidatedSearchRequest(BaseModel):
    """Search request with comprehensive validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    q: str = Field(..., min_length=1, max_length=50, description="Search query")
    limit: int = Field(20, ge=1, le=100)

    @field_validator("q")
    @classmethod
    def validate_search_query_field(cls, v: str) -> str:
        """Validate search query against SQL injection"""
        return validate_search_query(v)


class ValidatedSingleTickerRequest(BaseModel):
    """Single ticker request with validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    ticker: str = Field(..., min_length=1, max_length=10)
    include_analytics: bool = True

    @field_validator("ticker")
    @classmethod
    def validate_ticker_field(cls, v: str) -> str:
        """Validate single ticker format"""
        return validate_ticker_format(v)


class ValidatedPaginationRequest(BaseModel):
    """Pagination request with validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    limit: int = Field(1000, ge=1, le=10000)
    offset: int = Field(0, ge=0, le=1000000)
    has_location: bool = False


class ValidatedDateRangeRequest(BaseModel):
    """Date range request with comprehensive validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    start_date: Optional[date] = None
    end_date: Optional[date] = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_date_fields(cls, v):
        """Validate individual dates"""
        if v is None:
            return v

        # Handle string dates
        if isinstance(v, str):
            try:
                v = datetime.fromisoformat(v).date()
            except ValueError:
                raise ValueError("Date must be in ISO format (YYYY-MM-DD)")

        if not isinstance(v, date):
            raise ValueError("Date must be a valid date object")

        # Check date bounds
        if not MIN_DATE <= v <= MAX_DATE:
            raise ValueError(f"Date must be between {MIN_DATE} and {MAX_DATE}")

        return v

    @model_validator(mode="after")
    def validate_date_range_logic(self):
        """Validate date range logic"""
        validate_date_range(self.start_date, self.end_date)
        return self


# Geospatial validation models


class ValidatedCircleRequest(BaseModel):
    """Circle spatial query with comprehensive validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    latitude: float = Field(..., description="Center latitude (-90 to 90)")
    longitude: float = Field(..., description="Center longitude (-180 to 180)")
    radius_km: float = Field(
        ..., description="Radius in kilometers (0.1 to 1000)"
    )
    limit: int = Field(100, ge=1, le=1000)
    include_market_data: bool = False

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        """Validate latitude coordinate"""
        return validate_coordinate(v, "Latitude", -90.0, 90.0)

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        """Validate longitude coordinate"""
        return validate_coordinate(v, "Longitude", -180.0, 180.0)

    @field_validator("radius_km")
    @classmethod
    def validate_radius(cls, v: float) -> float:
        """Validate radius"""
        try:
            radius = float(v)
        except (TypeError, ValueError):
            raise ValueError("Radius must be a valid number")

        if radius < 0.1:
            raise ValueError("Radius must be at least 0.1 km")
        if radius > 1000:
            raise ValueError("Radius must be 1000 km or less")

        return radius


class ValidatedPolygonRequest(BaseModel):
    """Polygon spatial query with comprehensive validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    coordinates: List[List[float]] = Field(
        ...,
        min_length=3,
        max_length=100,
        description="Polygon coordinates [[lat, lng], [lat, lng], ...]",
    )
    limit: int = Field(1000, ge=1, le=1000)
    include_market_data: bool = False

    @field_validator("coordinates")
    @classmethod
    def validate_coordinates(cls, v: List[List[float]]) -> List[List[float]]:
        """Validate polygon coordinates"""
        if len(v) < 3:
            raise ValueError("Polygon must have at least 3 coordinates")
        if len(v) > 100:
            raise ValueError("Polygon cannot have more than 100 coordinates")

        validated_coords = []
        for i, coord in enumerate(v):
            if not isinstance(coord, (list, tuple)) or len(coord) != 2:
                raise ValueError(
                    f"Coordinate {i} must be [latitude, longitude]"
                )

            try:
                lat, lng = float(coord[0]), float(coord[1])
            except (TypeError, ValueError, IndexError):
                raise ValueError(f"Coordinate {i} must contain valid numbers")

            # Validate coordinate bounds
            lat = validate_coordinate(
                lat, f"Coordinate {i} latitude", -90.0, 90.0
            )
            lng = validate_coordinate(
                lng, f"Coordinate {i} longitude", -180.0, 180.0
            )

            validated_coords.append([lat, lng])

        return validated_coords


class ValidatedStateRequest(BaseModel):
    """State query with validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    state: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="US state code (e.g., 'CA', 'NY')",
    )
    limit: int = Field(100, ge=1, le=1000)
    include_market_data: bool = False

    @field_validator("state")
    @classmethod
    def validate_state_code(cls, v: str) -> str:
        """Validate US state code"""
        state = v.strip().upper()
        if not re.match(r"^[A-Z]{2}$", state):
            raise ValueError("State must be a 2-letter US state code")
        return state


class ValidatedNearbyRequest(BaseModel):
    """Nearby companies request with validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    ticker: str = Field(..., min_length=1, max_length=10)
    radius_km: float = Field(50.0, gt=0.1, le=1000)
    limit: int = Field(50, ge=1, le=500)

    @field_validator("ticker")
    @classmethod
    def validate_ticker_field(cls, v: str) -> str:
        """Validate ticker format"""
        return validate_ticker_format(v)


# Market data validation models


class ValidatedBulkMarketRequest(BaseModel):
    """Bulk market data request with comprehensive validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    tickers: List[str] = Field(..., min_length=1, max_length=50)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit_per_ticker: int = Field(100, ge=1, le=1000)
    include_location: bool = True

    @field_validator("tickers")
    @classmethod
    def validate_ticker_list(cls, v: List[str]) -> List[str]:
        """Validate list of tickers"""
        if len(v) > 50:
            raise ValueError("Maximum 50 tickers allowed")

        validated_tickers = []
        for ticker in v:
            validated_ticker = validate_ticker_format(ticker)
            validated_tickers.append(validated_ticker)

        return validated_tickers

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_date_fields(cls, v):
        """Validate date fields"""
        if v is None:
            return v

        if isinstance(v, str):
            try:
                v = datetime.fromisoformat(v).date()
            except ValueError:
                raise ValueError("Date must be in ISO format (YYYY-MM-DD)")

        if not isinstance(v, date):
            raise ValueError("Date must be a valid date object")

        if not MIN_DATE <= v <= MAX_DATE:
            raise ValueError(f"Date must be between {MIN_DATE} and {MAX_DATE}")

        return v

    @model_validator(mode="after")
    def validate_date_range_logic(self):
        """Validate date range logic"""
        validate_date_range(self.start_date, self.end_date)
        return self


class ValidatedTimeSeriesRequest(BaseModel):
    """Time series analysis request with validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    ticker: str = Field(..., min_length=1, max_length=10)
    period_days: int = Field(30, ge=7, le=365)
    analysis_type: str = Field(
        "volatility", pattern="^(volatility|trend|correlation)$"
    )

    @field_validator("ticker")
    @classmethod
    def validate_ticker_field(cls, v: str) -> str:
        """Validate ticker format"""
        return validate_ticker_format(v)


class ValidatedRegionStatsRequest(BaseModel):
    """Regional statistics request with validation"""

    model_config = ConfigDict(str_strip_whitespace=True)

    region_type: str = Field(..., pattern="^(circle|polygon|state)$")
    region_params: dict = Field(...)
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_date_fields(cls, v):
        """Validate date fields"""
        if v is None:
            return v

        if isinstance(v, str):
            try:
                v = datetime.fromisoformat(v).date()
            except ValueError:
                raise ValueError("Date must be in ISO format (YYYY-MM-DD)")

        if not isinstance(v, date):
            raise ValueError("Date must be a valid date object")

        if not MIN_DATE <= v <= MAX_DATE:
            raise ValueError(f"Date must be between {MIN_DATE} and {MAX_DATE}")

        return v

    @model_validator(mode="after")
    def validate_date_range_logic(self):
        """Validate date range logic"""
        validate_date_range(self.start_date, self.end_date)
        return self

    @field_validator("region_params")
    @classmethod
    def validate_region_params(cls, v: dict, info) -> dict:
        """Validate region parameters based on region type"""
        region_type = info.data.get("region_type")

        if region_type == "circle":
            required_fields = ["latitude", "longitude", "radius_km"]
            for field in required_fields:
                if field not in v:
                    raise ValueError(f"Circle region requires {field}")

            # Validate coordinates
            validate_coordinate(v["latitude"], "Latitude", -90.0, 90.0)
            validate_coordinate(v["longitude"], "Longitude", -180.0, 180.0)

            # Validate radius
            radius = float(v["radius_km"])
            if radius < 0.1 or radius > 1000:
                raise ValueError("Radius must be between 0.1 and 1000 km")

        elif region_type == "polygon":
            if "coordinates" not in v:
                raise ValueError("Polygon region requires coordinates")

            coords = v["coordinates"]
            if len(coords) < 3:
                raise ValueError("Polygon must have at least 3 coordinates")

        elif region_type == "state":
            if "state" not in v:
                raise ValueError("State region requires state code")

            state = str(v["state"]).strip().upper()
            if not re.match(r"^[A-Z]{2}$", state):
                raise ValueError("State must be a 2-letter US state code")
            v["state"] = state

        return v
