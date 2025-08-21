import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from app.core.async_database import async_questdb_manager
from app.core.cache_manager import cached
from shared.models import (
    CompletedMarketRecord,
    CompletedMarketRecordWithHistory,
    CompanyRecord,
)
from shared.config import settings
from app.core.rate_limit import limiter

router = APIRouter(prefix="/data", tags=["Core Data"])
logger = logging.getLogger(__name__)


def handle_endpoint_error(error: Exception, operation: str) -> HTTPException:
    """Handle endpoint errors with proper logging and sanitized responses"""
    logger.error(f"{operation} failed: {str(error)}")
    return HTTPException(status_code=500, detail="Internal server error")


def validate_ticker(ticker: str) -> str:
    """Validate and normalize a single ticker symbol"""
    if not ticker or not isinstance(ticker, str):
        raise HTTPException(
            status_code=400, detail="Ticker must be a non-empty string"
        )

    ticker = ticker.strip().upper()

    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    # Validate ticker format (1-10 alphanumeric characters)
    import re

    if not re.match(r"^[A-Z0-9]{1,10}$", ticker):
        raise HTTPException(
            status_code=400,
            detail="Invalid ticker format. Must be 1-10 alphanumeric characters",
        )

    return ticker


@router.get("/complete/bulk", response_model=List[CompletedMarketRecord])
@limiter.limit("30/minute")
@cached(ttl=120, key_prefix="bulk_complete")
async def get_bulk_complete_market_data(
    request: Request, 
    tickers: Optional[str] = Query(None, description="Comma-separated list of tickers"), 
    limit: int = Query(2000, ge=1, le=5000)
):
    """
    Get complete market data for multiple tickers.
    If no tickers are provided, fetches data for all companies with locations up to the limit.
    """
    import asyncio
    from app.models.validation import validate_ticker_format

    ticker_list = []
    if tickers:
        for t in tickers.split(",")[:limit]:
            try:
                validated_ticker = validate_ticker_format(t)
                ticker_list.append(validated_ticker)
            except ValueError as e:
                logger.warning(f"Skipping invalid ticker: {t} - {e}")
                continue
    else:
        # Fetch all companies with locations up to the limit
        all_companies_query = f"SELECT ticker FROM {settings.questdb_companies_table} WHERE latitude IS NOT NULL AND longitude IS NOT NULL LIMIT $1"
        all_companies = await async_questdb_manager.fetch_all(all_companies_query, limit)
        ticker_list = [row['ticker'] for row in all_companies]

    if not ticker_list:
        return []

    try:
        semaphore = asyncio.Semaphore(20)

        async def fetch_ticker_data(ticker: str):
            async with semaphore:
                try:
                    return await _fetch_complete_ticker_data(ticker)
                except Exception as e:
                    logger.warning(f"Failed to fetch data for {ticker}: {e}")
                    return None

        ticker_results = await asyncio.gather(*[fetch_ticker_data(ticker) for ticker in ticker_list])
        return [r for r in ticker_results if r is not None]

    except Exception as e:
        raise handle_endpoint_error(e, "Bulk complete market data query")


async def _fetch_complete_ticker_data(ticker: str) -> Optional[CompletedMarketRecord]:
    """Helper to fetch complete data for one ticker"""
    try:
        query = f"""
            WITH LatestData AS (
                SELECT
                    ts as latest_date,
                    open as latest_open,
                    high as latest_high,
                    low as latest_low,
                    close as latest_close,
                    volume as latest_volume
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1
                LATEST ON ts PARTITION BY ticker
            )
            SELECT 
                c.ticker,
                c.name as company_name,
                c.address,
                c.latitude,
                c.longitude,
                ld.latest_date,
                ld.latest_open,
                ld.latest_high,
                ld.latest_low,
                ld.latest_close,
                ld.latest_volume
            FROM {settings.questdb_companies_table} c
            LEFT JOIN LatestData ld ON 1=1
            WHERE c.ticker = $1
            LIMIT 1;
        """
        data = await async_questdb_manager.fetch_one(query, ticker)

        if not data or data["latest_date"] is None:
            return None

        full_data = {
            'price_change_24h': None,
            'price_change_pct_24h': None,
            'avg_volume_30d': None,
            'volatility_30d': None,
            **data
        }

        return CompletedMarketRecord(**full_data)
    except Exception as e:
        logger.error(f"Error fetching data for ticker {ticker}: {e}", exc_info=True)
        return None

      
@router.get("/complete/{ticker}", response_model=CompletedMarketRecord)
@limiter.limit("80/minute")
@cached(ttl=600, key_prefix="complete:")
async def get_complete_market_data(
    request: Request,
    ticker: str,
    include_analytics: bool = Query(
        True, description="Include 30-day analytics"
    ),
):
    """
    Get complete market data for a ticker matching original specification:
    'company ticker, company name, geocoded location, plaintext address,
    most recent OHLCV data with the possibility of getting more OHLCV data'
    """

    try:
        # Validate ticker input
        ticker_validated = validate_ticker(ticker)

        # Calculate 30-day window start in Python to avoid QuestDB interval syntax
        window_start = datetime.now().date() - timedelta(days=30)

        # Build secure query using parameterized approach
        query = f"""
            WITH latest_ohlcv AS (
                SELECT 
                    ticker, ts as date, open, high, low, close, volume
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1
                ORDER BY ts DESC
                LIMIT 1
            ),
            previous_day AS (
                SELECT 
                    ticker, close as prev_close
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $2 
                AND ts < (SELECT date FROM latest_ohlcv)
                ORDER BY ts DESC
                LIMIT 1
            ),
            analytics AS (
                SELECT 
                    ticker,
                    AVG(volume) as avg_volume_30d,
                    stddev_samp(close) as volatility_30d
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $3
                AND ts >= $4
                GROUP BY ticker
            )
            SELECT 
                c.ticker,
                c.name as company_name,
                c.address,
                c.latitude,
                c.longitude,
                CAST(l.date AS date) as latest_date,
                l.open as latest_open,
                l.high as latest_high,
                l.low as latest_low,
                l.close as latest_close,
                l.volume as latest_volume,
                CASE 
                    WHEN p.prev_close IS NOT NULL 
                    THEN l.close - p.prev_close 
                    ELSE NULL 
                END as price_change_24h,
                CASE 
                    WHEN p.prev_close IS NOT NULL AND p.prev_close > 0
                    THEN ((l.close - p.prev_close) / p.prev_close) * 100
                    ELSE NULL 
                END as price_change_pct_24h,
                CAST(a.avg_volume_30d AS int) as avg_volume_30d,
                a.volatility_30d
            FROM {settings.questdb_companies_table} c
            LEFT JOIN latest_ohlcv l ON c.ticker = l.ticker
            LEFT JOIN previous_day p ON c.ticker = p.ticker
            LEFT JOIN analytics a ON c.ticker = a.ticker
            WHERE c.ticker = $5
        """

        result = await async_questdb_manager.fetch_one(
            query,
            ticker_validated,
            ticker_validated,
            ticker_validated,
            window_start,
            ticker_validated,
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for ticker {ticker}",
            )

        if not result["latest_date"]:
            raise HTTPException(
                status_code=404,
                detail=f"No OHLCV data found for ticker {ticker}",
            )

        return CompletedMarketRecord(
            ticker=result["ticker"],
            company_name=result["company_name"],
            address=result["address"],
            latitude=result["latitude"],
            longitude=result["longitude"],
            latest_date=result["latest_date"],
            latest_open=result["latest_open"],
            latest_high=result["latest_high"],
            latest_low=result["latest_low"],
            latest_close=result["latest_close"],
            latest_volume=result["latest_volume"],
            price_change_24h=result["price_change_24h"],
            price_change_pct_24h=result["price_change_pct_24h"],
            avg_volume_30d=result["avg_volume_30d"],
            volatility_30d=result["volatility_30d"],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise handle_endpoint_error(e, "Complete market data query")


@router.get(
    "/complete/{ticker}/with-history",
    response_model=CompletedMarketRecordWithHistory,
)
@limiter.limit("40/minute")
async def get_complete_market_data_with_history(
    request: Request,
    ticker: str,
    days: int = Query(30, ge=1, le=365, description="Days of historical data"),
    include_analytics: bool = Query(
        True, description="Include 30-day analytics"
    ),
):
    """
    Get complete market data with historical OHLCV data
    """

    # Validate ticker first
    ticker_validated = validate_ticker(ticker)

    # Get the base complete record by calling the underlying logic directly
    # to avoid slowapi rate limiting issues
    try:
        # Calculate 30-day window start in Python to avoid QuestDB interval syntax
        window_start = datetime.now().date() - timedelta(days=30)

        # Build secure query using parameterized approach
        query = f"""
            WITH latest_ohlcv AS (
                SELECT 
                    ticker, ts as date, open, high, low, close, volume
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1
                ORDER BY ts DESC
                LIMIT 1
            ),
            previous_day AS (
                SELECT 
                    ticker, close as prev_close
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $2 
                AND ts < (SELECT date FROM latest_ohlcv)
                ORDER BY ts DESC
                LIMIT 1
            ),
            analytics AS (
                SELECT 
                    ticker,
                    AVG(volume) as avg_volume_30d,
                    stddev_samp(close) as volatility_30d
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $3
                AND ts >= $4
                GROUP BY ticker
            )
            SELECT 
                c.ticker,
                c.name as company_name,
                c.address,
                c.latitude,
                c.longitude,
                CAST(l.date AS date) as latest_date,
                l.open as latest_open,
                l.high as latest_high,
                l.low as latest_low,
                l.close as latest_close,
                l.volume as latest_volume,
                CASE 
                    WHEN p.prev_close IS NOT NULL 
                    THEN l.close - p.prev_close 
                    ELSE NULL 
                END as price_change_24h,
                CASE 
                    WHEN p.prev_close IS NOT NULL AND p.prev_close > 0
                    THEN ((l.close - p.prev_close) / p.prev_close) * 100
                    ELSE NULL 
                END as price_change_pct_24h,
                CAST(a.avg_volume_30d AS int) as avg_volume_30d,
                a.volatility_30d
            FROM {settings.questdb_companies_table} c
            LEFT JOIN latest_ohlcv l ON c.ticker = l.ticker
            LEFT JOIN previous_day p ON c.ticker = p.ticker
            LEFT JOIN analytics a ON c.ticker = a.ticker
            WHERE c.ticker = $5
        """

        result = await async_questdb_manager.fetch_one(
            query,
            ticker_validated,
            ticker_validated,
            ticker_validated,
            window_start,
            ticker_validated,
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for ticker {ticker}",
            )

        if not result["latest_date"]:
            raise HTTPException(
                status_code=404,
                detail=f"No OHLCV data found for ticker {ticker}",
            )

        complete_record = CompletedMarketRecord(
            ticker=result["ticker"],
            company_name=result["company_name"],
            address=result["address"],
            latitude=result["latitude"],
            longitude=result["longitude"],
            latest_date=result["latest_date"],
            latest_open=result["latest_open"],
            latest_high=result["latest_high"],
            latest_low=result["latest_low"],
            latest_close=result["latest_close"],
            latest_volume=result["latest_volume"],
            price_change_24h=result["price_change_24h"],
            price_change_pct_24h=result["price_change_pct_24h"],
            avg_volume_30d=result["avg_volume_30d"],
            volatility_30d=result["volatility_30d"],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise handle_endpoint_error(e, "Complete market data query")

    try:
        # Get historical OHLCV data
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)

        # Build secure parameterized query
        history_query = f"""
            SELECT ts as "Date", open as "Open", high as "High",
                   low as "Low", close as "Close", volume as "Volume"
            FROM {settings.questdb_ohlcv_table}
            WHERE ticker = $1 AND ts BETWEEN $2 AND $3
            ORDER BY ts DESC
            LIMIT 1000
        """

        history_results = await async_questdb_manager.fetch_all(
            history_query, ticker_validated, start_date, end_date
        )

        # Convert to response record format
        from shared.models import OHLCVResponseRecord

        historical_data = [
            OHLCVResponseRecord(
                Date=row["Date"],
                Open=row["Open"],
                High=row["High"],
                Low=row["Low"],
                Close=row["Close"],
                Volume=row["Volume"],
            )
            for row in history_results
        ]

        # Combine with complete record
        return CompletedMarketRecordWithHistory(
            ticker=complete_record.ticker,
            company_name=complete_record.company_name,
            address=complete_record.address,
            latitude=complete_record.latitude,
            longitude=complete_record.longitude,
            latest_date=complete_record.latest_date,
            latest_open=complete_record.latest_open,
            latest_high=complete_record.latest_high,
            latest_low=complete_record.latest_low,
            latest_close=complete_record.latest_close,
            latest_volume=complete_record.latest_volume,
            price_change_24h=complete_record.price_change_24h,
            price_change_pct_24h=complete_record.price_change_pct_24h,
            avg_volume_30d=complete_record.avg_volume_30d,
            volatility_30d=complete_record.volatility_30d,
            historical_data=historical_data,
        )

    except Exception as e:
        raise handle_endpoint_error(e, "Historical data query")


@router.get("/companies", response_model=List[CompanyRecord])
@limiter.limit("50/minute")
async def get_all_companies(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    has_location: bool = False,
):
    """
    Get list of all companies with basic information
    """

    try:
        # Build secure query with pagination
        where_clause = ""

        if has_location:
            where_clause = (
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
            )

        query = f"""
            WITH ranked AS (
                SELECT 
                    ticker, name, address, latitude, longitude,
                    ROW_NUMBER() OVER (ORDER BY ticker) as rn
                FROM {settings.questdb_companies_table}
                {where_clause}
            )
            SELECT ticker, name, address, latitude, longitude
            FROM ranked
            WHERE rn > $1 AND rn <= $2
            ORDER BY ticker
        """

        results = await async_questdb_manager.fetch_all(
            query, offset, offset + limit
        )
        return [
            CompanyRecord(
                ticker=row["ticker"],
                name=row["name"],
                address=row["address"],
                latitude=row["latitude"],
                longitude=row["longitude"],
            )
            for row in results
        ]

    except Exception as e:
        raise handle_endpoint_error(e, "Companies query")


@router.get("/search", response_model=List[CompletedMarketRecord])
@limiter.limit("60/minute")
async def search_companies(request: Request, q: str, limit: int = 20):
    """
    Search companies by name or ticker symbol
    """

    try:
        # Validate search query against SQL injection patterns
        from app.models.validation import validate_search_query

        validated_q = validate_search_query(q)

        # First find matching companies (avoid scanning entire OHLCV)
        search_pattern = f"%{validated_q}%"
        q_upper = validated_q.upper()
        q_prefix = f"{q_upper}%"

        companies_query = f"""
            SELECT ticker, name, address, latitude, longitude
            FROM {settings.questdb_companies_table}
            WHERE ticker ILIKE $1 OR name ILIKE $2
            ORDER BY 
                CASE WHEN ticker = $3 THEN 0 ELSE 1 END,
                CASE WHEN ticker ILIKE $4 THEN 0 ELSE 1 END,
                LENGTH(name),
                ticker
            LIMIT $5
        """

        companies = await async_questdb_manager.fetch_all(
            companies_query,
            search_pattern,
            search_pattern,
            q_upper,
            q_prefix,
            limit,
        )

        if not companies:
            return []

# Get tickers for market data fetching

        # Get real market data for each company instead of mock data
        results = []
        for company in companies:
            # Get latest market data for this company
            ticker = company["ticker"]

            # Get most recent OHLCV data
            latest_query = f"""
                SELECT ts as latest_date, open as latest_open, high as latest_high, 
                       low as latest_low, close as latest_close, volume as latest_volume
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1
                ORDER BY ts DESC
                LIMIT 1
            """
            latest_data = await async_questdb_manager.fetch_one(
                latest_query, ticker
            )

            # Calculate 30-day analytics
            window_start = datetime.now().date() - timedelta(days=30)
            analytics_query = f"""
                SELECT AVG(volume) as avg_volume_30d, stddev_samp(close) as volatility_30d
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1 AND ts >= $2
            """
            analytics_data = await async_questdb_manager.fetch_one(
                analytics_query, ticker, window_start
            )

            # Initialize change calculations
            price_change_24h = None
            price_change_pct_24h = None

            # Get previous day close for change calculation
            if latest_data:
                prev_query = f"""
                    SELECT close as prev_close
                    FROM {settings.questdb_ohlcv_table}
                    WHERE ticker = $1 AND ts < $2
                    ORDER BY ts DESC
                    LIMIT 1
                """
                prev_data = await async_questdb_manager.fetch_one(
                    prev_query, ticker, latest_data["latest_date"]
                )

                # Calculate changes
                if prev_data and prev_data["prev_close"]:
                    price_change_24h = (
                        latest_data["latest_close"] - prev_data["prev_close"]
                    )
                    price_change_pct_24h = (
                        price_change_24h / prev_data["prev_close"]
                    ) * 100

            # Build result with real data
            results.append(
                {
                    "ticker": company["ticker"],
                    "company_name": company["name"],
                    "address": company["address"],
                    "latitude": company["latitude"],
                    "longitude": company["longitude"],
                    "latest_date": latest_data["latest_date"]
                    if latest_data
                    else None,
                    "latest_open": float(latest_data["latest_open"])
                    if latest_data and latest_data["latest_open"]
                    else 0.0,
                    "latest_high": float(latest_data["latest_high"])
                    if latest_data and latest_data["latest_high"]
                    else 0.0,
                    "latest_low": float(latest_data["latest_low"])
                    if latest_data and latest_data["latest_low"]
                    else 0.0,
                    "latest_close": float(latest_data["latest_close"])
                    if latest_data and latest_data["latest_close"]
                    else 0.0,
                    "latest_volume": int(latest_data["latest_volume"])
                    if latest_data and latest_data["latest_volume"]
                    else 0,
                    "price_change_24h": price_change_24h,
                    "price_change_pct_24h": price_change_pct_24h,
                    "avg_volume_30d": int(analytics_data["avg_volume_30d"])
                    if analytics_data and analytics_data["avg_volume_30d"]
                    else None,
                    "volatility_30d": float(analytics_data["volatility_30d"])
                    if analytics_data and analytics_data["volatility_30d"]
                    else None,
                }
            )
        return [
            CompletedMarketRecord(
                ticker=row["ticker"],
                company_name=row["company_name"],
                address=row["address"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                latest_date=row["latest_date"],
                latest_open=row["latest_open"],
                latest_high=row["latest_high"],
                latest_low=row["latest_low"],
                latest_close=row["latest_close"],
                latest_volume=row["latest_volume"],
                price_change_24h=row["price_change_24h"],
                price_change_pct_24h=row["price_change_pct_24h"],
                avg_volume_30d=row["avg_volume_30d"],
                volatility_30d=row["volatility_30d"],
            )
            for row in results
        ]

    except Exception as e:
        raise handle_endpoint_error(e, "Company search query")


@router.get("/stats/summary")
@limiter.limit("100/minute")
@cached(ttl=1800, key_prefix="stats:")
async def get_data_summary(request: Request):
    """
    Get summary statistics about the available data
    """

    try:
        # Company counts
        company_stats = await async_questdb_manager.fetch_one(
            f"SELECT COUNT(*) as total_companies, COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as geocoded_companies FROM {settings.questdb_companies_table}"
        )

        # OHLCV counts using latest-by aggregation
        with_data = await async_questdb_manager.fetch_one(
            f"SELECT COUNT(*) as companies_with_data FROM (SELECT ticker FROM {settings.questdb_ohlcv_table} LATEST BY ticker)"
        )

        ohlcv_stats = await async_questdb_manager.fetch_one(
            f"SELECT COUNT(*) as total_ohlcv_records, CAST(MIN(ts) AS date) as earliest_date, CAST(MAX(ts) AS date) as latest_date FROM {settings.questdb_ohlcv_table}"
        )

        summary = {
            "total_companies": int(company_stats["total_companies"] or 0),
            "geocoded_companies": int(company_stats["geocoded_companies"] or 0),
            "companies_with_data": int(with_data["companies_with_data"] or 0),
            "total_ohlcv_records": int(ohlcv_stats["total_ohlcv_records"] or 0),
            "earliest_date": ohlcv_stats["earliest_date"],
            "latest_date": ohlcv_stats["latest_date"],
        }

        total_companies = max(summary["total_companies"], 1)
        return {
            "summary": summary,
            "timestamp": datetime.now().isoformat(),
            "data_completeness": {
                "companies_with_locations": f"{(summary['geocoded_companies'] / total_companies) * 100:.1f}%",
                "companies_with_market_data": f"{(summary['companies_with_data'] / total_companies) * 100:.1f}%",
            },
        }

    except Exception as e:
        raise handle_endpoint_error(e, "Data summary query")
