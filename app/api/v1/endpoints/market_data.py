import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from app.core.async_database import async_questdb_manager
from app.core.cache_manager import cached
from app.models.validation import validate_ticker_format, ValidatedBulkMarketRequest
from shared.models import OHLCVResponseRecord
from shared.config import settings
from app.core.rate_limit import limiter

router = APIRouter(prefix="/market-data", tags=["Market Data"])
logger = logging.getLogger(__name__)


def handle_endpoint_error(error: Exception, operation: str) -> HTTPException:
    """Handle endpoint errors with proper logging and sanitized responses"""
    logger.error(f"{operation} failed: {str(error)}")
    return HTTPException(status_code=500, detail="Internal server error")


@router.get("/ohlcv/{ticker}", response_model=List[OHLCVResponseRecord])
@limiter.limit("60/minute")
@cached(ttl=900, key_prefix="ohlcv:")
async def get_ticker_ohlcv(
    request: Request,
    ticker: str,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
):
    """Get OHLCV data for a ticker."""
    try:
        ticker_upper = validate_ticker_format(ticker)
        params = [ticker_upper]
        
        date_conditions = []
        if start_date:
            params.append(start_date)
            date_conditions.append(f"ts >= ${len(params)}")
        if end_date:
            params.append(end_date)
            date_conditions.append(f"ts <= ${len(params)}")

        date_filter = f"AND {' AND '.join(date_conditions)}" if date_conditions else ""
        
        params.append(limit)
        limit_param_index = len(params)

        query = f"""
            SELECT ts as "Date", open as "Open", high as "High", 
                   low as "Low", close as "Close", volume as "Volume"
            FROM {settings.questdb_ohlcv_table}
            WHERE ticker = $1 {date_filter}
            ORDER BY ts DESC
            LIMIT ${limit_param_index}
        """

        results = await async_questdb_manager.fetch_all(query, *params)
        if not results:
            raise HTTPException(status_code=404, detail=f"No data found for ticker {ticker}")

        return [OHLCVResponseRecord(**row) for row in results]
    except HTTPException:
        raise
    except Exception as e:
        raise handle_endpoint_error(e, "OHLCV data query")


@router.get("/market-overview")
@limiter.limit("20/minute")
@cached(ttl=600, key_prefix="market_overview")
async def get_market_overview(request: Request):
    """Get market overview with geospatial distribution"""
    try:
        query = f"""
            WITH LatestOHLCV AS (
                SELECT 
                    ticker,
                    ts,
                    close,
                    volume
                FROM '{settings.questdb_ohlcv_table}'
                LATEST ON ts PARTITION BY ticker
            )
            SELECT
                c.state,
                count(*) AS company_count,
                avg(lo.close) AS avg_price,
                sum(lo.volume) AS total_volume
            FROM '{settings.questdb_companies_table}' c
            JOIN LatestOHLCV lo ON c.ticker = lo.ticker
            WHERE c.state IS NOT NULL
            GROUP BY c.state
            ORDER BY company_count DESC;
        """
        
        geo_distribution_data = await async_questdb_manager.fetch_all(query)

        if not geo_distribution_data:
            raise HTTPException(status_code=404, detail="No market data available to generate overview.")

        total_companies = sum(int(row['company_count']) for row in geo_distribution_data)
        total_volume = sum(int(row['total_volume']) for row in geo_distribution_data)
        
        weighted_price_sum = sum(float(row['avg_price']) * int(row['company_count']) for row in geo_distribution_data if row['avg_price'] is not None)
        market_avg_price = weighted_price_sum / total_companies if total_companies > 0 else 0

        market_summary = {
            "total_companies": total_companies,
            "avg_price": market_avg_price,
            "total_volume": total_volume,
        }

        overview = {
            "market_summary": market_summary,
            "geographical_distribution": geo_distribution_data,
            "timestamp": date.today().isoformat(),
        }

        return overview

    except Exception as e:
        raise handle_endpoint_error(e, "Market overview query")


@router.get("/latest/{ticker}", response_model=OHLCVResponseRecord)
@limiter.limit("120/minute")
@cached(ttl=300, key_prefix="latest:")
async def get_latest_ohlcv(request: Request, ticker: str):
    """Get latest OHLCV data with Redis caching"""

    # Try Redis cache first
    ticker_upper = validate_ticker_format(ticker)
    cache_key = f"latest:{ticker_upper}"
    cached_data = await async_redis_manager.cache_get(cache_key)

    if cached_data:
        import json

        data = json.loads(cached_data)
        return OHLCVResponseRecord(
            Date=data["Date"],
            Open=data["Open"],
            High=data["High"],
            Low=data["Low"],
            Close=data["Close"],
            Volume=data["Volume"],
        )

    try:
        # QuestDB latest value query with time-series optimization
        query = f"""
            SELECT ts as "Date", open as "Open", high as "High",
                   low as "Low", close as "Close", volume as "Volume"
            FROM {settings.questdb_ohlcv_table}
            WHERE ticker = $1
            ORDER BY ts DESC
            LIMIT 1
        """

        result = await async_questdb_manager.fetch_one(query, ticker_upper)

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for ticker {ticker}",
            )

        response = OHLCVResponseRecord(
            Date=result["Date"],
            Open=float(result["Open"]),
            High=float(result["High"]),
            Low=float(result["Low"]),
            Close=float(result["Close"]),
            Volume=int(result["Volume"]),
        )

        # Cache the result
        import json

        await async_redis_manager.cache_set(
            cache_key, json.dumps(response.model_dump(), default=str), ttl=300
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise handle_endpoint_error(e, "OHLCV data query")


@router.post("/bulk-with-location")
@limiter.limit("20/minute")
async def get_bulk_ohlcv_with_location(
    request: Request, bulk_request: ValidatedBulkMarketRequest
):
    """Get bulk OHLCV data with company location information"""

    if len(bulk_request.tickers) > 50:
        raise HTTPException(
            status_code=400, detail="Maximum 50 tickers per request"
        )

    try:
        tickers_upper = bulk_request.tickers

        # Build date conditions
        date_conditions = []
        params = []

        if bulk_request.start_date:
            date_conditions.append("o.ts >= $1")
            params.append(bulk_request.start_date)
        if bulk_request.end_date:
            param_num = len(params) + 1
            date_conditions.append(f"o.ts <= ${param_num}")
            params.append(bulk_request.end_date)

        date_filter = (
            " AND " + " AND ".join(date_conditions) if date_conditions else ""
        )

        results = []

        for ticker in tickers_upper:
            ticker_params = list(params)
            ticker_params.append(ticker)

            if bulk_request.include_location:
                query = f"""
                    SELECT 
                        o.ts as "Date",
                        o.ticker,
                        o.open, o.high, o.low, o.close, o.volume,
                        c.name, c.latitude, c.longitude,
                        ROW_NUMBER() OVER (PARTITION BY o.ticker ORDER BY o.ts DESC) as rn
                    FROM {settings.questdb_ohlcv_table} o
                    LEFT JOIN {settings.questdb_companies_table} c ON o.ticker = c.ticker
                    WHERE o.ticker = ${len(ticker_params)}{date_filter}
                """

                # Wrap in subquery to apply limit per ticker
                final_query = f"""
                    SELECT * FROM ({query}) ranked
                    WHERE rn <= ${len(ticker_params) + 1}
                    ORDER BY "Date" DESC
                """
                ticker_params.append(bulk_request.limit_per_ticker)
            else:
                # Just OHLCV data
                query = f"""
                    SELECT 
                        o.ts as "Date",
                        o.ticker, o.open, o.high, o.low, o.close, o.volume,
                        ROW_NUMBER() OVER (PARTITION BY o.ticker ORDER BY o.ts DESC) as rn
                    FROM {settings.questdb_ohlcv_table} o
                    WHERE o.ticker = ${len(ticker_params)}{date_filter}
                """

                final_query = f"""
                    SELECT * FROM ({query}) ranked
                    WHERE rn <= ${len(ticker_params) + 1}
                    ORDER BY "Date" DESC
                """
                ticker_params.append(bulk_request.limit_per_ticker)

            ticker_results = await async_questdb_manager.fetch_all(
                final_query, *ticker_params
            )
            results.extend(ticker_results)

        # Group by ticker
        grouped_data = {}
        for row in results:
            ticker = row["ticker"]
            if ticker not in grouped_data:
                grouped_data[ticker] = []

            data_point = {
                "Date": row["Date"],
                "Open": float(row["open"]),
                "High": float(row["high"]),
                "Low": float(row["low"]),
                "Close": float(row["close"]),
                "Volume": int(row["volume"]),
            }

            if bulk_request.include_location and "latitude" in row:
                data_point.update(
                    {
                        "name": row["name"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
                    }
                )

            grouped_data[ticker].append(data_point)

        return grouped_data

    except Exception as e:
        raise handle_endpoint_error(e, "Bulk market data query")


@router.get("/time-series-analysis/{ticker}")
@limiter.limit("30/minute")
async def get_time_series_analysis(
    request: Request,
    ticker: str,
    period_days: int = 30,
    analysis_type: str = "volatility",
):
    """Advanced time-series analysis using QuestDB analytical functions"""

    try:
        # Validate and use request data
        ticker_upper = ticker.strip().upper()
        if not re.match(r"^[A-Z0-9]{1,10}$", ticker_upper):
            raise HTTPException(status_code=400, detail="Invalid ticker format")

        if period_days < 7 or period_days > 365:
            raise HTTPException(
                status_code=400, detail="Period must be between 7 and 365 days"
            )

        if analysis_type not in ["volatility", "trend", "correlation"]:
            raise HTTPException(status_code=400, detail="Invalid analysis type")

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=period_days)

        # Use AsyncPG with parameterized query
        query = f"""
            SELECT ts as date, close
            FROM {settings.questdb_ohlcv_table}
            WHERE ticker = $1 AND ts BETWEEN $2 AND $3
            ORDER BY ts
        """

        results = await async_questdb_manager.fetch_all(
            query, ticker_upper, start_date, end_date
        )

        return {
            "ticker": ticker_upper,
            "analysis_type": analysis_type,
            "period_days": period_days,
            "data": [dict(row) for row in results],
        }

    except Exception as e:
        raise handle_endpoint_error(e, "Time-series analysis")
