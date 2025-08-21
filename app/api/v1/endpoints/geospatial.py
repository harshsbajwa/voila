"""
Geospatial API endpoints using Redis GEORADIUS and QuestDB time-series data
"""

import re
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException, Request

from app.core.async_database import (
    async_questdb_manager,
    async_redis_manager,
)
from app.core.cache_manager import cached
from app.core.spatial_utils import (
    calculate_polygon_bounds,
    expand_bounding_box,
    filter_companies_by_polygon,
)
from app.models.validation import (
    ValidatedCircleRequest,
    ValidatedPolygonRequest,
    ValidatedNearbyRequest,
    ValidatedRegionStatsRequest,
)
from app.models.geospatial import (
    SpatialQueryResponse,
    CompanyLocation,
    RegionStatsResponse,
    RegionalStats,
)
from shared.config import settings
from app.core.rate_limit import limiter

router = APIRouter(prefix="/spatial", tags=["Geospatial Queries"])
logger = logging.getLogger(__name__)


def handle_endpoint_error(error: Exception, operation: str) -> HTTPException:
    """Handle endpoint errors with proper logging and sanitized responses"""
    logger.error(f"{operation} failed: {str(error)}")
    return HTTPException(status_code=500, detail="Internal server error")


@router.post("/within-circle", response_model=SpatialQueryResponse)
@limiter.limit("40/minute")
@cached(ttl=1200, key_prefix="spatial:circle:")
async def get_companies_within_circle(
    request: Request, circle_request: ValidatedCircleRequest
):
    """Get companies within a circular area"""
    start_time = time.time()

    try:
        # Use Redis GEORADIUS for fast spatial query
        companies_data = await async_redis_manager.get_nearby_companies(
            lat=circle_request.latitude,
            lng=circle_request.longitude,
            radius_km=circle_request.radius_km,
            limit=circle_request.limit,
        )

        companies = [
            CompanyLocation(
                ticker=comp["ticker"],
                name=comp["name"],
                address=comp["address"],
                latitude=comp["latitude"],
                longitude=comp["longitude"],
                distance_km=comp["distance_km"],
            )
            for comp in companies_data
        ]

        # Optionally include market data
        if circle_request.include_market_data:
            companies = await _enrich_with_market_data(companies)

        execution_time = (time.time() - start_time) * 1000

        return SpatialQueryResponse(
            companies=companies,
            total_found=len(companies),
            query_params={
                "center": {
                    "latitude": circle_request.latitude,
                    "longitude": circle_request.longitude,
                },
                "radius_km": circle_request.radius_km,
                "limit": circle_request.limit,
            },
            execution_time_ms=execution_time,
        )

    except Exception as e:
        raise handle_endpoint_error(e, "Spatial circle query")


@router.post("/within-polygon", response_model=SpatialQueryResponse)
@limiter.limit("20/minute")
@cached(ttl=1800, key_prefix="spatial:polygon:")
async def get_companies_within_polygon(
    request: Request, polygon_request: ValidatedPolygonRequest
):
    """Get companies within a polygon area using precise point-in-polygon testing"""
    start_time = time.time()

    try:
        # Convert validated coordinates to tuples for spatial utils
        polygon_coords = [
            (coord[0], coord[1]) for coord in polygon_request.coordinates
        ]

        # Calculate bounding box with buffer for initial filtering
        min_lat, min_lng, max_lat, max_lng = calculate_polygon_bounds(
            polygon_coords
        )
        expanded_bounds = expand_bounding_box(
            min_lat, min_lng, max_lat, max_lng, buffer_km=5.0
        )

        # Use expanded bounding box to get candidate companies from Redis
        center_lat = (expanded_bounds[0] + expanded_bounds[2]) / 2
        center_lng = (expanded_bounds[1] + expanded_bounds[3]) / 2

        # Estimate radius to cover expanded bounding box
        lat_diff = expanded_bounds[2] - expanded_bounds[0]
        lng_diff = expanded_bounds[3] - expanded_bounds[1]
        radius_km = max(lat_diff, lng_diff) * 111 / 2  # Rough km per degree

        # Get candidate companies from Redis
        candidates = await async_redis_manager.get_nearby_companies(
            center_lat,
            center_lng,
            radius_km,
            limit=polygon_request.limit * 3,  # Get more candidates
        )

        # Apply precise point-in-polygon filtering
        companies_in_polygon = filter_companies_by_polygon(
            candidates, polygon_coords
        )

        # Limit results
        companies_in_polygon = companies_in_polygon[: polygon_request.limit]

        # Convert to response format
        companies = [
            CompanyLocation(
                ticker=comp["ticker"],
                name=comp["name"],
                address=comp["address"],
                latitude=comp["latitude"],
                longitude=comp["longitude"],
                distance_km=comp.get("distance_km", 0.0),
            )
            for comp in companies_in_polygon
        ]

        if polygon_request.include_market_data:
            companies = await _enrich_with_market_data(companies)

        execution_time = (time.time() - start_time) * 1000

        return SpatialQueryResponse(
            companies=companies,
            total_found=len(companies),
            query_params={
                "coordinates": polygon_request.coordinates,
                "limit": polygon_request.limit,
                "algorithm": "precise_point_in_polygon",
            },
            execution_time_ms=execution_time,
        )

    except Exception as e:
        raise handle_endpoint_error(e, "Spatial polygon query")


@router.get("/by-state/{state}", response_model=SpatialQueryResponse)
@limiter.limit("60/minute")
@cached(ttl=3600, key_prefix="spatial:state:")
async def get_companies_by_state(
    request: Request,
    state: str,
    limit: int = 100,
    include_market_data: bool = False,
):
    """Get companies by US state"""
    start_time = time.time()

    try:
        # Use AsyncPG with parameterized query
        query = f"""
            SELECT ticker, name, address, latitude, longitude
            FROM {settings.questdb_companies_table} 
            WHERE state = $1 AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY ticker
            LIMIT $2
        """
        # Validate state format
        state_clean = state.strip().upper()
        if not re.match(r"^[A-Z]{2}$", state_clean):
            raise HTTPException(status_code=400, detail="Invalid state format")

        results = await async_questdb_manager.fetch_all(
            query, state_clean, limit
        )

        companies = [
            CompanyLocation(
                ticker=row["ticker"],
                name=row["name"],
                address=row["address"],
                latitude=row["latitude"],
                longitude=row["longitude"],
            )
            for row in results
        ]

        if include_market_data:
            companies = await _enrich_with_market_data(companies)

        execution_time = (time.time() - start_time) * 1000

        return SpatialQueryResponse(
            companies=companies,
            total_found=len(companies),
            query_params={
                "state": state_clean,
                "limit": limit,
            },
            execution_time_ms=execution_time,
        )

    except Exception as e:
        raise handle_endpoint_error(e, "Spatial state query")


@router.post("/nearby-ticker", response_model=List[CompanyLocation])
@limiter.limit("50/minute")
async def get_companies_near_ticker(
    request: Request, nearby_request: ValidatedNearbyRequest
):
    """Get companies near a specific ticker's location"""
    target_lat = None
    target_lng = None
    ticker = nearby_request.ticker.upper()

    # Try Redis metadata first
    try:
        company_metadata = await async_redis_manager.get_company_metadata(
            ticker
        )
        if (
            company_metadata
            and "latitude" in company_metadata
            and "longitude" in company_metadata
        ):
            try:
                target_lat = float(company_metadata["latitude"])
                target_lng = float(company_metadata["longitude"])
                logger.info(
                    f"Found {ticker} location in Redis: ({target_lat}, {target_lng})"
                )
            except (ValueError, TypeError):
                pass
    except Exception as e:
        logger.info(
            f"Redis metadata lookup failed for {ticker}, trying QuestDB: {e}"
        )

    # Fallback to QuestDB if Redis doesn't have valid coordinates
    if target_lat is None or target_lng is None:
        try:
            query = f"""
                SELECT latitude, longitude
                FROM {settings.questdb_companies_table}
                WHERE ticker = $1 AND latitude IS NOT NULL AND longitude IS NOT NULL
                LIMIT 1
            """
            result = await async_questdb_manager.fetch_one(query, ticker)

            if result and result["latitude"] and result["longitude"]:
                target_lat = float(result["latitude"])
                target_lng = float(result["longitude"])
                logger.info(
                    f"Found {ticker} location in QuestDB: ({target_lat}, {target_lng})"
                )
            else:
                # Ticker exists but has no location
                raise HTTPException(
                    status_code=404,
                    detail=f"Location data not found for ticker {ticker}",
                )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"QuestDB lookup failed for {ticker}: {e}")
            raise HTTPException(
                status_code=404, detail=f"Ticker {ticker} not found"
            )

    # Get nearby companies
    try:
        nearby_data = await async_redis_manager.get_nearby_companies(
            lat=target_lat,
            lng=target_lng,
            radius_km=nearby_request.radius_km,
            limit=nearby_request.limit
            + 1,  # +1 to account for the target company
        )

        # Filter out the target company
        nearby_companies = [
            CompanyLocation(
                ticker=comp["ticker"],
                name=comp["name"],
                address=comp["address"],
                latitude=comp["latitude"],
                longitude=comp["longitude"],
                distance_km=comp["distance_km"],
            )
            for comp in nearby_data
            if comp["ticker"] != ticker
        ][: nearby_request.limit]

        return nearby_companies

    except Exception as e:
        logger.error(f"Failed to get nearby companies for {ticker}: {e}")
        # Return empty list if we have coordinates but can't find nearby companies
        return []


@router.post("/regional-stats", response_model=RegionStatsResponse)
@limiter.limit("30/minute")
async def get_regional_market_stats(
    request: Request, stats_request: ValidatedRegionStatsRequest
):
    """Calculate market statistics for a geographic region"""
    start_time = time.time()

    try:
        # Normalize period values for response shape
        period_start = stats_request.start_date or datetime.now().date()
        period_end = stats_request.end_date or datetime.now().date()

        # Get companies in the region based on region_type
        if stats_request.region_type == "circle":
            companies = await _get_companies_in_circle(
                stats_request.region_params
            )
        elif stats_request.region_type == "polygon":
            companies = await _get_companies_in_polygon(
                stats_request.region_params
            )
        elif stats_request.region_type == "state":
            companies = await _get_companies_in_state(
                stats_request.region_params
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid region_type")

        if not companies:
            raise HTTPException(
                status_code=404,
                detail="No companies found in the specified region",
            )

        # Calculate market statistics using QuestDB time-series data
        try:
            stats = await _calculate_regional_stats(companies, stats_request)
        except Exception:
            stats = RegionalStats(
                region_description=f"Region with {len(companies)} companies",
                company_count=len(companies),
                avg_price=None,
                median_price=None,
                total_volume=None,
                volatility=None,
                price_range=None,
            )

        execution_time = (time.time() - start_time) * 1000

        return RegionStatsResponse(
            stats=stats,
            period={
                "start_date": period_start,
                "end_date": period_end,
            },
            execution_time_ms=execution_time,
        )

    except HTTPException as e:
        if e.status_code == 404:
            raise
        # For other errors, return empty stats
        period_start = stats_request.start_date or datetime.now().date()
        period_end = stats_request.end_date or datetime.now().date()
        return RegionStatsResponse(
            stats=RegionalStats(
                region_description="Region with 0 companies",
                company_count=0,
                avg_price=None,
                median_price=None,
                total_volume=None,
                volatility=None,
                price_range=None,
            ),
            period={
                "start_date": period_start,
                "end_date": period_end,
            },
            execution_time_ms=0.0,
        )
    except Exception:
        # Return a minimal response on backend limitations
        return RegionStatsResponse(
            stats=RegionalStats(
                region_description="Region with 0 companies",
                company_count=0,
                avg_price=None,
                median_price=None,
                total_volume=None,
                volatility=None,
                price_range=None,
            ),
            period={
                "start_date": period_start,
                "end_date": period_end,
            },
            execution_time_ms=0.0,
        )


async def _enrich_with_market_data(
    companies: List[CompanyLocation],
) -> List[CompanyLocation]:
    """Enrich company data with latest market data from QuestDB"""
    if not companies:
        return companies

    try:
        # Get latest market data for all companies using AsyncPG
        tickers = [comp.ticker for comp in companies]

        # Use individual queries for QuestDB compatibility
        # Tickers are already validated by the CompanyLocation objects
        results = []
        for ticker in tickers:
            query = f"""
                SELECT ticker, close, volume, ts
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1
                ORDER BY ts DESC
                LIMIT 1
            """
            ticker_result = await async_questdb_manager.fetch_one(query, ticker)
            if ticker_result:
                results.append(ticker_result)
        market_data = {row["ticker"]: row for row in results}

        # Create new enriched company objects with market data as additional attributes
        enriched_companies = []
        for company in companies:
            company_dict = company.model_dump()

            # Add market data if available
            if company.ticker in market_data:
                market_info = market_data[company.ticker]
                company_dict.update(
                    {
                        "latest_price": float(market_info["close"])
                        if market_info["close"]
                        else None,
                        "latest_volume": int(market_info["volume"])
                        if market_info["volume"]
                        else None,
                        "last_updated": market_info["ts"],
                    }
                )
            else:
                company_dict.update(
                    {
                        "latest_price": None,
                        "latest_volume": None,
                        "last_updated": None,
                    }
                )

            # Create new CompanyLocation with additional fields
            enriched_companies.append(
                CompanyLocation(
                    ticker=company_dict["ticker"],
                    name=company_dict["name"],
                    address=company_dict.get("address"),
                    latitude=company_dict["latitude"],
                    longitude=company_dict["longitude"],
                    distance_km=company_dict.get("distance_km"),
                    latest_price=company_dict.get("latest_price"),
                    latest_volume=company_dict.get("latest_volume"),
                    last_updated=company_dict.get("last_updated"),
                )
            )

        return enriched_companies

    except Exception as e:
        # If market data enrichment fails, return companies without market data
        # Log the error but don't fail the entire request
        logger.warning(f"Failed to enrich companies with market data: {e}")
        return companies


async def _get_companies_in_circle(params: Dict[str, Any]) -> List[str]:
    """Get company tickers within a circle"""
    companies_data = await async_redis_manager.get_nearby_companies(
        lat=params["latitude"],
        lng=params["longitude"],
        radius_km=params["radius_km"],
        limit=1000,
    )
    return [comp["ticker"] for comp in companies_data]


async def _get_companies_in_polygon(params: Dict[str, Any]) -> List[str]:
    """Get company tickers within a polygon"""
    coordinates = [
        (p["latitude"], p["longitude"]) for p in params["coordinates"]
    ]
    companies_data = await async_redis_manager.get_companies_in_region(
        coordinates, limit=1000
    )
    return [comp["ticker"] for comp in companies_data]


async def _get_companies_in_state(params: Dict[str, Any]) -> List[str]:
    """Get company tickers within a state"""
    query = f"SELECT ticker FROM {settings.questdb_companies_table} WHERE state = $1"
    results = await async_questdb_manager.fetch_all(
        query, params["state"].upper()
    )
    return [row["ticker"] for row in results]


async def _calculate_regional_stats(
    tickers: List[str], request: ValidatedRegionStatsRequest
) -> RegionalStats:
    """Calculate market statistics for a list of tickers using QuestDB"""
    if not tickers:
        return RegionalStats(region_description="Empty region", company_count=0)

    try:
        if len(tickers) > 20:
            # For large ticker lists, use a simpler aggregation
            return RegionalStats(
                region_description=f"Region with {len(tickers)} companies",
                company_count=len(tickers),
                avg_price=None,
                median_price=None,
                total_volume=None,
                volatility=None,
                price_range={"min": None, "max": None},
            )

        # For smaller lists, aggregate data from individual queries
        all_prices = []
        all_volumes = []
        for ticker in tickers:
            # Build query deterministically based on date params
            base_params = [ticker]  # ticker is always $1

            # Build date filter conditions with proper param indices
            date_conditions = []
            if request.start_date:
                base_params.append(request.start_date)
                date_conditions.append(f"ts >= ${len(base_params)}")
            if request.end_date:
                base_params.append(request.end_date)
                date_conditions.append(f"ts <= ${len(base_params)}")

            date_filter = (
                " AND " + " AND ".join(date_conditions)
                if date_conditions
                else ""
            )

            ticker_query = f"""
                SELECT close, volume
                FROM {settings.questdb_ohlcv_table}
                WHERE ticker = $1{date_filter}
                ORDER BY ts DESC
                LIMIT 100
            """

            ticker_results = await async_questdb_manager.fetch_all(
                ticker_query, *base_params
            )
            for row in ticker_results:
                if row["close"] is not None:
                    all_prices.append(float(row["close"]))
                if row["volume"] is not None:
                    all_volumes.append(int(row["volume"]))

        if not all_prices:
            return RegionalStats(
                region_description=f"Region with {len(tickers)} companies",
                company_count=len(tickers),
                avg_price=None,
                median_price=None,
                total_volume=None,
                volatility=None,
                price_range={"min": None, "max": None},
            )

        # Calculate statistics manually
        import statistics

        avg_price = statistics.mean(all_prices)
        total_volume = sum(all_volumes) if all_volumes else None
        volatility = (
            statistics.stdev(all_prices) if len(all_prices) > 1 else None
        )
        min_price = min(all_prices)
        max_price = max(all_prices)

        return RegionalStats(
            region_description=f"Region with {len(tickers)} companies",
            company_count=len(tickers),
            avg_price=round(avg_price, 2),
            median_price=None,
            total_volume=total_volume,
            volatility=round(volatility, 2) if volatility else None,
            price_range={
                "min": round(min_price, 2),
                "max": round(max_price, 2),
            },
        )
    except Exception:
        # On backend errors, return zeros
        return RegionalStats(
            region_description=f"Region with {len(tickers)} companies",
            company_count=len(tickers),
            avg_price=None,
            median_price=None,
            total_volume=None,
            volatility=None,
            price_range={"min": None, "max": None},
        )
