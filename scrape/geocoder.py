import time
import random
import re
import threading
from typing import Optional, Tuple, List

import requests
import backoff
import duckdb
from tqdm import tqdm
from colorama import Fore, Style

from shared.config import settings

# US Census Geocoding API
CENSUS_GEOCODE_URL = (
    "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
)

RATE_LIMIT_DELAY = 0.5  # 500ms between requests (2 req/s)
RATE_LIMIT_JITTER = 0.1

# Threading setup
thread_local = threading.local()

# Headers for Census API
HEADERS = {
    "User-Agent": "voila-market-scraper/1.0 (research purposes)",
    "Accept": "application/json",
}


def get_session():
    """Get thread-local requests session"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session


def normalize_address(address: str) -> str:
    """Normalize address for better geocoding success"""
    if not address:
        return address

    # Remove common prefixes that might confuse geocoder
    address = re.sub(
        r"^(C/O|CARE OF|ATTN:|ATTENTION:)\s+", "", address, flags=re.IGNORECASE
    )

    # Standardize common abbreviations
    replacements = {
        r"\bSTREET\b": "ST",
        r"\bAVENUE\b": "AVE",
        r"\bBOULEVARD\b": "BLVD",
        r"\bROAD\b": "RD",
        r"\bDRIVE\b": "DR",
        r"\bPLACE\b": "PL",
        r"\bLANE\b": "LN",
        r"\bCOURT\b": "CT",
        r"\bSUITE\b": "STE",
        r"\bAPARTMENT\b": "APT",
        r"\bFLOOR\b": "FL",
        r"\bNORTH\b": "N",
        r"\bSOUTH\b": "S",
        r"\bEAST\b": "E",
        r"\bWEST\b": "W",
    }

    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)

    # Clean up extra whitespace and punctuation
    address = re.sub(r"\s+", " ", address)  # Multiple spaces to single
    address = re.sub(r",\s*,", ",", address)  # Double commas
    address = address.strip()

    return address


@backoff.on_exception(
    backoff.expo,
    (requests.RequestException, requests.HTTPError),
    max_tries=3,
    jitter=backoff.full_jitter,
    max_time=60,
)
def geocode_address(address: str) -> Optional[Tuple[float, float]]:
    """Geocode an address using US Census Geocoder"""
    if not address or not address.strip():
        return None

    # Normalize address for better success rate
    normalized_address = normalize_address(address.strip())

    # Rate limiting with jitter
    delay = RATE_LIMIT_DELAY + random.uniform(
        -RATE_LIMIT_JITTER, RATE_LIMIT_JITTER
    )
    time.sleep(delay)

    session = get_session()

    params = {
        "address": normalized_address,
        "benchmark": "Public_AR_Current",  # Current address ranges
        "format": "json",
    }

    try:
        response = session.get(CENSUS_GEOCODE_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Parse Census API response
        result = data.get("result", {})
        address_matches = result.get("addressMatches", [])

        if address_matches:
            match = address_matches[0]  # Take first match
            coordinates = match.get("coordinates", {})

            if coordinates and "x" in coordinates and "y" in coordinates:
                lng = float(coordinates["x"])
                lat = float(coordinates["y"])

                # Basic validation - ensure coordinates are within US bounds
                if -180 <= lng <= -60 and 15 <= lat <= 75:
                    return (lat, lng)

        return None

    except requests.HTTPError as e:
        if e.response.status_code == 429:
            raise  # Let backoff handle rate limiting
        else:
            return None
    except (requests.RequestException, ValueError, KeyError):
        return None


def load_companies_for_geocoding() -> List[Tuple[str, str, str]]:
    """Load companies that need geocoding from database"""
    try:
        with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
            # Get companies that have addresses but no coordinates
            result = conn.execute("""
                SELECT Ticker, Name, Address 
                FROM locations 
                WHERE Address IS NOT NULL 
                AND Address != ''
                AND (Latitude IS NULL OR Longitude IS NULL)
                ORDER BY Ticker
            """).fetchall()

            return [(row[0], row[1], row[2]) for row in result]
    except Exception:
        return []


def update_coordinates_in_db(ticker: str, lat: float, lng: float):
    """Update coordinates for a company in the database"""
    try:
        with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
            conn.execute(
                """
                UPDATE locations 
                SET Latitude = ?, Longitude = ? 
                WHERE Ticker = ?
            """,
                [lat, lng, ticker],
            )
    except Exception:
        pass


def export_geojson(output_path: Optional[str] = None):
    """Export company locations as GeoJSON"""
    from pathlib import Path

    if output_path is None:
        output_path = str(settings.geojson_output_path)

    try:
        with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
            # Get all companies with coordinates
            result = conn.execute("""
                SELECT Ticker, Name, Address, Latitude, Longitude
                FROM locations 
                WHERE Latitude IS NOT NULL 
                AND Longitude IS NOT NULL
                ORDER BY Ticker
            """).fetchall()

            if not result:
                return

            # Build GeoJSON structure
            features = []
            for ticker, name, address, lat, lng in result:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lng, lat],  # GeoJSON uses [lng, lat]
                    },
                    "properties": {
                        "ticker": ticker,
                        "name": name,
                        "address": address,
                    },
                }
                features.append(feature)

            geojson = {"type": "FeatureCollection", "features": features}

            # Save to file
            output_path_obj = Path(output_path)
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)

            import json

            with open(output_path_obj, "w") as f:
                json.dump(geojson, f, indent=2)

            print(f"Exported {len(features)} companies to {output_path}")

    except Exception as e:
        print(f"Error exporting GeoJSON: {e}")


def main():
    """Main function to geocode company addresses"""
    print("Starting address geocoding with US Census Geocoder...")

    # Load companies that need geocoding
    companies = load_companies_for_geocoding()
    if not companies:
        print("No companies need geocoding")
        return

    print(f"Found {len(companies)} companies to geocode")

    # Process with progress bar
    successful_geocodes = 0
    failed_geocodes = 0

    with tqdm(
        total=len(companies),
        desc="🌍 Geocoding addresses",
        bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.BLUE, Style.RESET_ALL),
    ) as pbar:
        # Process sequentially to respect rate limits and avoid overloading Census API
        for ticker, name, address in companies:
            coordinates = geocode_address(address)

            if coordinates:
                lat, lng = coordinates
                update_coordinates_in_db(ticker, lat, lng)
                successful_geocodes += 1
            else:
                failed_geocodes += 1

            pbar.update(1)
            pbar.set_postfix({"✓": successful_geocodes, "✗": failed_geocodes})

    print("\nGeocoding complete!")
    print(f"   • Total addresses: {len(companies)}")
    print(f"   • Successfully geocoded: {successful_geocodes}")
    print(f"   • Failed: {failed_geocodes}")
    if len(companies) > 0:
        print(
            f"   • Success rate: {successful_geocodes / len(companies) * 100:.1f}%"
        )

    # Export GeoJSON if we have any successful geocodes
    if successful_geocodes > 0:
        print(f"\nExporting GeoJSON to {settings.geojson_output_path}...")
        export_geojson()


if __name__ == "__main__":
    main()
