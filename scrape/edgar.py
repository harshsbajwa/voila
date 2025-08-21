import time
import random
import threading
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import backoff
import duckdb
from tqdm import tqdm
from colorama import Fore, Style

from shared.config import settings

# EDGAR API configuration
EDGAR_BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# Rate limiting: SEC requires max 10 requests per second
RATE_LIMIT_DELAY = 0.15  # 150ms between requests
RATE_LIMIT_JITTER = 0.05

# Threading setup
thread_local = threading.local()

# Headers required by SEC.gov
HEADERS = {
    "User-Agent": "voila-market-scraper/1.0 (compliance@example.com)",
    "Accept": "application/json",
}


def get_session():
    """Get thread-local requests session"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session


@backoff.on_exception(
    backoff.expo,
    (requests.RequestException, requests.HTTPError),
    max_tries=3,
    jitter=backoff.full_jitter,
    max_time=30,
)
def make_edgar_request(
    url: str, params: Optional[Dict] = None
) -> Optional[Dict]:
    """Make rate-limited request to EDGAR API"""
    # Rate limiting with jitter
    delay = RATE_LIMIT_DELAY + random.uniform(
        -RATE_LIMIT_JITTER, RATE_LIMIT_JITTER
    )
    time.sleep(delay)

    session = get_session()
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None  # Company not found in EDGAR
        elif e.response.status_code == 429:
            raise  # Let backoff handle rate limiting
        else:
            return None
    except (requests.RequestException, Exception):
        return None


def get_company_tickers_mapping() -> Dict[str, str]:
    """Get mapping of ticker -> CIK from SEC"""
    try:
        data = make_edgar_request(COMPANY_TICKERS_URL)
        if not data:
            return {}

        # Convert to ticker -> CIK mapping
        ticker_to_cik = {}
        for entry in data.values():
            if (
                isinstance(entry, dict)
                and "ticker" in entry
                and "cik_str" in entry
            ):
                ticker = entry["ticker"].upper()
                cik = str(entry["cik_str"]).zfill(10)  # Pad to 10 digits
                ticker_to_cik[ticker] = cik

        return ticker_to_cik
    except Exception:
        return {}


def extract_company_info(ticker: str, cik: str) -> Optional[Dict]:
    """Extract company information from EDGAR"""
    try:
        # Get company facts for entity name
        facts_url = f"{EDGAR_BASE_URL}/CIK{cik}.json"
        facts_data = make_edgar_request(facts_url)

        if not facts_data:
            return None

        company_info = {
            "ticker": ticker,
            "cik": cik,
            "name": facts_data.get("entityName"),
            "address": None,
            "city": None,
            "state": None,
            "zip": None,
            "sic": None,
            "business_description": None,
        }

        # Get detailed information from submissions endpoint
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        submissions_data = make_edgar_request(submissions_url)

        if submissions_data:
            # Use name from submissions if not found in facts
            if not company_info["name"]:
                company_info["name"] = submissions_data.get("name")

            # Extract address from submissions data
            addresses = submissions_data.get("addresses", {})

            # Try business address first, then mailing address
            for addr_type in ["business", "mailing"]:
                if addr_type in addresses:
                    addr_info = addresses[addr_type]
                    if isinstance(addr_info, dict):
                        # Combine street1 and street2 if available
                        street_parts = []
                        if addr_info.get("street1"):
                            street_parts.append(addr_info["street1"])
                        if addr_info.get("street2"):
                            street_parts.append(addr_info["street2"])

                        company_info["address"] = (
                            ", ".join(street_parts) if street_parts else None
                        )
                        company_info["city"] = addr_info.get("city")
                        company_info["state"] = addr_info.get("stateOrCountry")
                        company_info["zip"] = addr_info.get("zipCode")
                        break

            # Extract SIC and description
            company_info["sic"] = submissions_data.get("sic")
            company_info["business_description"] = submissions_data.get(
                "sicDescription"
            )

        return company_info

    except Exception:
        return None


def process_tickers_batch(
    tickers: List[str], ticker_to_cik: Dict[str, str]
) -> List[Dict]:
    """Process a batch of tickers for company information"""
    results = []

    # Filter tickers that have CIK mappings
    valid_tickers = [
        (t, ticker_to_cik[t]) for t in tickers if t in ticker_to_cik
    ]

    if not valid_tickers:
        return results

    with ThreadPoolExecutor(
        max_workers=3
    ) as executor:  # Conservative for rate limiting
        # Submit extraction jobs
        future_to_ticker = {
            executor.submit(extract_company_info, ticker, cik): ticker
            for ticker, cik in valid_tickers
        }

        # Collect results
        for future in as_completed(future_to_ticker):
            try:
                info = future.result()
                if info and info.get("name"):
                    results.append(info)
            except Exception:
                continue

    return results


def load_tickers_from_db() -> List[str]:
    """Load unique tickers from DuckDB"""
    try:
        with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
            result = conn.execute(
                "SELECT DISTINCT Ticker FROM ohlcv ORDER BY Ticker"
            ).fetchall()
            return [row[0] for row in result if row[0]]
    except Exception:
        return []


def save_company_info_to_db(company_data: List[Dict]):
    """Save company information to DuckDB locations table"""
    if not company_data:
        return

    try:
        with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
            # Clear existing data
            conn.execute("DELETE FROM locations")

            # Insert new data
            insert_sql = """
            INSERT INTO locations (Ticker, Name, Address, Latitude, Longitude)
            VALUES (?, ?, ?, NULL, NULL)
            """

            for company in company_data:
                # Combine address fields
                address_parts = []
                if company.get("address"):
                    address_parts.append(company["address"])
                if company.get("city"):
                    address_parts.append(company["city"])
                if company.get("state"):
                    address_parts.append(company["state"])
                if company.get("zip"):
                    address_parts.append(company["zip"])

                full_address = (
                    ", ".join(filter(None, address_parts))
                    if address_parts
                    else None
                )

                conn.execute(
                    insert_sql,
                    [company["ticker"], company["name"], full_address],
                )
    except Exception:
        pass


def main():
    """Main function to extract company information for all tickers"""
    print("Starting EDGAR company information extraction...")

    # Load tickers from database
    tickers = load_tickers_from_db()
    if not tickers:
        print("No tickers found in database")
        return

    print(f"Found {len(tickers)} tickers to process")

    # Get ticker->CIK mapping
    print("Fetching ticker->CIK mapping from SEC...")
    ticker_to_cik = get_company_tickers_mapping()

    if not ticker_to_cik:
        print("Failed to get ticker->CIK mapping")
        return

    # Filter to tickers we can process
    processable_tickers = [t for t in tickers if t in ticker_to_cik]
    print(
        f"Found CIK mappings for {len(processable_tickers)}/{len(tickers)} tickers"
    )

    if not processable_tickers:
        print("No tickers have CIK mappings")
        return

    # Process in batches with progress bar
    batch_size = 50
    all_results = []

    with tqdm(
        total=len(processable_tickers),
        desc="Extracting company data",
        bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.GREEN, Style.RESET_ALL),
    ) as pbar:
        for i in range(0, len(processable_tickers), batch_size):
            batch = processable_tickers[i : i + batch_size]
            batch_results = process_tickers_batch(batch, ticker_to_cik)
            all_results.extend(batch_results)

            pbar.update(len(batch))
            pbar.set_postfix(
                {"✓": len(all_results), "✗": i + len(batch) - len(all_results)}
            )

    # Save to database
    print(f"\nSaving {len(all_results)} company records to database...")
    save_company_info_to_db(all_results)

    print("\nEDGAR extraction complete!")
    print(f"   • Processed: {len(processable_tickers)} tickers")
    print(f"   • Successful: {len(all_results)} companies")
    print(
        f"   • Success rate: {len(all_results) / len(processable_tickers) * 100:.1f}%"
    )


if __name__ == "__main__":
    main()
