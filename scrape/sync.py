#!/usr/bin/env python3
import time
from datetime import datetime, timezone, date, timedelta
from typing import List, Dict

import duckdb
import redis
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm
from colorama import Fore, Style
import math

from shared.config import settings


class QuestDBManager:
    """Manages QuestDB connections and operations"""

    def __init__(self):
        self.connection_params = {
            "host": settings.questdb_host,
            "port": settings.questdb_pg_port,
            "user": settings.questdb_user,
            "password": settings.questdb_password,
            "database": settings.questdb_database,
        }

    def get_connection(self):
        """Get a QuestDB connection via PostgreSQL wire protocol"""
        return psycopg2.connect(**self.connection_params)

    def create_tables(self):
        """Create QuestDB tables with proper partitioning"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Recreate OHLCV table with minimal column set used by API to avoid corrupted/unused cols
                try:
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {settings.questdb_ohlcv_table}"
                    )
                except Exception:
                    pass
                cursor.execute(f"""
                    CREATE TABLE {settings.questdb_ohlcv_table} (
                        ts TIMESTAMP,
                        ticker SYMBOL,
                        open DOUBLE,
                        high DOUBLE,
                        low DOUBLE,
                        close DOUBLE,
                        volume LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL;
                """)

                # Create companies table with geospatial columns
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {settings.questdb_companies_table} (
                        ticker SYMBOL,
                        name STRING,
                        address STRING,
                        city STRING,
                        state STRING,
                        country STRING,
                        latitude DOUBLE,
                        longitude DOUBLE,
                        sic_code STRING,
                        business_description STRING,
                        last_updated TIMESTAMP
                    );
                """)

                conn.commit()

    def ensure_tables(self) -> None:
        """Ensure tables exist without dropping them (for incremental updates)"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {settings.questdb_ohlcv_table} (
                        ts TIMESTAMP,
                        ticker SYMBOL,
                        open DOUBLE,
                        high DOUBLE,
                        low DOUBLE,
                        close DOUBLE,
                        volume LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL;
                """)

                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {settings.questdb_companies_table} (
                        ticker SYMBOL,
                        name STRING,
                        address STRING,
                        city STRING,
                        state STRING,
                        country STRING,
                        latitude DOUBLE,
                        longitude DOUBLE,
                        sic_code STRING,
                        business_description STRING,
                        last_updated TIMESTAMP
                    );
                """)

                conn.commit()

    def sync_ohlcv_data(self, batch_data: List[Dict]) -> bool:
        """Sync OHLCV data to QuestDB using batch insert"""
        if not batch_data:
            return True

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Prepare batch insert
                    insert_sql = f"""
                        INSERT INTO {settings.questdb_ohlcv_table} 
                        (ts, ticker, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

                    # Convert data to tuple format
                    batch_tuples = []
                    for row in batch_data:
                        # Normalize to UTC naive to avoid timestamptz casts
                        ts = row["Date"]
                        if getattr(ts, "tzinfo", None) is not None:
                            ts = ts.astimezone(timezone.utc).replace(
                                tzinfo=None
                            )

                        batch_tuples.append(
                            (
                                ts,
                                row["Ticker"],
                                float(row["Open"]),
                                float(row["High"]),
                                float(row["Low"]),
                                float(row["Close"]),
                                int(row["Volume"]),
                            )
                        )

                    # Execute batch insert
                    execute_batch(
                        cursor, insert_sql, batch_tuples, page_size=1000
                    )
                    conn.commit()
                    return True

        except Exception as e:
            print(f"Error syncing OHLCV data to QuestDB: {e}")
            return False

    def sync_companies_data(self, batch_data: List[Dict]) -> bool:
        """Sync company data to QuestDB"""
        if not batch_data:
            return True

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Refresh the table to avoid duplicates and unsupported UPSERT syntax
                    try:
                        cursor.execute(
                            f"TRUNCATE TABLE {settings.questdb_companies_table}"
                        )
                    except Exception:
                        cursor.execute(
                            f"DELETE FROM {settings.questdb_companies_table}"
                        )

                    insert_sql = f"""
                        INSERT INTO {settings.questdb_companies_table}
                        (ticker, name, address, city, state, country, latitude, longitude, 
                         sic_code, business_description, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    batch_tuples = []
                    for row in batch_data:
                        # Parse address into components if available
                        address_parts = (row.get("Address") or "").split(", ")
                        city = (
                            address_parts[-3]
                            if len(address_parts) >= 3
                            else None
                        )
                        state = (
                            address_parts[-2]
                            if len(address_parts) >= 2
                            else None
                        )

                        batch_tuples.append(
                            (
                                row["Ticker"],
                                row["Name"],
                                row.get("Address"),
                                city,
                                state,
                                "USA",  # Assuming US companies for now
                                row.get("Latitude"),
                                row.get("Longitude"),
                                None,  # SIC code - add if available
                                None,  # Business description - add if available
                                datetime.now(timezone.utc).replace(tzinfo=None),
                            )
                        )

                    execute_batch(
                        cursor, insert_sql, batch_tuples, page_size=500
                    )
                    conn.commit()
                    return True

        except Exception as e:
            print(f"Error syncing companies data to QuestDB: {e}")
            return False


class RedisGeoManager:
    """Manages Redis geospatial indexing"""

    def __init__(self):
        self.redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=True,
        )
        self.geo_key = settings.redis_geo_key

    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            self.redis_client.ping()
            return True
        except Exception:
            return False

    def sync_company_locations(self, companies_data: List[Dict]) -> int:
        """Sync company locations to Redis geospatial index. Returns count, or -1 on failure."""
        if not companies_data:
            return 0

        try:
            # Clear existing data
            self.redis_client.delete(self.geo_key)

            # Add companies to geospatial index
            pipe = self.redis_client.pipeline()
            location_count = 0

            for company in companies_data:
                raw_lat = company.get("Latitude")
                raw_lng = company.get("Longitude")
                raw_ticker = company.get("Ticker")

                # Validate and normalize inputs
                try:
                    lat = float(raw_lat)
                    lng = float(raw_lng)
                    if math.isnan(lat) or math.isnan(lng):
                        continue
                except (TypeError, ValueError):
                    continue

                if raw_ticker is None:
                    continue
                ticker = str(raw_ticker)
                if ticker == "":
                    continue

                # Use raw command for compatibility across redis-py versions
                pipe.execute_command("GEOADD", self.geo_key, lng, lat, ticker)
                location_count += 1

                # Also store company metadata with namespace
                metadata_key = f"voila:company:{ticker}"
                pipe.hset(
                    metadata_key,
                    mapping={
                        "name": company.get("Name", ""),
                        "address": company.get("Address", ""),
                        "latitude": lat,
                        "longitude": lng,
                    },
                )
                pipe.expire(metadata_key, settings.redis_cache_ttl)

            # Execute pipeline
            pipe.execute()
            print(
                f"Added {location_count} company locations to Redis geospatial index"
            )
            return location_count

        except Exception as e:
            print(f"Error syncing to Redis: {e}")
            return -1

    def get_nearby_companies(
        self, lat: float, lng: float, radius_km: float, limit: int = 100
    ) -> List[Dict]:
        """Get companies within radius using Redis GEORADIUS"""
        try:
            # GEORADIUS returns list of members within radius
            nearby = self.redis_client.georadius(
                self.geo_key,
                lng,
                lat,
                radius_km,
                unit="km",
                withdist=True,
                withcoord=True,
                sort="ASC",
                count=limit,
            )

            results = []
            for item in nearby:
                ticker = item[0]
                distance_km = float(item[1])
                coords = item[2]

                # Get company metadata with namespace
                metadata = self.redis_client.hgetall(f"voila:company:{ticker}")

                results.append(
                    {
                        "ticker": ticker,
                        "distance_km": distance_km,
                        "latitude": coords[1],
                        "longitude": coords[0],
                        "name": metadata.get("name", ""),
                        "address": metadata.get("address", ""),
                    }
                )

            return results

        except Exception as e:
            print(f"Error querying Redis geospatial: {e}")
            return []


class LiveDataSynchronizer:
    """Main synchronization orchestrator"""

    def __init__(self):
        self.questdb = QuestDBManager()
        self.redis_geo = RedisGeoManager()
        self.redis_added_count = 0

    def validate_connections(self, skip_questdb: bool = False) -> bool:
        """Validate all connections before starting"""
        print("Validating connections...")

        # Test QuestDB (optional)
        if not skip_questdb:
            try:
                with self.questdb.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                print(
                    f"{Fore.GREEN}✓ QuestDB connection successful{Style.RESET_ALL}"
                )
            except Exception as e:
                print(
                    f"{Fore.RED}✗ QuestDB connection failed: {e}{Style.RESET_ALL}"
                )
                return False

        # Test Redis
        if self.redis_geo.ping():
            print(f"{Fore.GREEN}✓ Redis connection successful{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}✗ Redis connection failed{Style.RESET_ALL}")
            return False

        # Test DuckDB staging
        try:
            with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
                conn.execute("SELECT 1")
            print(
                f"{Fore.GREEN}✓ DuckDB staging connection successful{Style.RESET_ALL}"
            )
        except Exception as e:
            print(
                f"{Fore.RED}✗ DuckDB staging connection failed: {e}{Style.RESET_ALL}"
            )
            return False

        return True

    def sync_all_data(self, skip_questdb: bool = False) -> bool:
        """Synchronize all data from DuckDB staging to live systems"""
        # Reset counters for each run
        self.redis_added_count = 0

        if not self.validate_connections(skip_questdb=skip_questdb):
            return False

        print(
            f"\n{Fore.CYAN}Starting live data synchronization...{Style.RESET_ALL}"
        )

        # Create tables and sync OHLCV (unless skipping QuestDB)
        if not skip_questdb:
            print("Creating QuestDB tables...")
            self.questdb.create_tables()

            # Sync OHLCV data
            if not self._sync_ohlcv_data():
                return False

        # Sync company data and geospatial index
        if not self._sync_companies_data(skip_questdb=skip_questdb):
            return False

        print(
            f"\n{Fore.GREEN}✓ Live data synchronization completed successfully!{Style.RESET_ALL}"
        )
        return True

    def _sync_ohlcv_data(self) -> bool:
        """Sync OHLCV data from DuckDB to QuestDB"""
        print("\nSyncing OHLCV data to QuestDB...")

        try:
            with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
                # Get total count for progress bar
                result = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
                total_rows = result[0] if result else 0
                print(f"Found {total_rows:,} OHLCV records to sync")

                if total_rows == 0:
                    print("No OHLCV data to sync")
                    return True

                # Process in batches
                batch_size = settings.questdb_batch_size
                batches_processed = 0

                with tqdm(
                    total=total_rows,
                    desc="Syncing OHLCV",
                    bar_format="{l_bar}%s{bar}%s{r_bar}"
                    % (Fore.BLUE, Style.RESET_ALL),
                ) as pbar:
                    offset = 0
                    while offset < total_rows:
                        # Fetch batch from DuckDB
                        query = f"""
                            SELECT * FROM ohlcv 
                            ORDER BY Ticker, Date 
                            LIMIT {batch_size} OFFSET {offset}
                        """
                        batch_df = conn.execute(query).df()

                        if batch_df.empty:
                            break

                        # Convert to list of dicts
                        batch_data = batch_df.to_dict("records")

                        # Sync to QuestDB
                        if self.questdb.sync_ohlcv_data(batch_data):
                            batches_processed += 1
                            pbar.update(len(batch_data))
                        else:
                            print(
                                f"Failed to sync batch {batches_processed + 1}"
                            )
                            return False

                        offset += batch_size

                print(
                    f"Successfully synced {batches_processed} batches to QuestDB"
                )
                return True

        except Exception as e:
            print(f"Error syncing OHLCV data: {e}")
            return False

    def _sync_companies_data(self, skip_questdb: bool = False) -> bool:
        """Sync company data from DuckDB to QuestDB and Redis"""
        print("\nSyncing company data to QuestDB and Redis...")

        try:
            with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
                # Get company data with location info
                query = """
                    SELECT * FROM locations 
                    WHERE Name IS NOT NULL
                    ORDER BY Ticker
                """
                companies_df = conn.execute(query).df()

                if companies_df.empty:
                    print("No company data to sync")
                    return True

                companies_data = companies_df.to_dict("records")
                print(f"Found {len(companies_data)} companies to sync")

                if not skip_questdb:
                    # Sync to QuestDB
                    print("Syncing companies to QuestDB...")
                    if not self.questdb.sync_companies_data(companies_data):
                        return False

                # Sync to Redis geospatial index (always)
                print("Syncing company locations to Redis...")
                added = self.redis_geo.sync_company_locations(companies_data)
                # Store count for summary
                self.redis_added_count = max(0, added)
                if added < 0:
                    return False

                print(f"Successfully synced {len(companies_data)} companies")
                return True

        except Exception as e:
            print(f"Error syncing company data: {e}")
            return False

    def sync_ohlcv_for_date(self, target_date: date) -> bool:
        """Sync OHLCV data for a specific date from DuckDB to QuestDB"""
        print(f"\nSyncing OHLCV data for {target_date} to QuestDB...")

        try:
            with duckdb.connect(str(settings.duckdb_path_resolved)) as conn:
                start_date = target_date
                end_date = target_date + timedelta(days=1)

                query = """
                    SELECT * FROM ohlcv
                    WHERE Date >= ? AND Date < ?
                    ORDER BY Ticker, Date
                """

                batch_df = conn.execute(
                    query,
                    parameters=[
                        start_date.strftime("%Y-%m-%d"),
                        end_date.strftime("%Y-%m-%d"),
                    ],
                ).df()

                if batch_df.empty:
                    print(f"No OHLCV data found for {target_date}")
                    return True

                batch_data = batch_df.to_dict("records")
                print(
                    f"Found {len(batch_data)} OHLCV records for {target_date}"
                )

                if self.questdb.sync_ohlcv_data(batch_data):
                    print(
                        f"Successfully synced {len(batch_data)} records to QuestDB"
                    )
                    return True
                else:
                    print(f"Failed to sync data for {target_date}")
                    return False

        except Exception as e:
            print(f"Error syncing OHLCV data for {target_date}: {e}")
            return False

    def sync_incremental(
        self, target_date: date, skip_questdb: bool = False
    ) -> bool:
        """Perform incremental sync for a specific date"""
        self.redis_added_count = 0

        if not self.validate_connections(skip_questdb=skip_questdb):
            return False

        print(
            f"\n{Fore.CYAN}Starting incremental sync for {target_date}...{Style.RESET_ALL}"
        )

        if not skip_questdb:
            print("Ensuring QuestDB tables exist...")
            self.questdb.ensure_tables()

            if not self.sync_ohlcv_for_date(target_date):
                return False

        if not self._sync_companies_data(skip_questdb=skip_questdb):
            return False

        print(
            f"\n{Fore.GREEN}✓ Incremental sync completed successfully!{Style.RESET_ALL}"
        )
        return True


def main():
    """Main entry point"""
    print(f"{Fore.GREEN} DATA SYNCHRONIZATION{Style.RESET_ALL}")
    print("=" * 50)

    synchronizer = LiveDataSynchronizer()

    start_time = time.time()
    success = synchronizer.sync_all_data()
    elapsed = time.time() - start_time

    if success:
        print(
            f"\n{Fore.GREEN} Synchronization completed in {elapsed:.1f}s{Style.RESET_ALL}"
        )
        print("\nData is now available in:")
        print(
            f"  • QuestDB: {settings.questdb_host}:{settings.questdb_pg_port}"
        )
        print(
            f"  • Redis Geo Index: {settings.redis_host}:{settings.redis_port}"
        )
    else:
        print(
            f"\n{Fore.RED} Synchronization failed after {elapsed:.1f}s{Style.RESET_ALL}"
        )
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
