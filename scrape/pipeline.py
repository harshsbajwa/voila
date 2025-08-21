#!/usr/bin/env python3
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List

import duckdb
from colorama import Fore, Style, init

init(autoreset=True)

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrape import backfill, edgar, geocoder, spatial
from scrape.sync import LiveDataSynchronizer
from shared.config import DUCKDB_PATH


class PipelineProgress:
    """Progress tracker for the pipeline"""

    def __init__(self):
        self.steps = [
            "Load Tickers",
            "Scrape OHLCV Data",
            "Extract Company Info (EDGAR)",
            "Geocode Addresses",
            "Setup Spatial Database",
            "Export to Parquet",
            "Sync to Live Systems (QuestDB + Redis)",
        ]
        self.current_step = 0
        self.total_steps = len(self.steps)
        self.step_progress = {}
        self.start_time = time.time()

    def start_step(self, step_name: str):
        """Start a new pipeline step"""
        self.current_step += 1
        print(
            f"\n{Fore.CYAN}[{self.current_step}/{self.total_steps}] {step_name}{Style.RESET_ALL}"
        )
        self.step_progress[step_name] = {
            "start": time.time(),
            "status": "running",
        }

    def complete_step(
        self, step_name: str, success: bool = True, details: str = ""
    ):
        """Complete a pipeline step"""
        if step_name in self.step_progress:
            elapsed = time.time() - self.step_progress[step_name]["start"]
            status = "✓ Complete" if success else "✗ Failed"
            self.step_progress[step_name]["status"] = (
                "success" if success else "failed"
            )
            self.step_progress[step_name]["elapsed"] = elapsed

            detail_str = f" - {details}" if details else ""
            print(f"{status} in {elapsed:.1f}s{detail_str}")

        if not success:
            print(
                f"{Fore.RED}Pipeline failed at step: {step_name}{Style.RESET_ALL}"
            )

    def show_summary(self):
        """Show pipeline execution summary"""
        total_time = time.time() - self.start_time
        successful_steps = sum(
            1
            for step in self.step_progress.values()
            if step.get("status") == "success"
        )

        print(f"\n{'=' * 60}")
        print(f"{Fore.GREEN}PIPELINE EXECUTION SUMMARY{Style.RESET_ALL}")
        print(f"{'=' * 60}")
        print(f"Total execution time: {total_time:.1f}s")
        print(f"Completed steps: {successful_steps}/{self.total_steps}")
        print()

        for step_name, progress in self.step_progress.items():
            status_icon = "✓" if progress.get("status") == "success" else "✗"
            elapsed = progress.get("elapsed", 0)
            print(f"{status_icon} {step_name}: {elapsed:.1f}s")


class DataPipeline:
    """Streamlined data pipeline orchestrator"""

    def __init__(
        self,
        skip_scraping: bool = False,
        skip_geocoding: bool = False,
        sync_only: bool = False,
        skip_questdb: bool = False,
    ):
        self.skip_scraping = skip_scraping
        self.skip_geocoding = skip_geocoding
        self.sync_only = sync_only
        self.skip_questdb = skip_questdb
        self.progress = PipelineProgress()
        self.stats = {
            "tickers_loaded": 0,
            "ohlcv_records": 0,
            "companies_found": 0,
            "addresses_geocoded": 0,
            "spatial_records": 0,
            "questdb_records": 0,
        }

    def load_tickers(self) -> List[str]:
        """Load or generate ticker list"""
        step_name = "Load Tickers"
        self.progress.start_step(step_name)

        try:
            tickers = backfill.load()
            self.stats["tickers_loaded"] = len(tickers)
            self.progress.complete_step(
                step_name, True, f"{len(tickers)} tickers"
            )
            return tickers
        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return []

    def scrape_ohlcv_data(self, tickers) -> bool:
        """Scrape OHLCV data for all tickers"""
        step_name = "Scrape OHLCV Data"
        self.progress.start_step(step_name)

        if self.skip_scraping:
            # Get existing record count
            try:
                with duckdb.connect(str(DUCKDB_PATH)) as conn:
                    result = conn.execute(
                        "SELECT COUNT(*) FROM ohlcv"
                    ).fetchone()
                    self.stats["ohlcv_records"] = result[0] if result else 0
            except:
                pass
            self.progress.complete_step(
                step_name,
                True,
                f"Skipped - {self.stats['ohlcv_records']} existing records",
            )
            return True

        try:
            # Initialize database
            backfill.init(DUCKDB_PATH)

            # Run scraping
            failed = backfill.fill(tickers)

            # Get record count
            with duckdb.connect(str(DUCKDB_PATH)) as conn:
                result = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
                self.stats["ohlcv_records"] = result[0] if result else 0

            success_count = len(tickers) - len(failed)
            self.progress.complete_step(
                step_name,
                True,
                f"{success_count}/{len(tickers)} tickers, {self.stats['ohlcv_records']} records",
            )
            return True

        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return False

    def extract_company_info(self) -> bool:
        """Extract company information from EDGAR"""
        step_name = "Extract Company Info (EDGAR)"
        self.progress.start_step(step_name)

        try:
            # Run EDGAR extraction
            edgar.main()

            # Get company count
            with duckdb.connect(str(DUCKDB_PATH)) as conn:
                result = conn.execute(
                    "SELECT COUNT(*) FROM locations WHERE Name IS NOT NULL"
                ).fetchone()
                self.stats["companies_found"] = result[0] if result else 0

            self.progress.complete_step(
                step_name, True, f"{self.stats['companies_found']} companies"
            )
            return True

        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return False

    def geocode_addresses(self) -> bool:
        """Geocode company addresses"""
        step_name = "Geocode Addresses"
        self.progress.start_step(step_name)

        if self.skip_geocoding:
            # Get existing geocoded count
            try:
                with duckdb.connect(str(DUCKDB_PATH)) as conn:
                    result = conn.execute("""
                        SELECT COUNT(*) FROM locations 
                        WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
                    """).fetchone()
                    self.stats["addresses_geocoded"] = (
                        result[0] if result else 0
                    )
            except:
                pass
            self.progress.complete_step(
                step_name,
                True,
                f"Skipped - {self.stats['addresses_geocoded']} existing",
            )
            return True

        try:
            # Run geocoding
            geocoder.main()

            # Get geocoded count
            with duckdb.connect(str(DUCKDB_PATH)) as conn:
                result = conn.execute("""
                    SELECT COUNT(*) FROM locations 
                    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
                """).fetchone()
                self.stats["addresses_geocoded"] = result[0] if result else 0

            self.progress.complete_step(
                step_name, True, f"{self.stats['addresses_geocoded']} addresses"
            )
            return True

        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return False

    def setup_spatial_database(self) -> bool:
        """Setup DuckDB spatial database and export parquet"""
        step_name = "Setup Spatial Database"
        self.progress.start_step(step_name)

        try:
            # Setup spatial extensions and tables
            success = spatial.setup_spatial_database()
            if not success:
                self.progress.complete_step(
                    step_name, False, "Spatial setup failed"
                )
                return False

            # Get spatial record count from locations table
            with duckdb.connect(str(DUCKDB_PATH)) as conn:
                result = conn.execute("""
                    SELECT COUNT(*) FROM locations 
                    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
                """).fetchone()
                self.stats["spatial_records"] = result[0] if result else 0

            self.progress.complete_step(
                step_name,
                True,
                f"{self.stats['spatial_records']} spatial records",
            )
            return True

        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return False

    def export_to_parquet(self) -> bool:
        """Export spatial data to Parquet"""
        step_name = "Export to Parquet"
        self.progress.start_step(step_name)

        try:
            # Export spatial data
            count = spatial.export_spatial_data()

            self.progress.complete_step(
                step_name, True, f"{count} records exported"
            )
            return True

        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return False

    def sync_to_live_systems(self) -> bool:
        """Sync staged data to QuestDB and Redis geospatial index"""
        step_name = "Sync to Live Systems (QuestDB + Redis)"
        self.progress.start_step(step_name)

        try:
            # Create synchronizer and run sync
            synchronizer = LiveDataSynchronizer()
            success = synchronizer.sync_all_data(skip_questdb=self.skip_questdb)

            if success:
                # Estimate records (same as OHLCV records) unless skipped
                self.stats["questdb_records"] = (
                    self.stats["ohlcv_records"] if not self.skip_questdb else 0
                )
                # Compose Redis detail using synchronizer's actual count if available
                redis_locations = getattr(
                    synchronizer, "redis_added_count", None
                )
                if isinstance(redis_locations, int) and redis_locations >= 0:
                    redis_detail = f"Redis: {redis_locations} locations"
                    self.stats["addresses_geocoded"] = redis_locations
                else:
                    redis_detail = (
                        f"Redis: {self.stats['addresses_geocoded']} locations"
                    )

                self.progress.complete_step(
                    step_name,
                    True,
                    f"QuestDB: {self.stats['questdb_records']} records, {redis_detail}",
                )
                return True
            else:
                self.progress.complete_step(
                    step_name, False, "Synchronization failed"
                )
                return False

        except Exception as e:
            self.progress.complete_step(step_name, False, str(e))
            return False

    def run(self) -> bool:
        """Execute the complete pipeline"""
        print(
            f"{Fore.GREEN} STARTING VOILA MARKET DATA PIPELINE{Style.RESET_ALL}"
        )
        print(f"{Fore.GREEN}{'=' * 50}{Style.RESET_ALL}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: {DUCKDB_PATH}")
        print(f"Skip scraping: {self.skip_scraping}")
        print(f"Skip geocoding: {self.skip_geocoding}")
        print(f"Sync only: {self.sync_only}")
        print()

        # Fast path: only run synchronization step
        if self.sync_only:
            return self.sync_to_live_systems()

        # Execute pipeline steps
        steps = [
            (self.load_tickers, lambda: self.tickers),
            (lambda: self.scrape_ohlcv_data(self.tickers), lambda: True),
            (self.extract_company_info, lambda: True),
            (self.geocode_addresses, lambda: True),
            (self.setup_spatial_database, lambda: True),
            (self.export_to_parquet, lambda: True),
            (self.sync_to_live_systems, lambda: True),
        ]

        for i, (step_func, condition_func) in enumerate(steps):
            if i == 0:  # Load tickers
                self.tickers = step_func()
                if not self.tickers:
                    return False
            else:
                try:
                    condition_func()  # Check precondition
                    if not step_func():
                        return False
                except:
                    return False

        # Show summary
        self.progress.show_summary()

        print(
            f"\n{Fore.GREEN} PIPELINE COMPLETED SUCCESSFULLY!{Style.RESET_ALL}"
        )
        print("Final Statistics:")
        for key, value in self.stats.items():
            print(f"   • {key.replace('_', ' ').title()}: {value:,}")

        return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Voila Market Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py                    # Run full pipeline
  python pipeline.py --skip-scraping    # Skip OHLCV scraping (use existing data)
  python pipeline.py --skip-geocoding   # Skip address geocoding
  python pipeline.py --sync-only        # Skip directly to QuestDB + Redis sync
        """,
    )

    parser.add_argument(
        "--skip-scraping",
        action="store_true",
        help="Skip OHLCV data scraping (use existing database)",
    )

    parser.add_argument(
        "--skip-geocoding",
        action="store_true",
        help="Skip address geocoding step",
    )

    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Skip all steps and run only synchronization to live systems",
    )

    parser.add_argument(
        "--skip-questdb",
        action="store_true",
        help="Skip QuestDB connectivity and synchronization; only update Redis",
    )

    args = parser.parse_args()

    # Create and run pipeline
    pipeline = DataPipeline(
        skip_scraping=args.skip_scraping,
        skip_geocoding=args.skip_geocoding,
        sync_only=args.sync_only,
        skip_questdb=args.skip_questdb,
    )

    success = pipeline.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
