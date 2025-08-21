import sys
from pathlib import Path
from datetime import timedelta
import logging

import pendulum
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)

repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))
load_dotenv(repo_root / ".env", override=False)

import pandas_market_calendars as mcal  # noqa: E402
from scrape import backfill  # noqa: E402
from scrape.sync import LiveDataSynchronizer  # noqa: E402

logger = logging.getLogger(__name__)

timezone = pendulum.timezone("America/New_York")

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email": ["hsbajwah@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}


def check_trading_day(**context) -> bool:
    """Check if the logical date is a trading day on NYSE"""
    logical_date = context["logical_date"]
    target_date = logical_date.in_timezone(timezone).date()

    logger.info(f"Checking if {target_date} is a trading day")

    nyse = mcal.get_calendar("XNYS")
    is_trading = (
        nyse.valid_days(
            start_date=target_date.strftime("%Y-%m-%d"),
            end_date=target_date.strftime("%Y-%m-%d"),
        ).size
        > 0
    )

    if is_trading:
        logger.info(f"{target_date} is a trading day")
    else:
        logger.info(
            f"{target_date} is not a trading day, skipping downstream tasks"
        )

    return is_trading


def incremental_scrape(**context):
    """Scrape OHLCV data for the target date"""
    logical_date = context["logical_date"]
    target_date = logical_date.in_timezone(timezone).date()

    logger.info(f"Starting incremental scrape for {target_date}")

    failed_tickers = backfill.incremental_fill_for_date(target_date)

    total_tickers = len(backfill.load())
    failed_count = len(failed_tickers)
    success_count = total_tickers - failed_count
    failure_rate = failed_count / total_tickers if total_tickers > 0 else 0

    logger.info(
        f"Scraping completed: {success_count} success, {failed_count} failed"
    )

    if failure_rate > 0.10:
        error_msg = f"Failure rate too high: {failure_rate:.2%} ({failed_count}/{total_tickers})"
        logger.error(error_msg)
        logger.error(
            f"Failed tickers (first 20): {list(failed_tickers.keys())[:20]}"
        )
        raise Exception(error_msg)

    return {
        "success": success_count,
        "failed": failed_count,
        "date": str(target_date),
    }


def sync_live_systems(**context):
    """Sync scraped data to QuestDB and Redis"""
    logical_date = context["logical_date"]
    target_date = logical_date.in_timezone(timezone).date()

    logger.info(f"Starting live systems sync for {target_date}")

    synchronizer = LiveDataSynchronizer()
    success = synchronizer.sync_incremental(target_date, skip_questdb=False)

    if not success:
        error_msg = f"Failed to sync data for {target_date}"
        logger.error(error_msg)
        raise Exception(error_msg)

    logger.info(f"Successfully synced data for {target_date}")
    return {"date": str(target_date), "success": True}


with DAG(
    dag_id="ohlcv_daily",
    description="Daily OHLCV data ingestion at market close",
    default_args=default_args,
    schedule="30 16 * * 1-5",  # 4:30 PM ET, Monday-Friday
    start_date=pendulum.datetime(2025, 1, 1, tz=timezone),
    catchup=False,
    max_active_runs=1,
    tags={"market-data", "daily", "ohlcv"},
) as dag:
    check_trading = ShortCircuitOperator(
        task_id="check_trading_day",
        python_callable=check_trading_day,
    )

    scrape_data = PythonOperator(
        task_id="incremental_scrape",
        python_callable=incremental_scrape,
    )

    sync_data = PythonOperator(
        task_id="sync_live_systems",
        python_callable=sync_live_systems,
    )

    check_trading >> scrape_data >> sync_data
