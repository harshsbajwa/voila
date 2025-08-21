import os

# force yfinance to be single-threaded
# avoids deadlocks with the ThreadPoolExecutor
# must be set before yfinance is imported
os.environ["YFINANCE_MAX_THREADS"] = "1"

import subprocess
import threading
import multiprocessing
from datetime import datetime, date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import time
import random

import yfinance as yf
import polars as pl
import pandas as pd
import duckdb
from tqdm import tqdm
from colorama import Fore, Style
import backoff


from shared.config import (
    SCRAPE_START_DATE,
    TICKER_FILE,
    TICKER_SCRIPT,
    DUCKDB_PATH,
    DUCKDB_TABLE_NAME,
)

# ratelimit range
RATE_UPPER = 0.5
RATE_LOWER = 0.1

END_DATE = datetime.now().strftime("%Y-%m-%d")

THREADS = min(max(multiprocessing.cpu_count(), 2), 8)
EXPECTED_COLS = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Adj_Close",
    "Volume",
    "Dividends",
    "Stock_Splits",
    "Ticker",
]


def load() -> list[str]:
    if not TICKER_FILE.exists():
        print(f"'{TICKER_FILE}' not found. Running '{TICKER_SCRIPT}'...")
        result = subprocess.run(
            ["python", str(TICKER_SCRIPT)], capture_output=True, text=True
        )
        if result.returncode != 0:
            print(
                f"{Fore.RED}Failed to run ticker scraper:\n{result.stderr}{Style.RESET_ALL}"
            )
            exit(1)

    with open(TICKER_FILE, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


def init(db_path: Path):
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DUCKDB_TABLE_NAME} (
                Date TIMESTAMP,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Adj_Close DOUBLE,
                Volume BIGINT,
                Dividends DOUBLE,
                Stock_Splits DOUBLE,
                Ticker VARCHAR
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                Ticker VARCHAR PRIMARY KEY,
                Name VARCHAR,
                Address VARCHAR,
                Latitude DOUBLE,
                Longitude DOUBLE
            );
        """)


# use tls
thread_local = threading.local()


def conn_tls() -> duckdb.DuckDBPyConnection:
    if not hasattr(thread_local, "conn"):
        thread_local.conn = duckdb.connect(str(DUCKDB_PATH))
    return thread_local.conn


@backoff.on_exception(
    backoff.expo, Exception, max_tries=3, jitter=backoff.full_jitter
)
def download(ticker: str) -> tuple[str, bool | str]:
    # ratelimit
    time.sleep(random.uniform(RATE_LOWER, RATE_UPPER))

    # yf returns pandas df
    df_pd = yf.download(
        ticker,
        start=SCRAPE_START_DATE,
        end=END_DATE,
        progress=False,
        auto_adjust=False,
        threads=False,
        timeout=30,
    )

    # flatten multiindex cols
    if isinstance(df_pd.columns, pd.MultiIndex):
        df_pd.columns = df_pd.columns.get_level_values(0)

    # remove duplicate col names before converting to polars
    df_pd = df_pd.loc[:, ~df_pd.columns.duplicated()]

    if df_pd.empty:
        return ticker, "No data returned from yfinance"

    # convert stinky pandas df to clean polars immediately
    df_pl = pl.from_pandas(df_pd.reset_index())

    # batch conditional rename from yf cols to fit schema
    rename_map = {}
    if "Adj Close" in df_pl.columns:
        rename_map["Adj Close"] = "Adj_Close"
    if "Stock Splits" in df_pl.columns:
        rename_map["Stock Splits"] = "Stock_Splits"
    if rename_map:
        df_pl = df_pl.rename(rename_map)
    df_pl = df_pl.with_columns(pl.lit(ticker).alias("Ticker"))

    # batch create nulls/zeroes if missing
    existing_cols = set(df_pl.columns)
    cols_to_add = []
    for col in EXPECTED_COLS:
        if col not in existing_cols:
            dtype = pl.Float64
            if col == "Volume":
                dtype = pl.Int64
            cols_to_add.append(pl.lit(0, dtype=dtype).alias(col))
    if cols_to_add:
        df_pl = df_pl.with_columns(cols_to_add)

    # remove if ohlcv cols all zero
    price_cols = ["Open", "High", "Low", "Close", "Volume"]
    df_clean = df_pl.filter(
        ~pl.all_horizontal(pl.col(p).fill_null(0) == 0 for p in price_cols)
    ).select(EXPECTED_COLS)

    if df_clean.height == 0:
        return ticker, "Data became empty after cleaning (all zero rows)"

    # write to duckdb
    conn = conn_tls()
    conn.execute(f"INSERT INTO {DUCKDB_TABLE_NAME} SELECT * FROM df_clean")

    return ticker, True


def fill(tickers: list[str]):
    success_count = 0
    failed_tickers = {}

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        # futures dict to track tickers
        future_to_ticker = {
            executor.submit(download, ticker): ticker for ticker in tickers
        }

        pbar = tqdm(
            as_completed(future_to_ticker),
            total=len(tickers),
            ncols=80,
            desc="📡 Downloading",
            bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.CYAN, Style.RESET_ALL),
        )

        for future in pbar:
            try:
                ticker, result = future.result()
                if result is True:
                    success_count += 1
                else:
                    failed_tickers[ticker] = str(result)
            except Exception as e:
                ticker = future_to_ticker[future]
                failed_tickers[ticker] = f"Error: {e}"

            pbar.set_postfix({"✓": success_count, "✗": len(failed_tickers)})

    if hasattr(thread_local, "conn"):
        thread_local.conn.close()

    return failed_tickers


@backoff.on_exception(
    backoff.expo, Exception, max_tries=3, jitter=backoff.full_jitter
)
def download_one_day(ticker: str, target_date: date) -> tuple[str, bool | str]:
    time.sleep(random.uniform(RATE_LOWER, RATE_UPPER))

    start_date = target_date
    end_date = target_date + timedelta(days=1)

    df_pd = yf.download(
        ticker,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=False,
        threads=False,
        timeout=30,
    )

    if isinstance(df_pd.columns, pd.MultiIndex):
        df_pd.columns = df_pd.columns.get_level_values(0)

    df_pd = df_pd.loc[:, ~df_pd.columns.duplicated()]

    if df_pd.empty:
        return ticker, f"No data for {target_date}"

    df_pl = pl.from_pandas(df_pd.reset_index())

    rename_map = {}
    if "Adj Close" in df_pl.columns:
        rename_map["Adj Close"] = "Adj_Close"
    if "Stock Splits" in df_pl.columns:
        rename_map["Stock Splits"] = "Stock_Splits"
    if rename_map:
        df_pl = df_pl.rename(rename_map)
    df_pl = df_pl.with_columns(pl.lit(ticker).alias("Ticker"))

    existing_cols = set(df_pl.columns)
    cols_to_add = []
    for col in EXPECTED_COLS:
        if col not in existing_cols:
            dtype = pl.Float64
            if col == "Volume":
                dtype = pl.Int64
            cols_to_add.append(pl.lit(0, dtype=dtype).alias(col))
    if cols_to_add:
        df_pl = df_pl.with_columns(cols_to_add)

    price_cols = ["Open", "High", "Low", "Close", "Volume"]
    df_clean = df_pl.filter(
        ~pl.all_horizontal(pl.col(p).fill_null(0) == 0 for p in price_cols)
    ).select(EXPECTED_COLS)

    if df_clean.height == 0:
        return ticker, f"All zero data for {target_date}"

    conn = conn_tls()
    conn.execute(f"INSERT INTO {DUCKDB_TABLE_NAME} SELECT * FROM df_clean")

    return ticker, True


def incremental_fill_for_date(
    target_date: date, tickers: Optional[list[str]] = None
) -> dict[str, str]:
    if tickers is None:
        tickers = load()

    init(DUCKDB_PATH)

    with duckdb.connect(str(DUCKDB_PATH)) as conn:
        conn.execute(
            f"DELETE FROM {DUCKDB_TABLE_NAME} WHERE DATE(Date) = DATE(?)",
            parameters=[target_date.strftime("%Y-%m-%d")],
        )
        print(f"Cleared existing data for {target_date}")

    print(
        f"Starting incremental scrape for {target_date} with {len(tickers)} tickers"
    )
    success_count = 0
    failed_tickers = {}

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_ticker = {
            executor.submit(download_one_day, ticker, target_date): ticker
            for ticker in tickers
        }

        completed = 0
        for future in as_completed(future_to_ticker):
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{len(tickers)} tickers processed")

            try:
                ticker, result = future.result()
                if result is True:
                    success_count += 1
                else:
                    failed_tickers[ticker] = str(result)
            except Exception as e:
                ticker = future_to_ticker[future]
                failed_tickers[ticker] = f"Error: {e}"

    if hasattr(thread_local, "conn"):
        thread_local.conn.close()

    print(
        f"Incremental scrape completed: {success_count} success, {len(failed_tickers)} failed"
    )

    return failed_tickers


if __name__ == "__main__":
    tickers = load()
    init(DUCKDB_PATH)

    print(
        f"Using {THREADS} threads to download {len(tickers)} tickers (End Date: {END_DATE})"
    )

    failed = fill(tickers)

    print("\n--- Download Complete ---")
    if not failed:
        print(
            f"{Fore.GREEN}All tickers processed successfully.{Style.RESET_ALL}"
        )
    else:
        print(
            f"{Fore.YELLOW}Finished with {len(failed)} failed tickers.{Style.RESET_ALL}"
        )
        print("First 10 failures:")
        for i, (ticker, reason) in enumerate(list(failed.items())[:10]):
            print(f"  - {ticker}: {reason}")

    print(f"\nDuckDB database is at: {DUCKDB_PATH.resolve()}")
