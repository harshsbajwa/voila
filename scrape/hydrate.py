import socket
import time
from datetime import datetime, timezone

import duckdb
import polars as pl
from tqdm import tqdm

from shared.config import (
    QUESTDB_HOST,
    QUESTDB_ILP_PORT,
    DUCKDB_PATH,
    HYDRATE_BATCH_SIZE,
)


def send(payload: str):
    """Sends a string of ILP data to QuestDB over a TCP socket."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((QUESTDB_HOST, QUESTDB_ILP_PORT))
            sock.sendall(payload.encode("utf-8"))
    except ConnectionRefusedError:
        print(
            "ERROR: Connection refused. Is QuestDB running and port 9009 exposed?"
        )
        exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while sending data: {e}")
        exit(1)


def main():
    """
    Main function to read all data from DuckDB and hydrate QuestDB.
    """
    if not DUCKDB_PATH.exists():
        print(f"ERROR: DuckDB database not found at '{DUCKDB_PATH}'.")
        print("Please run the initial backfill script to create it first.")
        return

    print(f"Connecting to DuckDB at '{DUCKDB_PATH}'...")
    con = duckdb.connect(database=str(DUCKDB_PATH), read_only=True)

    print("Reading all OHLCV data into memory...")
    df = con.execute("SELECT * FROM ohlcv ORDER BY Ticker, Date").pl()
    print(f"Loaded {len(df):,} records.")

    batch = []
    start_time = time.time()

    print(
        f"Starting hydration to QuestDB (Host: {QUESTDB_HOST}:{QUESTDB_ILP_PORT})..."
    )

    for row in tqdm(
        df.iter_rows(named=True), total=df.height, desc="Formatting to ILP"
    ):
        try:
            tags = f"Ticker={row['Ticker']}"

            fields = []
            for col, val in row.items():
                if val is None or col in ["Ticker", "Date"]:
                    continue

                if isinstance(val, (int, float)):
                    suffix = "i" if col == "Volume" else ""
                    fields.append(f"{col}={val}{suffix}")

            if not fields:
                continue

            # timestamp in unix time (ns), UTC
            dt_obj: datetime = row["Date"]
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            timestamp_ns = int(dt_obj.timestamp() * 1_000_000_000)

            # format: table_name,tags fields timestamp_ns
            ilp_line = f"ohlcv,{tags} {','.join(fields)} {timestamp_ns}"
            batch.append(ilp_line)

            # batch
            if len(batch) >= HYDRATE_BATCH_SIZE:
                send("\n".join(batch) + "\n")
                batch = []

        except Exception as e:
            print(f"\nWARNING: Could not process row: {row}. Error: {e}")

    # send final partial batch if any records are left
    if batch:
        send("\n".join(batch) + "\n")

    end_time = time.time()
    print(
        f"Processed and sent {len(df):,} records in {end_time - start_time:.2f} seconds."
    )


if __name__ == "__main__":
    main()
