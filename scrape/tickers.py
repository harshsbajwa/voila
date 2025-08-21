import polars as pl
from pathlib import Path

from shared.config import TICKER_FILE

URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"


def fetch(url: str) -> pl.DataFrame:
    try:
        return pl.read_csv(url, separator="|")
    except Exception as e:
        print(f"Failed to download from {url}: {e}")
        raise


def filter(df: pl.DataFrame) -> list[str]:
    filtered_df = df.filter(
        (
            # exclude warrants, units, preferred shares, etc.
            pl.col("Security Name").str.contains("Common")
        )
    )
    tickers = filtered_df.get_column("Symbol").unique().sort().to_list()
    return tickers


def save(tickers: list[str], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(tickers))
    print(f"Saved {len(tickers)} common NASDAQ tickers to '{path}'")


if __name__ == "__main__":
    try:
        print("Fetching and filtering NASDAQ tickers...")
        raw = fetch(URL)
        tickers = filter(raw)
        save(tickers, TICKER_FILE)
    except Exception as e:
        print(f"An error occurred: {e}")
