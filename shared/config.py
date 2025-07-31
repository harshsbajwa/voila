from pathlib import Path

# Project Structure
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# DuckDB
DUCKDB_PATH = DATA_DIR / "ohlcv.duckdb"
DUCKDB_TABLE_NAME = "ohlcv"

# QuestDB
QUESTDB_HOST = "127.0.0.1"
QUESTDB_ILP_PORT = 9009
QUESTDB_TABLE_NAME = "ohlcv"

# Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_OHLCV_TOPIC = "ohlcv-data"
KAFKA_CONSUMER_GROUP = "questdb-ingester-group"

# Scraper
TICKER_FILE = DATA_DIR / "tickers.txt"
TICKER_SCRIPT = "scrape/tickers.py"
SCRAPE_START_DATE = "2000-01-01"

# Pipeline and Batching

## QuestDB Hydration
HYDRATE_BATCH_SIZE = 5_000

# Consumer Batch
CONSUMER_BATCH_SIZE = 1_000

# Consumer Partial Batch Timeout
CONSUMER_BATCH_TIMEOUT_S = 5.0
