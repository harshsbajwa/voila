from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support"""

    # Project Structure
    project_root: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent
    )

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    # DuckDB Configuration (Staging Only)
    duckdb_path: Optional[Path] = None
    duckdb_table_name: str = "ohlcv"

    @property
    def duckdb_path_resolved(self) -> Path:
        return self.duckdb_path or self.data_dir / "voila.duckdb"

    # QuestDB Configuration (Primary Live Database)
    questdb_host: str = "127.0.0.1"
    questdb_pg_port: int = 8812  # PostgreSQL wire protocol port for FastAPI
    questdb_ilp_port: int = 9009  # InfluxDB line protocol port for ingestion
    questdb_http_port: int = 9000  # HTTP port for admin
    questdb_user: str = "admin"
    questdb_password: str = "quest"
    questdb_database: str = "qdb"

    # QuestDB Tables
    questdb_ohlcv_table: str = "ohlcv"
    questdb_companies_table: str = "companies"

    # Redis Configuration (Geospatial Index + Caching)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    redis_geo_key: str = "companies:geo"  # Geospatial index key
    redis_cache_ttl: int = 3600  # Cache TTL in seconds

    # External API Configuration
    census_api_key: Optional[str] = None
    edgar_user_agent: str = "voila-market-scraper/1.0"
    edgar_contact_email: str = "hsbajwah@gmail.com"

    # Scraper Configuration
    ticker_script: str = "scrape/tickers.py"
    scrape_start_date: str = "2024-01-01"

    @property
    def ticker_file(self) -> Path:
        return self.data_dir / "tickers.txt"

    # Rate Limiting Configuration
    api_rate_limit_per_minute: int = 100
    edgar_rate_delay: float = 0.15
    edgar_rate_jitter: float = 0.05
    census_rate_delay: float = 0.5
    census_rate_jitter: float = 0.1

    # Pipeline and Batching Configuration
    questdb_batch_size: int = 5_000  # For QuestDB ingestion
    redis_batch_size: int = 1_000  # For Redis geospatial updates
    consumer_batch_size: int = 1_000
    consumer_batch_timeout_s: float = 5.0
    geocoding_batch_size: int = 10
    edgar_batch_size: int = 50

    # Threading Configuration
    max_scraping_threads: int = 8
    max_geocoding_threads: int = 3
    max_edgar_threads: int = 3

    # Geospatial Configuration
    @property
    def geojson_output_path(self) -> Path:
        return self.data_dir / "geojson" / "locations.geojson"

    @property
    def geo_parquet_path(self) -> Path:
        return self.data_dir / "geo" / "locations.parquet"

    # Spatial Query Defaults
    default_radius_km: float = 50.0
    max_radius_km: float = 1000.0
    max_spatial_results: int = 1000

    # Environment Configuration
    env: str = "development"  # development, staging, production
    admin_api_key: Optional[str] = (
        None  # Required for admin endpoints in production
    )

    # FastAPI Configuration
    api_title: str = "Voila Market Data API"
    api_version: str = "2.0.0"
    api_description: str = (
        "Time-series market data API with geospatial capabilities"
    )
    cors_origins: list = [
        "http://localhost:5173",  # Vite dev server
        "http://localhost:5174",  # Vite preview server
        "http://localhost:3000",  # Alternative dev port
        "http://localhost:8000",  # Backend itself
        "https://voila.amneet.me",  # Production frontend
        "https://api.amneet.me",  # Production API
        "*",  # Allow all in dev; remove for production
    ]

    # Database Connection Pooling
    questdb_pool_min_conn: int = 2
    questdb_pool_max_conn: int = 20
    redis_connection_pool_size: int = 10

    # Database Query Timeouts
    db_query_timeout_sec: float = 3.0  # Default timeout for DB queries
    db_query_timeout_long_sec: float = (
        10.0  # Longer timeout for complex queries
    )

    # Kafka Configuration (if needed)
    kafka_broker: str = "localhost:9092"
    kafka_ohlcv_topic: str = "ohlcv-data"
    kafka_consumer_group: str = "questdb-ingester-group"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        env_prefix = "VOILA_"


# Global settings instance
settings = Settings()

# Backward compatibility exports for existing code
PROJECT_ROOT = settings.project_root
DATA_DIR = settings.data_dir
DUCKDB_PATH = settings.duckdb_path_resolved
DUCKDB_TABLE_NAME = settings.duckdb_table_name
QUESTDB_HOST = settings.questdb_host
QUESTDB_ILP_PORT = settings.questdb_ilp_port
QUESTDB_TABLE_NAME = settings.questdb_ohlcv_table
KAFKA_BROKER = settings.kafka_broker
KAFKA_OHLCV_TOPIC = settings.kafka_ohlcv_topic
KAFKA_CONSUMER_GROUP = settings.kafka_consumer_group
TICKER_FILE = settings.ticker_file
TICKER_SCRIPT = settings.ticker_script
SCRAPE_START_DATE = settings.scrape_start_date
HYDRATE_BATCH_SIZE = settings.questdb_batch_size
CONSUMER_BATCH_SIZE = settings.consumer_batch_size
CONSUMER_BATCH_TIMEOUT_S = settings.consumer_batch_timeout_s
GEOJSON_OUTPUT_PATH = settings.geojson_output_path
GEO_PARQUET_PATH = settings.geo_parquet_path
