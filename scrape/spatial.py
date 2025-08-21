from pathlib import Path
from typing import Optional

import duckdb

from shared.config import settings


class SpatialDataManager:
    """Manages spatial data operations in DuckDB with spatial extension"""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or settings.duckdb_path_resolved

    def ensure_spatial_extension(self) -> bool:
        """Install and load spatial extension"""
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                # Install spatial extension
                try:
                    conn.execute("INSTALL spatial;")
                except Exception:
                    pass  # Already installed

                # Load spatial extension
                conn.execute("LOAD spatial;")

                # Verify spatial functions work
                conn.execute("SELECT ST_Point(0, 0);").fetchone()
                return True

        except Exception:
            return False

    def create_spatial_tables(self):
        """Create spatial tables and indexes"""
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                conn.execute("LOAD spatial;")

                # Update locations table to include spatial geometry
                conn.execute("DROP TABLE IF EXISTS locations_spatial;")
                conn.execute("""
                    CREATE TABLE locations_spatial AS
                    SELECT 
                        Ticker,
                        Name,
                        Address,
                        Latitude,
                        Longitude,
                        CASE 
                            WHEN Latitude IS NOT NULL AND Longitude IS NOT NULL 
                            THEN ST_Point(Longitude, Latitude)
                            ELSE NULL
                        END AS geom
                    FROM locations;
                """)

                # Create spatial index
                conn.execute(
                    "CREATE INDEX idx_locations_spatial_geom ON locations_spatial USING RTREE (geom);"
                )

        except Exception as e:
            print(f"Error creating spatial tables: {e}")

    def export_to_parquet(self, output_path: Optional[Path] = None) -> int:
        """Export spatial data to Parquet format"""
        try:
            if output_path is None:
                output_path = settings.geo_parquet_path
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with duckdb.connect(str(self.db_path)) as conn:
                conn.execute("LOAD spatial;")

                # Export locations with geometry as WKT for Parquet compatibility
                conn.execute(f"""
                    COPY (
                        SELECT 
                            Ticker,
                            Name,
                            Address,
                            Latitude,
                            Longitude,
                            ST_AsText(geom) as geometry_wkt
                        FROM locations_spatial
                        WHERE geom IS NOT NULL
                    ) TO '{output_path}' (FORMAT PARQUET);
                """)

                # Get row count
                result = conn.execute("""
                    SELECT COUNT(*) FROM locations_spatial WHERE geom IS NOT NULL
                """).fetchone()

                return result[0] if result else 0

        except Exception:
            return 0


def setup_spatial_database() -> bool:
    """Initialize spatial database setup"""
    print("Setting up spatial database...")

    manager = SpatialDataManager()

    if not manager.ensure_spatial_extension():
        print("Cannot load DuckDB spatial extension")
        return False

    print("DuckDB spatial extension loaded successfully")

    manager.create_spatial_tables()
    print("Spatial tables created successfully")

    return True


def export_spatial_data() -> int:
    """Export spatial data to Parquet format"""
    print("Exporting spatial data...")

    manager = SpatialDataManager()
    count = manager.export_to_parquet()

    if count > 0:
        print(
            f"Exported {count} spatial records to {settings.geo_parquet_path}"
        )
    else:
        print("No spatial records to export")

    return count


def main():
    """Main function for spatial data operations"""
    print("Starting spatial data processing...")

    if not setup_spatial_database():
        return

    export_spatial_data()

    print("Spatial processing complete!")


if __name__ == "__main__":
    main()
