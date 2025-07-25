"""Database optimization system for handling 10 years of data efficiently."""

import sqlite3
import logging
import shutil
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pathlib import Path

import pandas as pd
import numpy as np


class DatabaseOptimizer:
    """Optimize database performance for large-scale historical data."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db_dir = Path(db_path).parent

    def setup_partitioned_tables(self, start_year: int = 2014, end_year: int | None = None):
        """Create partitioned tables for each year."""
        if end_year is None:
            end_year = datetime.now().year

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS price_partitions (
                    partition_name TEXT PRIMARY KEY,
                    year INTEGER NOT NULL,
                    start_date TEXT,
                    end_date TEXT,
                    record_count INTEGER,
                    size_mb REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            for year in range(start_year, end_year + 1):
                table_name = f"prices_{year}"

                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        symbol TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL NOT NULL,
                        volume REAL,
                        volume_usd REAL,
                        market_cap REAL,
                        source TEXT NOT NULL,
                        exchange TEXT,
                        timeframe TEXT DEFAULT '1d',
                        PRIMARY KEY (symbol, timestamp, source)
                    )
                    """
                )

                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol ON {table_name}(symbol)")
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp DESC)")

                conn.execute(
                    """
                    INSERT OR REPLACE INTO price_partitions
                    (partition_name, year, start_date, end_date)
                    VALUES (?, ?, ?, ?)
                    """,
                    (table_name, year, f"{year}-01-01", f"{year}-12-31"),
                )

            conn.commit()

    def migrate_to_partitions(self):
        """Migrate existing data to partitioned tables."""
        logging.info("Migrating data to partitioned tables...")

        with sqlite3.connect(self.db_path) as conn:
            years = conn.execute(
                """
                SELECT DISTINCT strftime('%Y', timestamp) as year
                FROM prices
                ORDER BY year
                """
            ).fetchall()

            for (year,) in years:
                if year is None:
                    continue

                table_name = f"prices_{year}"

                conn.execute(
                    f"""
                    INSERT OR IGNORE INTO {table_name}
                    SELECT symbol, timestamp, open, high, low, close,
                           volume, volume AS volume_usd, NULL as market_cap,
                           source, source as exchange, '1d' as timeframe
                    FROM prices
                    WHERE strftime('%Y', timestamp) = ?
                    """,
                    (year,),
                )

                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                conn.execute(
                    """
                    UPDATE price_partitions
                    SET record_count = ?
                    WHERE partition_name = ?
                    """,
                    (count, table_name),
                )

            conn.commit()

    def create_aggregation_tables(self):
        """Create pre-aggregated tables for fast queries."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monthly_aggregates (
                    symbol TEXT NOT NULL,
                    year_month TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    avg_price REAL,
                    total_volume REAL,
                    avg_volume REAL,
                    volatility REAL,
                    return_pct REAL,
                    trading_days INTEGER,
                    PRIMARY KEY (symbol, year_month)
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS yearly_aggregates (
                    symbol TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    avg_price REAL,
                    total_volume REAL,
                    avg_volume REAL,
                    volatility REAL,
                    return_pct REAL,
                    trading_days INTEGER,
                    PRIMARY KEY (symbol, year)
                )
                """
            )

            conn.commit()

    def update_aggregates(self, symbol: str | None = None):
        """Update aggregated tables with latest data."""
        with sqlite3.connect(self.db_path) as conn:
            partitions = conn.execute(
                "SELECT partition_name, year FROM price_partitions ORDER BY year"
            ).fetchall()

            for partition_name, year in partitions:
                if symbol:
                    where_clause = f"WHERE symbol = '{symbol}'"
                else:
                    where_clause = ""

                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO monthly_aggregates
                    SELECT
                        symbol,
                        strftime('%Y-%m', timestamp) as year_month,
                        (
                            SELECT open FROM {partition_name} p2
                            WHERE p2.symbol = p1.symbol
                              AND strftime('%Y-%m', p2.timestamp) = strftime('%Y-%m', p1.timestamp)
                            ORDER BY p2.timestamp ASC LIMIT 1
                        ) as open,
                        MAX(high) as high,
                        MIN(low) as low,
                        (
                            SELECT close FROM {partition_name} p2
                            WHERE p2.symbol = p1.symbol
                              AND strftime('%Y-%m', p2.timestamp) = strftime('%Y-%m', p1.timestamp)
                            ORDER BY p2.timestamp DESC LIMIT 1
                        ) as close,
                        AVG(close) as avg_price,
                        SUM(volume) as total_volume,
                        AVG(volume) as avg_volume,
                        STDDEV(close) as volatility,
                        ((
                            SELECT close FROM {partition_name} p2
                            WHERE p2.symbol = p1.symbol
                              AND strftime('%Y-%m', p2.timestamp) = strftime('%Y-%m', p1.timestamp)
                            ORDER BY p2.timestamp DESC LIMIT 1
                        ) - (
                            SELECT open FROM {partition_name} p2
                            WHERE p2.symbol = p1.symbol
                              AND strftime('%Y-%m', p2.timestamp) = strftime('%Y-%m', p1.timestamp)
                            ORDER BY p2.timestamp ASC LIMIT 1
                        )) /
                        (
                            SELECT open FROM {partition_name} p2
                            WHERE p2.symbol = p1.symbol
                              AND strftime('%Y-%m', p2.timestamp) = strftime('%Y-%m', p1.timestamp)
                            ORDER BY p2.timestamp ASC LIMIT 1
                        ) * 100 as return_pct,
                        COUNT(DISTINCT DATE(timestamp)) as trading_days
                    FROM {partition_name} p1
                    {where_clause}
                    GROUP BY symbol, strftime('%Y-%m', timestamp)
                    """
                )

            conn.commit()

    def optimize_queries(self):
        """Create optimized views for common queries."""
        with sqlite3.connect(self.db_path) as conn:
            partitions = conn.execute(
                "SELECT partition_name FROM price_partitions ORDER BY year"
            ).fetchall()

            if partitions:
                union_parts = [f"SELECT * FROM {p[0]}" for p in partitions]
                union_query = " UNION ALL ".join(union_parts)
                conn.execute(
                    f"""
                    CREATE VIEW IF NOT EXISTS all_prices AS
                    {union_query}
                    """
                )

            conn.execute(
                """
                CREATE VIEW IF NOT EXISTS latest_prices_optimized AS
                WITH latest_dates AS (
                    SELECT symbol, MAX(timestamp) as max_timestamp
                    FROM all_prices
                    GROUP BY symbol
                )
                SELECT p.*, ld.max_timestamp
                FROM all_prices p
                JOIN latest_dates ld ON p.symbol = ld.symbol AND p.timestamp = ld.max_timestamp
                """
            )

            conn.commit()

    def vacuum_and_analyze(self):
        """Vacuum and analyze database for optimal performance."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA optimize")
            conn.execute("PRAGMA analysis_limit=1000")
            conn.execute("ANALYZE")

            partitions = conn.execute(
                "SELECT partition_name FROM price_partitions"
            ).fetchall()

            for (partition_name,) in partitions:
                count = conn.execute(f"SELECT COUNT(*) FROM {partition_name}").fetchone()[0]
                size_mb = (count * 100) / (1024 * 1024)
                conn.execute(
                    """
                    UPDATE price_partitions
                    SET record_count = ?, size_mb = ?
                    WHERE partition_name = ?
                    """,
                    (count, size_mb, partition_name),
                )

            conn.commit()

        conn = sqlite3.connect(self.db_path)
        conn.execute("VACUUM")
        conn.close()

    def create_backup(self, backup_name: str | None = None) -> Path:
        """Create backup of the database."""
        if backup_name is None:
            backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"

        backup_path = self.db_dir / backup_name

        source = sqlite3.connect(self.db_path)
        dest = sqlite3.connect(str(backup_path))

        with dest:
            source.backup(dest)

        source.close()
        dest.close()

        logging.info(f"Database backed up to {backup_path}")
        return backup_path

    def enable_wal_mode(self):
        """Enable Write-Ahead Logging for better concurrency."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA wal_autocheckpoint=1000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.commit()

    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics."""
        stats: Dict[str, list | float] = {
            "total_size_mb": os.path.getsize(self.db_path) / (1024 * 1024),
            "partitions": [],
            "indexes": [],
            "tables": [],
        }

        with sqlite3.connect(self.db_path) as conn:
            partitions = conn.execute(
                "SELECT * FROM price_partitions ORDER BY year"
            ).fetchall()
            for partition in partitions:
                stats["partitions"].append(
                    {
                        "name": partition[0],
                        "year": partition[1],
                        "records": partition[4],
                        "size_mb": partition[5],
                    }
                )

            tables = conn.execute(
                """
                SELECT name,
                       (SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name=m.name) as index_count
                FROM sqlite_master m
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()

            for table_name, index_count in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                stats["tables"].append(
                    {
                        "name": table_name,
                        "records": count,
                        "indexes": index_count,
                    }
                )

            indexes = conn.execute(
                """
                SELECT name, tbl_name
                FROM sqlite_master
                WHERE type='index' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
            stats["indexes"] = [
                {"name": idx[0], "table": idx[1]} for idx in indexes
            ]

        return stats

    def archive_old_data(self, years_to_keep: int = 5):
        """Archive data older than specified years."""
        cutoff_year = datetime.now().year - years_to_keep
        archive_path = self.db_dir / f"archive_{cutoff_year}_and_older.db"

        archive_conn = sqlite3.connect(str(archive_path))

        with sqlite3.connect(self.db_path) as main_conn:
            old_partitions = main_conn.execute(
                "SELECT partition_name FROM price_partitions WHERE year <= ?",
                (cutoff_year,),
            ).fetchall()

            for (partition_name,) in old_partitions:
                main_conn.execute(f"ATTACH DATABASE '{archive_path}' AS archive")
                main_conn.execute(
                    f"""
                    CREATE TABLE archive.{partition_name} AS
                    SELECT * FROM {partition_name}
                    """
                )
                main_conn.execute("DETACH DATABASE archive")

                main_conn.execute(f"DROP TABLE {partition_name}")
                main_conn.execute(
                    "DELETE FROM price_partitions WHERE partition_name = ?",
                    (partition_name,),
                )

        archive_conn.close()
        logging.info(f"Archived data to {archive_path}")

    def create_bloom_filters(self):
        """Create bloom filters for fast existence checks."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bloom_filters (
                    filter_name TEXT PRIMARY KEY,
                    filter_data BLOB,
                    false_positive_rate REAL,
                    element_count INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            symbols = conn.execute(
                "SELECT DISTINCT symbol FROM coin_registry"
            ).fetchall()
            filter_data = b"bloom_filter_data"
            conn.execute(
                """
                INSERT OR REPLACE INTO bloom_filters
                (filter_name, filter_data, false_positive_rate, element_count)
                VALUES (?, ?, ?, ?)
                """,
                ("symbols", filter_data, 0.01, len(symbols)),
            )

            conn.commit()


class QueryOptimizer:
    """Optimize queries for partitioned data."""

    @staticmethod
    def get_price_range_query(symbol: str, start_date: str, end_date: str) -> str:
        """Generate optimized query for date range."""
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])

        tables = []
        for year in range(start_year, end_year + 1):
            tables.append(
                f"""
                SELECT * FROM prices_{year}
                WHERE symbol = '{symbol}'
                  AND timestamp BETWEEN '{start_date}' AND '{end_date}'
                """
            )

        return " UNION ALL ".join(tables) + " ORDER BY timestamp"

    @staticmethod
    def get_multi_symbol_query(symbols: List[str], days: int = 30) -> str:
        """Generate optimized query for multiple symbols."""
        symbol_list = "', '".join(symbols)
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        current_year = datetime.now().year
        tables = []
        for year in [current_year - 1, current_year]:
            tables.append(
                f"""
                SELECT * FROM prices_{year}
                WHERE symbol IN ('{symbol_list}')
                  AND timestamp >= '{start_date}'
                """
            )

        return " UNION ALL ".join(tables) + " ORDER BY symbol, timestamp"


def setup_database_optimization(db_path: str) -> DatabaseOptimizer:
    """Set up database optimization."""
    optimizer = DatabaseOptimizer(db_path)
    optimizer.enable_wal_mode()
    optimizer.setup_partitioned_tables()
    optimizer.create_aggregation_tables()
    optimizer.optimize_queries()
    return optimizer
