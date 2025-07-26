import logging
import sqlite3
from pathlib import Path

from config import get_config
from database_optimizer import setup_database_optimization


def initialize_database(db_path: str):
    schema_file = Path(__file__).with_name("database_schema.sql")
    if schema_file.exists():
        with sqlite3.connect(db_path) as conn, open(schema_file, "r") as f:
            conn.executescript(f.read())
            conn.commit()
    setup_database_optimization(db_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    db = get_config("DB_PATH", "opus.db")
    initialize_database(db)
