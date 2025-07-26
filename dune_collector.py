# Simplified Dune Analytics collector based on previous long script
import asyncio
import logging
import sqlite3
import requests
import time
import json
from typing import Dict, List, Any

from config import get_config

logger = logging.getLogger(__name__)


class DuneAnalyticsCollector:
    """Fetch data from Dune Analytics queries."""

    BASE_URL = "https://api.dune.com/api/v1"

    def __init__(self, db_path: str = get_config("DB_PATH", "opus.db")):
        self.api_key = get_config("DUNE_API_KEY", "")
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dune_results (
                    query_id TEXT,
                    execution_id TEXT,
                    timestamp TEXT,
                    data JSON,
                    PRIMARY KEY (query_id, execution_id)
                )
                """
            )
            conn.commit()

    def execute_query(self, query_id: str) -> str:
        url = f"{self.BASE_URL}/query/{query_id}/execute"
        headers = {"X-Dune-API-Key": self.api_key, "Content-Type": "application/json"}
        try:
            resp = requests.post(url, headers=headers, json={"performance": "medium"})
            resp.raise_for_status()
            return resp.json().get("execution_id")
        except Exception as e:  # pragma: no cover - network issues
            logger.error(f"execute_query failed: {e}")
            return ""

    def get_results(self, execution_id: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/execution/{execution_id}/results"
        headers = {"X-Dune-API-Key": self.api_key}
        for _ in range(60):
            r = requests.get(f"{self.BASE_URL}/execution/{execution_id}/status", headers=headers)
            if r.ok and r.json().get("state") == "QUERY_STATE_COMPLETED":
                break
            time.sleep(5)
        r = requests.get(url, headers=headers)
        if r.ok:
            return r.json()
        return {}

    def store_results(self, query_id: str, execution_id: str, results: Dict[str, Any]):
        rows = results.get("result", {}).get("rows", [])
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO dune_results (query_id, execution_id, timestamp, data) VALUES (?, ?, datetime('now'), ?)",
                (query_id, execution_id, json.dumps(rows)),
            )
            conn.commit()

    def run_query(self, query_id: str):
        exec_id = self.execute_query(query_id)
        if not exec_id:
            return
        results = self.get_results(exec_id)
        if results:
            self.store_results(query_id, exec_id, results)
            logger.info(f"Stored results for {query_id}")


async def main(query_id: str):
    collector = DuneAnalyticsCollector()
    collector.run_query(query_id)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python dune_collector.py QUERY_ID")
    else:
        asyncio.run(main(sys.argv[1]))
