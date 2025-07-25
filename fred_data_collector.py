import asyncio
import datetime
import logging
import sqlite3
import aiosqlite
from typing import List, Dict, Optional
import aiohttp

from config import get_config

logger = logging.getLogger(__name__)


class FREDDataCollector:
    """Minimal FRED data collector storing 10 years of data."""

    BASE_URL = "https://api.stlouisfed.org/fred"

    # A small subset of common FRED series
    SERIES = {
        "GDP": "Gross Domestic Product",
        "CPIAUCSL": "Consumer Price Index",
        "UNRATE": "Unemployment Rate",
        "FEDFUNDS": "Effective Federal Funds Rate",
        "DGS10": "10-Year Treasury Rate",
    }

    def __init__(self, db_path: str = get_config("DB_PATH", "opus.db")):
        self.api_key = get_config("FRED_API_KEY", "")
        self.db_path = db_path

    async def init_db(self):
        """Create tables if they do not exist."""
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fred_series (
                    series_id TEXT PRIMARY KEY,
                    title TEXT
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fred_observations (
                    series_id TEXT,
                    date TEXT,
                    value REAL,
                    PRIMARY KEY (series_id, date)
                )
                """
            )
            await conn.commit()

    async def fetch_series_info(self, session: aiohttp.ClientSession, series_id: str) -> Dict:
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        url = f"{self.BASE_URL}/series"
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            if data.get("seriess"):
                return data["seriess"][0]
            return {}

    async def fetch_observations(self, session: aiohttp.ClientSession, series_id: str) -> List[Dict]:
        start = (datetime.datetime.now() - datetime.timedelta(days=3650)).strftime("%Y-%m-%d")
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start,
        }
        url = f"{self.BASE_URL}/series/observations"
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            return data.get("observations", [])

    async def _store_series_info(self, series_id: str, info: Dict):
        if not info:
            return
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO fred_series (series_id, title) VALUES (?, ?)",
                (series_id, info.get("title")),
            )
            await conn.commit()

    async def _store_observations(self, series_id: str, observations: List[Dict]):
        if not observations:
            return
        rows = []
        for obs in observations:
            value = obs.get("value")
            date = obs.get("date")
            if not value or value == ".":
                continue
            try:
                val = float(value)
            except ValueError:
                continue
            rows.append((series_id, date, val))

        if rows:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.executemany(
                    "INSERT OR REPLACE INTO fred_observations (series_id, date, value) VALUES (?, ?, ?)",
                    rows,
                )
                await conn.commit()

    async def fetch_all_series_data(self, series_ids: Optional[List[str]] = None):
        if series_ids is None:
            series_ids = list(self.SERIES.keys())
        await self.init_db()
        async with aiohttp.ClientSession() as session:
            for sid in series_ids:
                logger.info(f"Fetching {sid}")
                info = await self.fetch_series_info(session, sid)
                await self._store_series_info(sid, info)
                observations = await self.fetch_observations(session, sid)
                await self._store_observations(sid, observations)
                await asyncio.sleep(1)  # basic rate limiting
        logger.info("Completed FRED data fetch")


async def main(db_path: str = "fred_data.db"):
    collector = FREDDataCollector(db_path)
    await collector.fetch_all_series_data()


if __name__ == "__main__":
    asyncio.run(main())
