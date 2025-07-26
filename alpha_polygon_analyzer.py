# Simplified AlphaVantage and Polygon.io integration
import asyncio
import logging
import aiosqlite
from typing import List
import aiohttp
import json

from config import get_config

logger = logging.getLogger(__name__)


class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def fetch_daily(self, session: aiohttp.ClientSession, symbol: str):
        params = {
            'function': 'TIME_SERIES_DAILY_ADJUSTED',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'
        }
        async with session.get(self.BASE_URL, params=params) as resp:
            return await resp.json()


class PolygonClient:
    BASE_URL = "https://api.polygon.io"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def fetch_snapshot(self, session: aiohttp.ClientSession, symbol: str):
        url = f"{self.BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        async with session.get(url, params={'apiKey': self.api_key}) as resp:
            return await resp.json()


class AlphaPolygonAnalyzer:
    def __init__(self, db_path: str = get_config("DB_PATH", "opus.db")):
        self.av_key = get_config('ALPHA_VANTAGE_KEY', '')
        self.poly_key = get_config('POLYGON_KEY', '')
        self.db_path = db_path

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                    symbol TEXT,
                    timestamp TEXT,
                    close REAL,
                    source TEXT,
                    PRIMARY KEY (symbol, timestamp, source)
                )
                """
            )
            await conn.commit()

    async def fetch_and_store(self, symbols: List[str]):
        av_client = AlphaVantageClient(self.av_key)
        poly_client = PolygonClient(self.poly_key)

        await self.init_db()
        async with aiohttp.ClientSession() as session:
            async with aiosqlite.connect(self.db_path) as conn:
                rows = []  # accumulate rows before bulk insert
                for sym in symbols:
                    av_data = await av_client.fetch_daily(session, sym)
                    snap = await poly_client.fetch_snapshot(session, sym)

                    if 'Time Series (Daily)' in av_data:
                        latest_date, daily = next(iter(av_data['Time Series (Daily)'].items()))
                        close = float(daily['4. close'])
                        rows.append((sym, latest_date, close, 'alpha_vantage'))

                    if snap.get('ticker'):
                        # store Polygon last trade timestamp and price
                        trade = snap['ticker'].get('lastTrade', {})
                        rows.append((sym, trade.get('t'), trade.get('p', 0), 'polygon'))

                    await asyncio.sleep(1)

                if rows:
                    await conn.executemany(
                        "INSERT OR REPLACE INTO prices VALUES (?, ?, ?, ?)",
                        rows,
                    )
                    await conn.commit()


async def main():
    analyzer = AlphaPolygonAnalyzer()
    await analyzer.fetch_and_store(['AAPL', 'MSFT'])


if __name__ == '__main__':
    asyncio.run(main())
