# Simplified CCXT data collector
import asyncio
import aiosqlite
import logging
from typing import List
import ccxt.async_support as ccxt

logger = logging.getLogger(__name__)

class CCXTDataCollector:
    def __init__(self, db_path: str = 'crypto_data.db'):
        self.db_path = db_path
        self.exchange = ccxt.binance({'enableRateLimit': True})

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS ohlcv (
                    symbol TEXT,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    PRIMARY KEY (symbol, timestamp)
                )"""
            )
            await conn.commit()

    async def fetch_ohlcv(self, symbol: str):
        await self.init_db()
        data = await self.exchange.fetch_ohlcv(symbol, '1h', limit=24)
        async with aiosqlite.connect(self.db_path) as conn:
            for candle in data:
                await conn.execute(
                    'INSERT OR REPLACE INTO ohlcv VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (symbol, candle[0], candle[1], candle[2], candle[3], candle[4], candle[5])
                )
            await conn.commit()

    async def close(self):
        await self.exchange.close()


async def main():
    collector = CCXTDataCollector()
    try:
        await collector.fetch_ohlcv('BTC/USDT')
    finally:
        await collector.close()

if __name__ == '__main__':
    asyncio.run(main())
