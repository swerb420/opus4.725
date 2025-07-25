# Simplified CCXT data collector
import asyncio
import sqlite3
import logging
from typing import List
import ccxt.async_support as ccxt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CCXTDataCollector:
    def __init__(self, db_path: str = 'crypto_data.db'):
        self.db_path = db_path
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
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
            conn.commit()

    async def fetch_ohlcv(self, symbol: str):
        data = await self.exchange.fetch_ohlcv(symbol, '1h', limit=24)
        with sqlite3.connect(self.db_path) as conn:
            for candle in data:
                conn.execute(
                    'INSERT OR REPLACE INTO ohlcv VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (symbol, candle[0], candle[1], candle[2], candle[3], candle[4], candle[5])
                )
            conn.commit()

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
