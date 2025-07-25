"""Historical data collection system for building 10-year database."""

import asyncio
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import ccxt.async_support as ccxt
from tqdm import tqdm
import aiohttp
import json

class HistoricalDataCollector:
    """Collect and manage 10 years of historical crypto data."""
    
    def __init__(self, db_path: str, coin_manager):
        self.db_path = db_path
        self.coin_manager = coin_manager
        self.init_db()
        
        self.historical_sources = {
            'ccxt': {
                'max_history_days': 3650,
                'rate_limit': 50,
                'batch_size': 1000
            },
            'coingecko': {
                'max_history_days': 3650,
                'rate_limit': 10,
                'batch_size': 365
            },
            'messari': {
                'max_history_days': 1825,
                'rate_limit': 20,
                'batch_size': 500
            }
        }
    
    def init_db(self):
        """Initialize historical data tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS historical_progress (
                    symbol TEXT,
                    source TEXT,
                    data_type TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    last_fetch TEXT,
                    records_count INTEGER,
                    status TEXT,
                    PRIMARY KEY (symbol, source, data_type)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS data_quality (
                    symbol TEXT,
                    date TEXT,
                    completeness_score REAL,
                    gaps_count INTEGER,
                    sources_count INTEGER,
                    anomalies_count INTEGER,
                    PRIMARY KEY (symbol, date)
                )
            ''')
    
    async def collect_historical_data(self, symbols: List[str] = None, 
                                    start_date: str = '2014-01-01',
                                    end_date: str = None):
        """Collect historical data for all tracked coins."""
        if symbols is None:
            symbols = list(self.coin_manager.tracked_coins)
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logging.info(f"Starting historical collection for {len(symbols)} coins from {start_date} to {end_date}")
        
        total_tasks = len(symbols) * len(self.historical_sources)
        progress_bar = tqdm(total=total_tasks, desc="Historical Data Collection")
        
        for source in self.historical_sources:
            if source == 'ccxt':
                await self._collect_ccxt_historical(symbols, start_date, end_date, progress_bar)
            elif source == 'coingecko':
                await self._collect_coingecko_historical(symbols, start_date, end_date, progress_bar)
            elif source == 'messari':
                await self._collect_messari_historical(symbols, start_date, end_date, progress_bar)
        
        progress_bar.close()
        
        self._assess_data_quality(symbols, start_date, end_date)
    
    async def _collect_ccxt_historical(self, symbols: List[str], start_date: str, 
                                      end_date: str, progress_bar):
        """Collect historical data from CCXT exchanges."""
        exchanges_config = {
            'binance': {'timeframes': ['1d', '4h', '1h']},
            'coinbase': {'timeframes': ['1d', '6h']},
            'kraken': {'timeframes': ['1d', '4h']},
            'bybit': {'timeframes': ['1d']},
            'kucoin': {'timeframes': ['1d', '1h']}
        }
        
        for exchange_id, config in exchanges_config.items():
            try:
                exchange = getattr(ccxt, exchange_id)({
                    'enableRateLimit': True,
                    'rateLimit': self.historical_sources['ccxt']['rate_limit']
                })
                
                await exchange.load_markets()
                
                for symbol in symbols:
                    pairs = self._find_trading_pairs(exchange, symbol)
                    
                    for pair in pairs[:2]:
                        for timeframe in config['timeframes']:
                            await self._fetch_ohlcv_history(
                                exchange, pair, timeframe, start_date, end_date
                            )
                    
                    progress_bar.update(1)
                
                await exchange.close()
                
            except Exception as e:
                logging.error(f"Error with {exchange_id}: {e}")
    
    def _find_trading_pairs(self, exchange, base_symbol: str) -> List[str]:
        """Find available trading pairs for a symbol."""
        pairs = []
        quote_currencies = ['USDT', 'USD', 'USDC', 'BTC', 'ETH', 'BNB']
        
        for quote in quote_currencies:
            pair = f"{base_symbol}/{quote}"
            if pair in exchange.symbols:
                pairs.append(pair)
        
        return pairs
    
    async def _fetch_ohlcv_history(self, exchange, symbol: str, timeframe: str,
                                  start_date: str, end_date: str):
        """Fetch OHLCV history for a symbol."""
        try:
            since = int(pd.Timestamp(start_date).timestamp() * 1000)
            end = int(pd.Timestamp(end_date).timestamp() * 1000)

            all_candles = []
            records_saved = 0
            
            while since < end:
                candles = await exchange.fetch_ohlcv(
                    symbol, timeframe, since, 
                    limit=self.historical_sources['ccxt']['batch_size']
                )
                
                if not candles:
                    break
                
                all_candles.extend(candles)
                
                since = candles[-1][0] + 1
                
                if len(all_candles) >= 10000:
                    self._store_ohlcv_data(exchange.id, symbol, timeframe, all_candles)
                    records_saved += len(all_candles)
                    all_candles = []
                
                await asyncio.sleep(exchange.rateLimit / 1000)
            
            if all_candles:
                self._store_ohlcv_data(exchange.id, symbol, timeframe, all_candles)
                records_saved += len(all_candles)

            self._update_progress(
                symbol.split('/')[0], exchange.id, 'ohlcv',
                start_date, end_date, records_saved
            )
            
        except Exception as e:
            logging.error(f"Error fetching {symbol} from {exchange.id}: {e}")
    
    def _store_ohlcv_data(self, exchange: str, symbol: str, timeframe: str, candles: List):
        """Store OHLCV data in database."""
        with sqlite3.connect(self.db_path) as conn:
            for candle in candles:
                timestamp, open_, high, low, close, volume = candle
                conn.execute('''
                    INSERT OR REPLACE INTO prices
                    (symbol, timestamp, open, high, low, close, volume, 
                     source, asset_type, adjusted_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol.split('/')[0],
                    datetime.fromtimestamp(timestamp / 1000).isoformat(),
                    open_, high, low, close, volume,
                    f"{exchange}_{timeframe}", 'crypto', close
                ))
            conn.commit()
    
    async def _collect_coingecko_historical(self, symbols: List[str], start_date: str,
                                          end_date: str, progress_bar):
        """Collect historical data from CoinGecko (free tier)."""
        base_url = "https://api.coingecko.com/api/v3"
        
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    coin_id = await self._get_coingecko_id(session, symbol)
                    if not coin_id:
                        continue
                    
                    url = f"{base_url}/coins/{coin_id}/market_chart/range"
                    params = {
                        'vs_currency': 'usd',
                        'from': int(pd.Timestamp(start_date).timestamp()),
                        'to': int(pd.Timestamp(end_date).timestamp())
                    }
                    
                    async with session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self._store_coingecko_data(symbol, data)
                    
                    await asyncio.sleep(6)
                    
                except Exception as e:
                    logging.error(f"CoinGecko error for {symbol}: {e}")
                
                progress_bar.update(1)
    
    async def _get_coingecko_id(self, session, symbol: str) -> Optional[str]:
        """Map symbol to CoinGecko ID."""
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT metadata FROM tracked_coins WHERE symbol = ?",
                (symbol,)
            ).fetchone()
            
            if result and result[0]:
                metadata = json.loads(result[0])
                if 'coingecko_id' in metadata:
                    return metadata['coingecko_id']
        
        url = "https://api.coingecko.com/api/v3/coins/list"
        async with session.get(url) as resp:
            if resp.status == 200:
                coins = await resp.json()
                for coin in coins:
                    if coin['symbol'].upper() == symbol:
                        self._update_coin_metadata(symbol, {'coingecko_id': coin['id']})
                        return coin['id']
        
        return None
    
    def _store_coingecko_data(self, symbol: str, data: Dict):
        """Store CoinGecko historical data."""
        with sqlite3.connect(self.db_path) as conn:
            if 'prices' in data:
                for timestamp, price in data['prices']:
                    conn.execute('''
                        INSERT OR REPLACE INTO prices
                        (symbol, timestamp, close, source, asset_type)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        symbol,
                        datetime.fromtimestamp(timestamp / 1000).isoformat(),
                        price, 'coingecko', 'crypto'
                    ))
            
            if 'market_caps' in data:
                for timestamp, market_cap in data['market_caps']:
                    conn.execute('''
                        INSERT OR REPLACE INTO indicators
                        (series, timestamp, value, source, unit)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        f"{symbol}_MARKET_CAP",
                        datetime.fromtimestamp(timestamp / 1000).isoformat(),
                        market_cap, 'coingecko', 'USD'
                    ))
            
            if 'total_volumes' in data:
                for timestamp, volume in data['total_volumes']:
                    conn.execute('''
                        UPDATE prices
                        SET volume = ?
                        WHERE symbol = ? AND timestamp = ? AND source = ?
                    ''', (
                        volume, symbol,
                        datetime.fromtimestamp(timestamp / 1000).isoformat(),
                        'coingecko'
                    ))
            
            conn.commit()
    
    async def _collect_messari_historical(self, symbols: List[str], start_date: str,
                                        end_date: str, progress_bar):
        """Collect historical data from Messari (if API key available)."""
        messari_key = get_config("MESSARI_API_KEY", "")
        if not messari_key:
            progress_bar.update(len(symbols))
            return
        
        base_url = "https://data.messari.io/api/v1"
        headers = {"x-messari-api-key": messari_key}
        
        async with aiohttp.ClientSession(headers=headers) as session:
            for symbol in symbols:
                try:
                    url = f"{base_url}/assets/{symbol.lower()}/metrics/price/time-series"
                    params = {
                        'start': start_date,
                        'end': end_date,
                        'interval': '1d'
                    }
                    
                    async with session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self._store_messari_data(symbol, data)
                    
                    await asyncio.sleep(3)
                    
                except Exception as e:
                    logging.error(f"Messari error for {symbol}: {e}")
                
                progress_bar.update(1)
    
    def _store_messari_data(self, symbol: str, data: Dict):
        """Store Messari historical data."""
        if 'data' not in data or 'values' not in data['data']:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            for point in data['data']['values']:
                timestamp, open_, high, low, close, volume = point[:6]
                conn.execute('''
                    INSERT OR REPLACE INTO prices
                    (symbol, timestamp, open, high, low, close, volume, 
                     source, asset_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol,
                    datetime.fromtimestamp(timestamp / 1000).isoformat(),
                    open_, high, low, close, volume,
                    'messari', 'crypto'
                ))
            conn.commit()
    
    def _update_progress(self, symbol: str, source: str, data_type: str,
                        start_date: str, end_date: str, records_count: int):
        """Update historical data collection progress."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO historical_progress
                (symbol, source, data_type, start_date, end_date, 
                 last_fetch, records_count, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol, source, data_type, start_date, end_date,
                datetime.now().isoformat(), records_count, 'completed'
            ))
            conn.commit()
    
    def _update_coin_metadata(self, symbol: str, metadata_update: Dict):
        """Update coin metadata."""
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT metadata FROM tracked_coins WHERE symbol = ?",
                (symbol,)
            ).fetchone()
            
            if result:
                metadata = json.loads(result[0]) if result[0] else {}
                metadata.update(metadata_update)
                
                conn.execute(
                    "UPDATE tracked_coins SET metadata = ? WHERE symbol = ?",
                    (json.dumps(metadata), symbol)
                )
                conn.commit()
    
    def _assess_data_quality(self, symbols: List[str], start_date: str, end_date: str):
        """Assess data quality and identify gaps."""
        logging.info("Assessing data quality...")
        
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        expected_days = (end - start).days
        
        with sqlite3.connect(self.db_path) as conn:
            for symbol in symbols:
                query = '''
                    SELECT 
                        DATE(timestamp) as date,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT source) as source_count
                    FROM prices
                    WHERE symbol = ?
                    AND timestamp BETWEEN ? AND ?
                    GROUP BY DATE(timestamp)
                '''
                
                df = pd.read_sql_query(
                    query, conn, 
                    params=(symbol, start_date, end_date)
                )
                
                if df.empty:
                    completeness = 0
                    gaps = expected_days
                else:
                    actual_days = len(df)
                    completeness = actual_days / expected_days
                    
                    date_range = pd.date_range(start, end, freq='D')
                    existing_dates = pd.to_datetime(df['date'])
                    missing_dates = date_range.difference(existing_dates)
                    gaps = len(missing_dates)
                
                anomalies = self._detect_price_anomalies(symbol, start_date, end_date)
                
                conn.execute('''
                    INSERT OR REPLACE INTO data_quality
                    (symbol, date, completeness_score, gaps_count, 
                     sources_count, anomalies_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    symbol, datetime.now().date().isoformat(),
                    completeness, gaps,
                    df['source_count'].max() if not df.empty else 0,
                    len(anomalies)
                ))
            
            conn.commit()
    
    def _detect_price_anomalies(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        """Detect price anomalies in historical data."""
        anomalies = []
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query('''
                SELECT timestamp, close
                FROM prices
                WHERE symbol = ? AND timestamp BETWEEN ? AND ?
                AND source LIKE '%_1d'
                ORDER BY timestamp
            ''', conn, params=(symbol, start_date, end_date))
            
            if len(df) < 10:
                return anomalies
            
            df['return'] = df['close'].pct_change()
            
            mean_return = df['return'].mean()
            std_return = df['return'].std()
            
            for idx, row in df.iterrows():
                if abs(row['return'] - mean_return) > 3 * std_return:
                    anomalies.append({
                        'timestamp': row['timestamp'],
                        'return': row['return'],
                        'z_score': (row['return'] - mean_return) / std_return
                    })
        
        return anomalies
    
    def get_data_coverage_report(self) -> pd.DataFrame:
        """Generate data coverage report."""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT 
                    p.symbol,
                    hp.source,
                    hp.data_type,
                    hp.start_date,
                    hp.end_date,
                    hp.records_count,
                    dq.completeness_score,
                    dq.gaps_count,
                    dq.sources_count
                FROM historical_progress hp
                LEFT JOIN data_quality dq ON hp.symbol = dq.symbol
                ORDER BY hp.symbol, hp.source
            '''
            
            return pd.read_sql_query(query, conn)
    
    def fill_data_gaps(self, symbol: str, max_gap_days: int = 7):
        """Fill small gaps in historical data using interpolation."""
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query('''
                SELECT DATE(timestamp) as date, AVG(close) as close
                FROM prices
                WHERE symbol = ?
                GROUP BY DATE(timestamp)
                ORDER BY date
            ''', conn, params=(symbol,), parse_dates=['date'])
            
            if df.empty:
                return
            
            df.set_index('date', inplace=True)
            df_daily = df.resample('D').asfreq()
            gaps = df_daily[df_daily['close'].isna()]
            
            for date, row in gaps.iterrows():
                prev_valid = df_daily['close'].last_valid_index()
                next_valid = df_daily['close'].first_valid_index()
                
                if prev_valid and next_valid:
                    gap_size = (next_valid - prev_valid).days
                    
                    if gap_size <= max_gap_days:
                        interpolated_value = df_daily['close'].interpolate(method='linear').loc[date]
                        conn.execute('''
                            INSERT OR REPLACE INTO prices
                            (symbol, timestamp, close, source, asset_type)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            symbol, date.isoformat(), interpolated_value,
                            'interpolated', 'crypto'
                        ))
            
            conn.commit()


async def setup_historical_collection(db_path: str, coin_manager) -> HistoricalDataCollector:
    """Set up historical data collection system."""
    collector = HistoricalDataCollector(db_path, coin_manager)
    return collector
