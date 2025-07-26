"""YFinance comprehensive module for options flow, market sentiment, and minute data."""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import sqlite3
import asyncio
from typing import Dict, List, Optional, Tuple
import json
from concurrent.futures import ThreadPoolExecutor
import requests
from pathlib import Path

logger = logging.getLogger(__name__)


class YFinanceDataCollector:
    """Comprehensive yfinance data collector for market insights."""

    MARKET_INDICATORS = {
        'indices': ['^GSPC', '^DJI', '^IXIC', '^VIX', '^RUT', '^SOX'],
        'fear_greed': ['^VIX', 'GLD', 'TLT', 'HYG', 'UUP'],
        'sector_etfs': ['XLF', 'XLK', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLB', 'XLRE', 'XLU'],
        'crypto_stocks': ['COIN', 'MSTR', 'RIOT', 'MARA', 'CLSK', 'HIVE', 'BITF', 'HUT', 'SQ', 'PYPL'],
        'meme_stocks': ['GME', 'AMC', 'BB', 'BBBY', 'NOK', 'PLTR', 'CLOV', 'WISH'],
        'options_activity': ['SPY', 'QQQ', 'IWM', 'TSLA', 'AAPL', 'NVDA', 'AMD', 'META']
    }

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.init_db()

    def init_db(self):
        """Initialize yfinance database tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS yf_options_chain (
                    symbol TEXT NOT NULL,
                    expiration TEXT NOT NULL,
                    strike REAL NOT NULL,
                    option_type TEXT NOT NULL,
                    contract_symbol TEXT,
                    timestamp TEXT NOT NULL,
                    last_price REAL,
                    bid REAL,
                    ask REAL,
                    change REAL,
                    percent_change REAL,
                    volume INTEGER,
                    open_interest INTEGER,
                    implied_volatility REAL,
                    in_the_money BOOLEAN,
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    rho REAL,
                    theoretical_value REAL,
                    intrinsic_value REAL,
                    time_value REAL,
                    PRIMARY KEY (symbol, expiration, strike, option_type, timestamp)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS unusual_options (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    contract_symbol TEXT,
                    option_type TEXT,
                    strike REAL,
                    expiration TEXT,
                    volume INTEGER,
                    open_interest INTEGER,
                    volume_oi_ratio REAL,
                    price REAL,
                    total_cost REAL,
                    implied_volatility REAL,
                    delta REAL,
                    days_to_expiry INTEGER,
                    unusual_score REAL,
                    alert_sent BOOLEAN DEFAULT FALSE
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS yf_minute_data (
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    vwap REAL,
                    trades_count INTEGER,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS market_microstructure (
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    bid REAL,
                    ask REAL,
                    bid_size INTEGER,
                    ask_size INTEGER,
                    spread REAL,
                    spread_pct REAL,
                    mid_price REAL,
                    imbalance REAL,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS institutional_trades (
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    block_trades INTEGER,
                    block_volume INTEGER,
                    avg_block_size REAL,
                    dark_pool_volume INTEGER,
                    dark_pool_pct REAL,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS market_sentiment (
                    timestamp TEXT PRIMARY KEY,
                    vix REAL,
                    put_call_ratio REAL,
                    advance_decline REAL,
                    new_highs INTEGER,
                    new_lows INTEGER,
                    up_volume INTEGER,
                    down_volume INTEGER,
                    market_breadth REAL,
                    fear_greed_index REAL
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stock_fundamentals (
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    market_cap REAL,
                    pe_ratio REAL,
                    forward_pe REAL,
                    peg_ratio REAL,
                    price_to_book REAL,
                    enterprise_value REAL,
                    revenue_growth REAL,
                    earnings_growth REAL,
                    profit_margin REAL,
                    debt_to_equity REAL,
                    current_ratio REAL,
                    roa REAL,
                    roe REAL,
                    beta REAL,
                    short_interest REAL,
                    short_ratio REAL,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_options_symbol ON yf_options_chain(symbol)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_options_volume ON yf_options_chain(volume DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_unusual_symbol ON unusual_options(symbol, timestamp DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_minute_symbol ON yf_minute_data(symbol, timestamp DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_sentiment_ts ON market_sentiment(timestamp DESC)')

    def fetch_options_chain(
        self,
        symbol: str,
        include_greeks: bool = True,
        max_expirations: int = 5,
    ) -> pd.DataFrame:
        """Fetch options chain data.

        Parameters
        ----------
        symbol: str
            Ticker symbol to query.
        include_greeks: bool, optional
            Included for API compatibility. Greeks are always returned by
            ``yfinance``.
        max_expirations: int, optional
            Limit on the number of expiration dates to fetch. Defaults to 5.
        """
        try:
            ticker = yf.Ticker(symbol)
            expirations = ticker.options
            if not expirations:
                return pd.DataFrame()
            all_options = []
            timestamp = datetime.now()
            for exp_date in expirations[:max_expirations]:
                try:
                    opt_chain = ticker.option_chain(exp_date)
                    calls = opt_chain.calls
                    calls['option_type'] = 'CALL'
                    calls['expiration'] = exp_date
                    puts = opt_chain.puts
                    puts['option_type'] = 'PUT'
                    puts['expiration'] = exp_date
                    options = pd.concat([calls, puts], ignore_index=True)
                    options['symbol'] = symbol
                    options['timestamp'] = timestamp
                    spot_price = ticker.history(period='1d')['Close'].iloc[-1]
                    options['intrinsic_value'] = options.apply(
                        lambda x: max(0, spot_price - x['strike']) if x['option_type'] == 'CALL'
                        else max(0, x['strike'] - spot_price), axis=1
                    )
                    options['time_value'] = options['lastPrice'] - options['intrinsic_value']
                    options['in_the_money'] = options['inTheMoney']
                    all_options.append(options)
                except Exception as e:
                    logger.error(f"Error fetching options for {symbol} exp {exp_date}: {e}")
            if all_options:
                df = pd.concat(all_options, ignore_index=True)
                self._store_options_chain(df)
                return df
        except Exception as e:
            logger.error(f"Error fetching options chain for {symbol}: {e}")
        return pd.DataFrame()

    def detect_unusual_options(self, symbol: str = None, min_volume: int = 1000, min_ratio: float = 2.0) -> pd.DataFrame:
        """Detect unusual options activity."""
        with sqlite3.connect(self.db_path) as conn:
            if symbol:
                where_clause = f"WHERE symbol = '{symbol}'"
            else:
                where_clause = ""
            query = f'''
                SELECT *,
                    volume * implied_volatility * ABS(delta) as unusual_score
                FROM yf_options_chain
                {where_clause}
                {'AND' if where_clause else 'WHERE'} volume > ?
                AND volume > open_interest * ?
                AND timestamp > datetime('now', '-1 day')
                ORDER BY unusual_score DESC
                LIMIT 100
            '''
            unusual = pd.read_sql_query(query, conn, params=(min_volume, min_ratio))
            if not unusual.empty:
                unusual['days_to_expiry'] = (pd.to_datetime(unusual['expiration']) - datetime.now()).dt.days
                unusual['total_cost'] = unusual['last_price'] * unusual['volume'] * 100
                unusual['volume_oi_ratio'] = unusual['volume'] / unusual['open_interest']
                self._store_unusual_options(unusual)
            return unusual

    def fetch_minute_data(self, symbol: str, days: int = 5) -> pd.DataFrame:
        """Fetch minute-level data for detailed analysis."""
        try:
            ticker = yf.Ticker(symbol)
            if days <= 7:
                interval = '1m'
            else:
                interval = '2m'
                days = min(days, 60)
            df = ticker.history(period=f'{days}d', interval=interval)
            if not df.empty:
                df['symbol'] = symbol
                df['vwap'] = (df['High'] + df['Low'] + df['Close']) / 3
                df.reset_index(inplace=True)
                self._store_minute_data(df)
                self._calculate_microstructure(symbol, df)
            return df
        except Exception as e:
            logger.error(f"Error fetching minute data for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_market_sentiment(self) -> Dict:
        """Calculate overall market sentiment indicators."""
        sentiment = {'timestamp': datetime.now(), 'indicators': {}}
        try:
            vix = yf.Ticker('^VIX')
            vix_data = vix.history(period='1d')
            sentiment['indicators']['vix'] = vix_data['Close'].iloc[-1] if not vix_data.empty else None
            sentiment['indicators']['put_call_ratio'] = self._calculate_put_call_ratio()
            breadth = self._calculate_market_breadth()
            sentiment['indicators'].update(breadth)
            fear_greed = self._calculate_fear_greed_index()
            sentiment['indicators']['fear_greed_index'] = fear_greed
            self._store_market_sentiment(sentiment['indicators'])
        except Exception as e:
            logger.error(f"Error calculating market sentiment: {e}")
        return sentiment

    def fetch_stock_fundamentals(self, symbols: List[str]) -> pd.DataFrame:
        """Fetch fundamental data for stocks."""
        all_fundamentals = []
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                fundamentals = {
                    'symbol': symbol,
                    'timestamp': datetime.now(),
                    'market_cap': info.get('marketCap'),
                    'pe_ratio': info.get('trailingPE'),
                    'forward_pe': info.get('forwardPE'),
                    'peg_ratio': info.get('pegRatio'),
                    'price_to_book': info.get('priceToBook'),
                    'enterprise_value': info.get('enterpriseValue'),
                    'revenue_growth': info.get('revenueGrowth'),
                    'earnings_growth': info.get('earningsGrowth'),
                    'profit_margin': info.get('profitMargins'),
                    'debt_to_equity': info.get('debtToEquity'),
                    'current_ratio': info.get('currentRatio'),
                    'roa': info.get('returnOnAssets'),
                    'roe': info.get('returnOnEquity'),
                    'beta': info.get('beta'),
                    'short_interest': info.get('shortPercentOfFloat'),
                    'short_ratio': info.get('shortRatio')
                }
                all_fundamentals.append(fundamentals)
            except Exception as e:
                logger.error(f"Error fetching fundamentals for {symbol}: {e}")
        if all_fundamentals:
            df = pd.DataFrame(all_fundamentals)
            self._store_fundamentals(df)
            return df
        return pd.DataFrame()

    def analyze_options_flow(self, symbol: str = None) -> Dict:
        """Analyze options flow for sentiment."""
        with sqlite3.connect(self.db_path) as conn:
            where_clause = f"WHERE symbol = '{symbol}'" if symbol else ""
            query = f'''
                SELECT 
                    option_type,
                    SUM(volume * last_price * 100) as dollar_volume,
                    SUM(volume) as total_volume,
                    AVG(implied_volatility) as avg_iv,
                    SUM(CASE WHEN delta > 0.7 THEN volume ELSE 0 END) as itm_volume,
                    SUM(CASE WHEN delta <= 0.3 THEN volume ELSE 0 END) as otm_volume
                FROM yf_options_chain
                {where_clause}
                {'AND' if where_clause else 'WHERE'} timestamp > datetime('now', '-1 day')
                GROUP BY option_type
            '''
            flow = pd.read_sql_query(query, conn)
            if flow.empty:
                return {}
            call_volume = flow[flow['option_type'] == 'CALL']['total_volume'].sum()
            put_volume = flow[flow['option_type'] == 'PUT']['total_volume'].sum()
            analysis = {
                'symbol': symbol,
                'call_volume': call_volume,
                'put_volume': put_volume,
                'put_call_ratio': put_volume / call_volume if call_volume > 0 else 0,
                'call_dollar_volume': flow[flow['option_type'] == 'CALL']['dollar_volume'].sum(),
                'put_dollar_volume': flow[flow['option_type'] == 'PUT']['dollar_volume'].sum(),
                'sentiment': 'BULLISH' if call_volume > put_volume * 1.5 else 'BEARISH' if put_volume > call_volume * 1.5 else 'NEUTRAL'
            }
            return analysis

    def _calculate_microstructure(self, symbol: str, minute_data: pd.DataFrame):
        """Calculate market microstructure metrics."""
        if minute_data.empty:
            return
        microstructure = []
        for i in range(1, len(minute_data)):
            current = minute_data.iloc[i]
            prev = minute_data.iloc[i-1]
            high_low_spread = current['High'] - current['Low']
            mid_price = (current['High'] + current['Low']) / 2
            if current['Close'] > prev['Close']:
                buy_volume = current['Volume'] * 0.6
                sell_volume = current['Volume'] * 0.4
            else:
                buy_volume = current['Volume'] * 0.4
                sell_volume = current['Volume'] * 0.6
            imbalance = (buy_volume - sell_volume) / (buy_volume + sell_volume) if (buy_volume + sell_volume) > 0 else 0
            microstructure.append({
                'symbol': symbol,
                'timestamp': current['Date'],
                'spread': high_low_spread,
                'spread_pct': (high_low_spread / mid_price) * 100 if mid_price > 0 else 0,
                'mid_price': mid_price,
                'imbalance': imbalance
            })
        if microstructure:
            df = pd.DataFrame(microstructure)
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('market_microstructure', conn, if_exists='append', index=False)

    def _calculate_put_call_ratio(self) -> float:
        """Calculate market-wide put/call ratio."""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT 
                    SUM(CASE WHEN option_type = 'PUT' THEN volume ELSE 0 END) as put_volume,
                    SUM(CASE WHEN option_type = 'CALL' THEN volume ELSE 0 END) as call_volume
                FROM yf_options_chain
                WHERE timestamp > datetime('now', '-1 day')
                AND symbol IN ('SPY', 'QQQ', 'IWM')
            '''
            result = conn.execute(query).fetchone()
            if result and result[1] > 0:
                return result[0] / result[1]
        return 0.0

    def _load_sp500_symbols(self, refresh_hours: int = 24) -> List[str]:
        """Retrieve S&P 500 symbols using a cached JSON file."""
        cache_file = Path(self.db_path).with_name('sp500_symbols.json')
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text())
                last_update = datetime.fromisoformat(data.get('last_updated'))
                if datetime.now() - last_update < timedelta(hours=refresh_hours):
                    symbols = data.get('symbols', [])
                    if isinstance(symbols, list) and symbols:
                        return symbols
            except Exception as e:
                logging.error(f"Failed to load symbol cache: {e}")

        try:
            sp500 = pd.read_html(
                'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            )[0]
            symbols = sp500['Symbol'].tolist()
            cache_file.write_text(
                json.dumps({'last_updated': datetime.now().isoformat(), 'symbols': symbols})
            )
            return symbols
        except Exception as e:
            logging.error(f"Error fetching S&P 500 symbols: {e}")
            if cache_file.exists():
                try:
                    data = json.loads(cache_file.read_text())
                    return data.get('symbols', [])
                except Exception:
                    pass
        return []

    def _calculate_market_breadth(self) -> Dict:
        """Calculate market breadth indicators."""
        breadth = {}
        try:
            symbols = self._load_sp500_symbols()[:50]
            advances = 0
            declines = 0
            up_volume = 0
            down_volume = 0
            for symbol in symbols:
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(period='1d')
                    if not hist.empty:
                        change = hist['Close'].iloc[-1] - hist['Open'].iloc[-1]
                        volume = hist['Volume'].iloc[-1]
                        if change > 0:
                            advances += 1
                            up_volume += volume
                        else:
                            declines += 1
                            down_volume += volume
                except:
                    continue
            breadth['advance_decline'] = advances / (advances + declines) if (advances + declines) > 0 else 0.5
            breadth['up_volume'] = up_volume
            breadth['down_volume'] = down_volume
            breadth['market_breadth'] = up_volume / (up_volume + down_volume) if (up_volume + down_volume) > 0 else 0.5
        except Exception as e:
            logger.error(f"Error calculating market breadth: {e}")
        return breadth

    def _calculate_fear_greed_index(self) -> float:
        """Calculate simplified Fear & Greed Index."""
        components = []
        try:
            vix = yf.Ticker('^VIX').history(period='1d')['Close'].iloc[-1]
            vix_score = max(0, min(100, 100 - (vix - 10) * 2.5))
            components.append(vix_score)
            spy = yf.Ticker('SPY').history(period='126d')
            if len(spy) > 125:
                current = spy['Close'].iloc[-1]
                ma125 = spy['Close'].rolling(125).mean().iloc[-1]
                momentum_score = max(0, min(100, 50 + (current / ma125 - 1) * 500))
                components.append(momentum_score)
            gold = yf.Ticker('GLD').history(period='20d')
            if len(gold) > 1:
                gold_return = (gold['Close'].iloc[-1] / gold['Close'].iloc[-20] - 1) * 100
                gold_score = max(0, min(100, 50 - gold_return * 10))
                components.append(gold_score)
        except Exception as e:
            logger.error(f"Error calculating fear/greed: {e}")
        return sum(components) / len(components) if components else 50.0

    def _store_options_chain(self, df: pd.DataFrame):
        """Store options chain data."""
        if df.empty:
            return
        columns_to_store = [
            'symbol', 'expiration', 'strike', 'option_type', 'contractSymbol',
            'timestamp', 'lastPrice', 'bid', 'ask', 'change', 'percentChange',
            'volume', 'openInterest', 'impliedVolatility', 'inTheMoney',
            'intrinsic_value', 'time_value'
        ]
        for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
            if greek in df.columns:
                columns_to_store.append(greek)
        df_to_store = df[columns_to_store].copy()
        with sqlite3.connect(self.db_path) as conn:
            df_to_store.to_sql('yf_options_chain', conn, if_exists='append', index=False)

    def _store_unusual_options(self, df: pd.DataFrame):
        """Store unusual options activity."""
        columns = [
            'symbol', 'timestamp', 'contractSymbol', 'option_type', 'strike',
            'expiration', 'volume', 'openInterest', 'volume_oi_ratio',
            'lastPrice', 'total_cost', 'impliedVolatility', 'delta',
            'days_to_expiry', 'unusual_score'
        ]
        df_to_store = df[columns].copy()
        with sqlite3.connect(self.db_path) as conn:
            df_to_store.to_sql('unusual_options', conn, if_exists='append', index=False)

    def _store_minute_data(self, df: pd.DataFrame):
        """Store minute-level data."""
        df_to_store = df[['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'vwap']].copy()
        df_to_store.columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap']
        with sqlite3.connect(self.db_path) as conn:
            df_to_store.to_sql('yf_minute_data', conn, if_exists='append', index=False)

    def _store_market_sentiment(self, indicators: Dict):
        """Store market sentiment indicators."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO market_sentiment
                (timestamp, vix, put_call_ratio, advance_decline, up_volume,
                 down_volume, market_breadth, fear_greed_index)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now().isoformat(),
                indicators.get('vix'),
                indicators.get('put_call_ratio'),
                indicators.get('advance_decline'),
                indicators.get('up_volume'),
                indicators.get('down_volume'),
                indicators.get('market_breadth'),
                indicators.get('fear_greed_index')
            ))
            conn.commit()

    def _store_fundamentals(self, df: pd.DataFrame):
        """Store stock fundamentals."""
        with sqlite3.connect(self.db_path) as conn:
            df.to_sql('stock_fundamentals', conn, if_exists='append', index=False)

    async def run_comprehensive_update(self):
        """Run comprehensive market data update."""
        logger.info("Starting YFinance comprehensive update...")
        self.fetch_market_sentiment()
        for symbol in self.MARKET_INDICATORS['options_activity']:
            self.fetch_options_chain(symbol, max_expirations=5)
            await asyncio.sleep(1)
        unusual = self.detect_unusual_options()
        if not unusual.empty:
            logger.info(f"Found {len(unusual)} unusual options activities")
        for symbol in self.MARKET_INDICATORS['meme_stocks'][:5]:
            self.fetch_minute_data(symbol, days=1)
            await asyncio.sleep(1)
        self.fetch_stock_fundamentals(self.MARKET_INDICATORS['crypto_stocks'])
        logger.info("YFinance update completed")


async def setup_yfinance_collector(db_path: str) -> YFinanceDataCollector:
    """Initialize and return YFinance collector."""
    return YFinanceDataCollector(db_path)
