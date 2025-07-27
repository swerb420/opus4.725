"""Enhanced coin management system with auto-discovery and Telegram control."""

import asyncio
import sqlite3
import json
import logging
from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime, timedelta
import ccxt.async_support as ccxt
import pandas as pd
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode

class CoinManager:
    """Dynamically manage and discover cryptocurrencies across all data sources."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.tracked_coins: Set[str] = set()
        self.coin_metadata: Dict[str, Dict] = {}
        self.init_db()
        self.load_tracked_coins()
    
    def init_db(self):
        """Initialize coin management tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tracked_coins (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    enabled BOOLEAN DEFAULT TRUE,
                    added_date TEXT,
                    added_by TEXT,
                    market_cap_rank INTEGER,
                    categories JSON,
                    chains JSON,
                    contract_addresses JSON,
                    social_links JSON,
                    metadata JSON,
                    last_updated TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS coin_discovery_log (
                    symbol TEXT,
                    source TEXT,
                    discovery_date TEXT,
                    volume_24h REAL,
                    market_cap REAL,
                    price REAL,
                    change_24h REAL,
                    PRIMARY KEY (symbol, source, discovery_date)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS coin_alerts (
                    symbol TEXT,
                    alert_type TEXT,
                    threshold REAL,
                    comparison TEXT,
                    enabled BOOLEAN DEFAULT TRUE,
                    created_by TEXT,
                    created_date TEXT,
                    PRIMARY KEY (symbol, alert_type)
                )
            ''')
    
    def load_tracked_coins(self):
        """Load currently tracked coins from database."""
        with sqlite3.connect(self.db_path) as conn:
            coins = conn.execute(
                "SELECT symbol FROM tracked_coins WHERE enabled = TRUE"
            ).fetchall()
            self.tracked_coins = {coin[0] for coin in coins}
            
            metadata = conn.execute(
                "SELECT symbol, name, categories, chains, contract_addresses FROM tracked_coins"
            ).fetchall()
            
            for symbol, name, categories, chains, addresses in metadata:
                self.coin_metadata[symbol] = {
                    'name': name,
                    'categories': json.loads(categories) if categories else [],
                    'chains': json.loads(chains) if chains else [],
                    'addresses': json.loads(addresses) if addresses else {}
                }
    
    async def discover_new_coins(self, exchanges: Dict[str, ccxt.Exchange]) -> List[str]:
        """Discover new coins from exchanges."""
        all_symbols: Set[str] = set()
        new_coins: List[str] = []
        
        for exchange_name, exchange in exchanges.items():
            try:
                markets = exchange.markets
                for symbol, market in markets.items():
                    if market['active']:
                        base = market['base']
                        quote = market['quote']
                        if base not in ['USD', 'USDT', 'USDC', 'BUSD', 'EUR', 'GBP']:
                            all_symbols.add(base)
                        if quote not in ['USD', 'USDT', 'USDC', 'BUSD', 'EUR', 'GBP'] and 'USD' not in quote:
                            all_symbols.add(quote)
            except Exception as e:
                logging.error(f"Error discovering coins from {exchange_name}: {e}")
        
        for symbol in all_symbols:
            if symbol not in self.tracked_coins:
                new_coins.append(symbol)
                await self._fetch_coin_data(symbol, exchanges)
        
        if new_coins:
            self._log_discoveries(new_coins)
            logging.info(f"Discovered {len(new_coins)} new coins: {new_coins[:10]}...")
        
        return new_coins
    
    async def _fetch_coin_data(self, symbol: str, exchanges: Dict[str, ccxt.Exchange]):
        """Fetch initial data for a newly discovered coin."""
        data = {
            'symbol': symbol,
            'exchanges': [],
            'total_volume': 0,
            'prices': []
        }
        
        for exchange_name, exchange in exchanges.items():
            try:
                pairs = [s for s in exchange.symbols if symbol in s.split('/')[0]]
                
                if pairs:
                    main_pair = next((p for p in pairs if 'USDT' in p), pairs[0])
                    ticker = await exchange.fetch_ticker(main_pair)
                    
                    data['exchanges'].append(exchange_name)
                    data['total_volume'] += ticker.get('quoteVolume', 0)
                    data['prices'].append(ticker.get('last', 0))
            except Exception as e:
                logging.debug(f"Error fetching {symbol} from {exchange_name}: {e}")
        
        if data['prices']:
            avg_price = sum(data['prices']) / len(data['prices'])
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR IGNORE INTO coin_discovery_log
                    (symbol, source, discovery_date, volume_24h, price)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    symbol, ','.join(data['exchanges']),
                    datetime.now().isoformat(),
                    data['total_volume'], avg_price
                ))
                conn.commit()
    
    def add_coin(self, symbol: str, name: str = None, added_by: str = 'system',
                 categories: List[str] = None, chains: List[str] = None,
                 addresses: Dict[str, str] = None) -> bool:
        """Add a coin to tracking."""
        symbol = symbol.upper()
        
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO tracked_coins
                    (symbol, name, enabled, added_date, added_by, categories, 
                     chains, contract_addresses, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol, name or symbol, True, datetime.now().isoformat(), added_by,
                    json.dumps(categories or []), json.dumps(chains or []),
                    json.dumps(addresses or {}), datetime.now().isoformat()
                ))
                conn.commit()
                
                self.tracked_coins.add(symbol)
                self.coin_metadata[symbol] = {
                    'name': name or symbol,
                    'categories': categories or [],
                    'chains': chains or [],
                    'addresses': addresses or {}
                }
                
                logging.info(f"Added {symbol} to tracking")
                return True
                
            except Exception as e:
                logging.error(f"Error adding coin {symbol}: {e}")
                return False
    
    def remove_coin(self, symbol: str) -> bool:
        """Remove a coin from tracking."""
        symbol = symbol.upper()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE tracked_coins SET enabled = FALSE WHERE symbol = ?",
                (symbol,)
            )
            conn.commit()
        
        self.tracked_coins.discard(symbol)
        return True
    
    def set_alert(self, symbol: str, alert_type: str, threshold: float,
                  comparison: str = '>', created_by: str = 'user') -> bool:
        """Set a custom alert for a coin."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO coin_alerts
                (symbol, alert_type, threshold, comparison, enabled, created_by, created_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol.upper(), alert_type, threshold, comparison,
                True, created_by, datetime.now().isoformat()
            ))
            conn.commit()
        return True
    
    def get_alerts(self, symbol: str = None) -> List[Dict]:
        """Get alerts for a coin or all coins."""
        with sqlite3.connect(self.db_path) as conn:
            if symbol:
                query = "SELECT * FROM coin_alerts WHERE symbol = ? AND enabled = TRUE"
                params = (symbol.upper(),)
            else:
                query = "SELECT * FROM coin_alerts WHERE enabled = TRUE"
                params = ()
            
            alerts = conn.execute(query, params).fetchall()
            
            return [{
                'symbol': a[0],
                'alert_type': a[1],
                'threshold': a[2],
                'comparison': a[3]
            } for a in alerts]
    
    def search_coins(self, query: str) -> List[Tuple[str, str]]:
        """Search for coins by symbol or name."""
        query = query.upper()
        results = []
        
        for symbol, metadata in self.coin_metadata.items():
            if query in symbol or query in metadata['name'].upper():
                results.append((symbol, metadata['name']))
        
        return results[:10]
    
    def get_coins_by_category(self, category: str) -> List[str]:
        """Get all coins in a specific category."""
        coins: List[str] = []
        for symbol, metadata in self.coin_metadata.items():
            if category.lower() in [c.lower() for c in metadata.get('categories', [])]:
                coins.append(symbol)
        return coins
    
    def get_trending_discoveries(self, days: int = 7) -> pd.DataFrame:
        """Get recently discovered coins with high activity."""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT 
                    symbol,
                    COUNT(DISTINCT source) as exchange_count,
                    MAX(volume_24h) as max_volume,
                    AVG(price) as avg_price,
                    MAX(discovery_date) as last_seen
                FROM coin_discovery_log
                WHERE discovery_date > datetime('now', ?)
                GROUP BY symbol
                ORDER BY max_volume DESC
                LIMIT 50
            '''
            
            df = pd.read_sql_query(query, conn, params=(f'-{days} days',))
            return df
    
    def auto_add_trending(self, min_volume: float = 1000000, min_exchanges: int = 2):
        """Automatically add trending newly discovered coins."""
        trending = self.get_trending_discoveries()
        
        added = []
        for _, coin in trending.iterrows():
            if (
                coin['max_volume'] >= min_volume and 
                coin['exchange_count'] >= min_exchanges and
                coin['symbol'] not in self.tracked_coins
            ):
                if self.add_coin(coin['symbol'], added_by='auto_discovery'):
                    added.append(coin['symbol'])
        
        if added:
            logging.info(f"Auto-added {len(added)} trending coins: {added}")
        
        return added


class TelegramCoinCommands:
    """Telegram commands for coin management."""
    
    def __init__(self, coin_manager: CoinManager):
        self.coin_manager = coin_manager
    
    def setup_handlers(self, dispatcher):
        from telegram.ext import CommandHandler, CallbackQueryHandler
        
        dispatcher.add_handler(CommandHandler('addcoin', self.cmd_add_coin))
        dispatcher.add_handler(CommandHandler('removecoin', self.cmd_remove_coin))
        dispatcher.add_handler(CommandHandler('listcoins', self.cmd_list_coins))
        dispatcher.add_handler(CommandHandler('searchcoin', self.cmd_search_coin))
        dispatcher.add_handler(CommandHandler('coinalert', self.cmd_coin_alert))
        dispatcher.add_handler(CommandHandler('trending', self.cmd_trending))
        dispatcher.add_handler(CommandHandler('categories', self.cmd_categories))
        dispatcher.add_handler(CallbackQueryHandler(self.handle_coin_callback, pattern='^coin_'))
    
    def cmd_add_coin(self, update, context):
        if not context.args:
            update.message.reply_text(
                "Usage: /addcoin SYMBOL [name]\n"
                "Example: /addcoin TRX Tron"
            )
            return
        
        symbol = context.args[0].upper()
        name = ' '.join(context.args[1:]) if len(context.args) > 1 else symbol
        
        if self.coin_manager.add_coin(symbol, name, added_by=f'telegram:{update.effective_user.id}'):
            keyboard = [
                [InlineKeyboardButton("\ud83d\udcca Set Price Alert", callback_data=f"coin_alert_{symbol}")],
                [InlineKeyboardButton("\ud83c\udff7\ufe0f Add Category", callback_data=f"coin_category_{symbol}")],
                [InlineKeyboardButton("\ud83d\udd17 Add Contract", callback_data=f"coin_contract_{symbol}")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            update.message.reply_text(
                f"\u2705 Added *{symbol}* ({name}) to tracking!\n\n"
                f"The bot will now collect data for this coin from all available sources.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        else:
            update.message.reply_text(f"\u274c Failed to add {symbol}")
    
    def cmd_remove_coin(self, update, context):
        if not context.args:
            update.message.reply_text("Usage: /removecoin SYMBOL")
            return
        
        symbol = context.args[0].upper()
        
        if self.coin_manager.remove_coin(symbol):
            update.message.reply_text(f"\u2705 Removed {symbol} from tracking")
        else:
            update.message.reply_text(f"\u274c Failed to remove {symbol}")
    
    def cmd_list_coins(self, update, context):
        coins = sorted(self.coin_manager.tracked_coins)
        
        if not coins:
            update.message.reply_text("No coins are currently being tracked.")
            return
        
        page_size = 50
        pages = [coins[i:i+page_size] for i in range(0, len(coins), page_size)]
        
        msg = f"*\ud83d\udcca Tracking {len(coins)} Coins:*\n\n"
        msg += ", ".join(pages[0])
        
        if len(pages) > 1:
            msg += f"\n\n_...and {len(coins) - page_size} more_"
            
            keyboard = [[InlineKeyboardButton(f"Page {i+1}", callback_data=f"coin_page_{i}")] 
                       for i in range(1, len(pages))]
            reply_markup = InlineKeyboardMarkup(keyboard)
            update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
        else:
            update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    
    def cmd_search_coin(self, update, context):
        if not context.args:
            update.message.reply_text("Usage: /searchcoin QUERY")
            return
        
        query = ' '.join(context.args)
        results = self.coin_manager.search_coins(query)
        
        if not results:
            update.message.reply_text(f"No coins found matching '{query}'")
            return
        
        msg = f"*\ud83d\udd0d Search Results for '{query}':*\n\n"
        for symbol, name in results:
            status = "\u2705" if symbol in self.coin_manager.tracked_coins else "\u2795"
            msg += f"{status} *{symbol}* - {name}\n"
        
        keyboard = []
        for symbol, name in results:
            if symbol not in self.coin_manager.tracked_coins:
                keyboard.append([InlineKeyboardButton(
                    f"Add {symbol}", 
                    callback_data=f"coin_add_{symbol}"
                )])
        
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    
    def cmd_coin_alert(self, update, context):
        if len(context.args) < 3:
            update.message.reply_text(
                "Usage: /coinalert SYMBOL TYPE VALUE\n"
                "Types: price_above, price_below, volume_above, change_above\n"
                "Example: /coinalert BTC price_above 50000"
            )
            return
        
        symbol = context.args[0].upper()
        alert_type = context.args[1].lower()
        try:
            value = float(context.args[2])
        except ValueError:
            update.message.reply_text("Value must be a number")
            return
        
        comparison = '>' if 'above' in alert_type else '<'
        
        if self.coin_manager.set_alert(
            symbol, alert_type, value, comparison,
            created_by=f'telegram:{update.effective_user.id}'
        ):
            update.message.reply_text(
                f"\u2705 Alert set for *{symbol}*\n"
                f"Type: {alert_type}\n"
                f"Threshold: {value}",
                parse_mode=ParseMode.MARKDOWN
            )
    
    def cmd_trending(self, update, context):
        trending = self.coin_manager.get_trending_discoveries(days=7)
        
        if trending.empty:
            update.message.reply_text("No trending discoveries in the past week.")
            return
        
        msg = "*\ud83d\udd25 Trending New Discoveries:*\n\n"
        
        for _, coin in trending.head(10).iterrows():
            status = "\u2705" if coin['symbol'] in self.coin_manager.tracked_coins else "\U0001f195"
            volume_m = coin['max_volume'] / 1_000_000
            msg += f"{status} *{coin['symbol']}* - ${volume_m:.1f}M volume\n"
            msg += f"   Found on {coin['exchange_count']} exchanges\n\n"
        
        keyboard = [[InlineKeyboardButton(
            "\u2795 Add All Trending", 
            callback_data="coin_add_trending"
        )]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    
    def cmd_categories(self, update, context):
        categories = [
            'DeFi', 'NFT', 'Gaming', 'Metaverse', 'Layer1', 'Layer2',
            'Exchange', 'Stablecoin', 'Meme', 'Privacy', 'Oracle', 'Storage'
        ]
        
        msg = "*\ud83d\udcc1 Coin Categories:*\n\n"
        
        for category in categories:
            coins = self.coin_manager.get_coins_by_category(category)
            if coins:
                msg += f"*{category}* ({len(coins)} coins)\n"
                msg += f"{', '.join(coins[:5])}"
                if len(coins) > 5:
                    msg += f" +{len(coins)-5} more"
                msg += "\n\n"
        
        update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    
    def handle_coin_callback(self, update, context):
        query = update.callback_query
        query.answer()
        
        data = query.data
        
        if data.startswith("coin_add_"):
            symbol = data.replace("coin_add_", "")
            if symbol == "trending":
                added = self.coin_manager.auto_add_trending()
                query.edit_message_text(f"\u2705 Added {len(added)} trending coins: {', '.join(added)}")
            else:
                if self.coin_manager.add_coin(symbol):
                    query.edit_message_text(f"\u2705 Added {symbol} to tracking!")
                else:
                    query.edit_message_text(f"\u274c Failed to add {symbol}")
        elif data.startswith("coin_alert_"):
            symbol = data.replace("coin_alert_", "")
            query.edit_message_text(
                f"Set alert for {symbol}:\n"
                f"Use: /coinalert {symbol} TYPE VALUE\n"
                f"Types: price_above, price_below, volume_above"
            )


async def setup_enhanced_coin_tracking(db_path: str, ccxt_collector) -> CoinManager:
    """Set up enhanced coin tracking with auto-discovery."""
    manager = CoinManager(db_path)
    
    if ccxt_collector and getattr(ccxt_collector, "exchange", None):
        new_coins = await manager.discover_new_coins({"default": ccxt_collector.exchange})
        manager.auto_add_trending(min_volume=5_000_000)
    
    return manager
