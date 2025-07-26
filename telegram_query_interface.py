"""Advanced Telegram bot interface for querying historical data and managing wallets."""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple, Any
import json
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import re

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from telegram.ext import CommandHandler, MessageHandler, Filters, CallbackQueryHandler, ConversationHandler

logger = logging.getLogger(__name__)


class TelegramQueryInterface:
    """Advanced query interface for Telegram bot."""

    QUERY_TYPE, DATE_RANGE, ANALYSIS_TYPE, CONFIRM = range(4)

    def __init__(self, db_path: str, telegram_system):
        self.db_path = db_path
        self.telegram = telegram_system
        self.query_cache = {}
        self.user_sessions = {}

    def setup_handlers(self, dispatcher):
        """Set up advanced query handlers."""
        dispatcher.add_handler(CommandHandler('query', self.cmd_query))
        dispatcher.add_handler(CommandHandler('analyze', self.cmd_analyze))
        dispatcher.add_handler(CommandHandler('patterns', self.cmd_patterns))
        dispatcher.add_handler(CommandHandler('correlate', self.cmd_correlate))
        dispatcher.add_handler(CommandHandler('backtest', self.cmd_backtest))
        dispatcher.add_handler(CommandHandler('addwallet', self.cmd_add_wallet))
        dispatcher.add_handler(CommandHandler('removewallet', self.cmd_remove_wallet))
        dispatcher.add_handler(CommandHandler('wallets', self.cmd_list_wallets))
        dispatcher.add_handler(CommandHandler('walletinfo', self.cmd_wallet_info))
        dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, self.handle_natural_query))

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('advancedquery', self.start_advanced_query)],
            states={
                self.QUERY_TYPE: [CallbackQueryHandler(self.select_query_type)],
                self.DATE_RANGE: [MessageHandler(Filters.text, self.set_date_range)],
                self.ANALYSIS_TYPE: [CallbackQueryHandler(self.select_analysis)],
                self.CONFIRM: [CallbackQueryHandler(self.execute_query)]
            },
            fallbacks=[CommandHandler('cancel', self.cancel_query)]
        )
        dispatcher.add_handler(conv_handler)

    def cmd_query(self, update: Update, context):
        """Handle /query command for database queries."""
        if not context.args:
            help_text = (
                "*📊 Query Examples:*\n\n"
                "`/query BTC price 2024-01-01 2024-12-01`\n"
                "`/query ETH volume last 30 days`\n"
                "`/query SOL sentiment week`\n"
                "`/query WHALE movements 0x123...`\n"
                "`/query OPTIONS flow SPY unusual`\n"
                "`/query CORRELATION BTC ETH 90d`\n"
                "`/query INFLUENCE elonmusk DOGE`\n"
                "\nOr just type naturally:\n"
                "_'Show me BTC price last month'_\n"
                "_'Find whales buying SOL'_\n"
                "_'What happened on Oct 15?'_"
            )
            update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)
            return

        query_parts = ' '.join(context.args).upper().split()
        query_type = query_parts[0]

        if query_type in ['BTC', 'ETH', 'SOL', 'DOGE', 'XRP', 'ADA']:
            self._handle_price_query(update, query_type, query_parts[1:])
        elif query_type == 'WHALE':
            self._handle_whale_query(update, query_parts[1:])
        elif query_type == 'OPTIONS':
            self._handle_options_query(update, query_parts[1:])
        elif query_type == 'CORRELATION':
            self._handle_correlation_query(update, query_parts[1:])
        elif query_type == 'INFLUENCE':
            self._handle_influence_query(update, query_parts[1:])
        else:
            update.message.reply_text("Unknown query type. Use /query for examples.")

    def cmd_analyze(self, update: Update, context):
        """Handle /analyze command for pattern analysis."""
        if not context.args:
            update.message.reply_text(
                "Usage: /analyze SYMBOL [pattern_type]\n"
                "Types: support_resistance, trend, breakout, accumulation"
            )
            return

        symbol = context.args[0].upper()
        pattern_type = context.args[1] if len(context.args) > 1 else 'all'

        analysis = self._analyze_patterns(symbol, pattern_type)

        if analysis:
            chart = self._create_analysis_chart(symbol, analysis)
            update.message.reply_photo(
                photo=chart,
                caption=f"*📈 {symbol} Analysis*\n\n{analysis['summary']}",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            update.message.reply_text(f"No data found for {symbol}")

    def cmd_patterns(self, update: Update, context):
        """Handle /patterns command to find historical patterns."""
        if len(context.args) < 2:
            update.message.reply_text(
                "Usage: /patterns SYMBOL DATE\n"
                "Example: /patterns BTC 2021-05-19\n"
                "Finds similar historical patterns"
            )
            return

        symbol = context.args[0].upper()
        target_date = context.args[1]
        patterns = self._find_similar_patterns(symbol, target_date)

        if patterns:
            msg = f"*🔍 Similar Patterns to {symbol} on {target_date}:*\n\n"
            for i, pattern in enumerate(patterns[:5], 1):
                msg += f"{i}. *{pattern['date']}*\n"
                msg += f"   Similarity: {pattern['similarity']:.2%}\n"
                msg += f"   Next 30d: {pattern['future_return']:+.2%}\n"
                msg += f"   Correlation: {pattern['correlation']:.3f}\n\n"
            update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text("No similar patterns found")

    def cmd_correlate(self, update: Update, context):
        """Handle /correlate command for correlation analysis."""
        if len(context.args) < 2:
            update.message.reply_text(
                "Usage: /correlate ASSET1 ASSET2 [days]\n"
                "Example: /correlate BTC ETH 90"
            )
            return

        asset1 = context.args[0].upper()
        asset2 = context.args[1].upper()
        days = int(context.args[2]) if len(context.args) > 2 else 30

        correlation_data = self._calculate_correlation(asset1, asset2, days)

        if correlation_data:
            chart = self._create_correlation_chart(asset1, asset2, correlation_data)
            msg = (
                f"*📊 Correlation Analysis*\n\n"
                f"{asset1} vs {asset2} ({days} days)\n\n"
                f"Correlation: *{correlation_data['correlation']:.3f}*\n"
                f"Beta: {correlation_data['beta']:.3f}\n"
                f"R-squared: {correlation_data['r_squared']:.3f}\n\n"
                f"When {asset1} moves 1%:\n"
                f"{asset2} typically moves {correlation_data['beta']:.2%}"
            )
            update.message.reply_photo(photo=chart, caption=msg, parse_mode=ParseMode.MARKDOWN)
        else:
            update.message.reply_text("Insufficient data for correlation analysis")

    def cmd_add_wallet(self, update: Update, context):
        """Handle /addwallet command."""
        if not context.args:
            update.message.reply_text(
                "Usage: /addwallet ADDRESS [label]\n"
                "Example: /addwallet 0x123... whale_wallet"
            )
            return

        address = context.args[0]
        label = ' '.join(context.args[1:]) if len(context.args) > 1 else None

        if not self._validate_wallet_address(address):
            update.message.reply_text("Invalid wallet address format")
            return

        success = self._add_wallet_to_tracking(address, label, update.effective_user.id)

        if success:
            update.message.reply_text(
                f"✅ Wallet added to tracking!\n"
                f"Address: `{address[:10]}...{address[-8:]}`\n"
                f"Label: {label or 'None'}",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            update.message.reply_text("Failed to add wallet")

    def cmd_list_wallets(self, update: Update, context):
        """List tracked wallets."""
        wallets = self._get_tracked_wallets(update.effective_user.id)

        if not wallets:
            update.message.reply_text("No wallets tracked. Use /addwallet to add one.")
            return

        msg = "*🔍 Tracked Wallets:*\n\n"
        for wallet in wallets:
            msg += f"• `{wallet['address'][:10]}...{wallet['address'][-8:]}`\n"
            if wallet['label']:
                msg += f"  Label: _{wallet['label']}_\n"
            msg += f"  Balance: ${wallet['balance']:,.2f}\n"
            msg += f"  Last Active: {wallet['last_active']}\n\n"
        keyboard = [
            [InlineKeyboardButton("📊 Wallet Analysis", callback_data="wallets_analyze")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="wallets_refresh")],
            [InlineKeyboardButton("➕ Add New", callback_data="wallets_add")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

    def cmd_wallet_info(self, update: Update, context):
        """Get detailed wallet information."""
        if not context.args:
            update.message.reply_text("Usage: /walletinfo ADDRESS")
            return

        address = context.args[0]
        info = self._get_wallet_detailed_info(address)

        if not info:
            update.message.reply_text("Wallet not found or not tracked")
            return

        chart = self._create_wallet_chart(info)
        msg = (
            f"*💼 Wallet Analysis*\n\n"
            f"Address: `{address[:10]}...{address[-8:]}`\n"
            f"Total Value: *${info['total_value']:,.2f}*\n"
            f"PnL (30d): {info['pnl_30d']:+.2%}\n\n"
            f"*Top Holdings:*\n"
        )
        for holding in info['holdings'][:5]:
            msg += f"• {holding['symbol']}: ${holding['value']:,.2f} ({holding['percentage']:.1%})\n"
        msg += f"\n*Recent Activity:*\n"
        msg += f"Transactions (24h): {info['tx_24h']}\n"
        msg += f"Volume (24h): ${info['volume_24h']:,.2f}\n"
        msg += f"Gas Spent (7d): ${info['gas_7d']:,.2f}\n"
        update.message.reply_photo(photo=chart, caption=msg, parse_mode=ParseMode.MARKDOWN)

    def handle_natural_query(self, update: Update, context):
        """Handle natural language queries."""
        query = update.message.text.lower()
        user_id = update.effective_user.id
        self.user_sessions[user_id] = {'last_query': query, 'timestamp': datetime.now()}
        intent = self._parse_query_intent(query)

        if intent['type'] == 'price':
            self._handle_price_nl_query(update, intent)
        elif intent['type'] == 'whale':
            self._handle_whale_nl_query(update, intent)
        elif intent['type'] == 'pattern':
            self._handle_pattern_nl_query(update, intent)
        elif intent['type'] == 'sentiment':
            self._handle_sentiment_nl_query(update, intent)
        elif intent['type'] == 'correlation':
            self._handle_correlation_nl_query(update, intent)
        else:
            suggestions = self._get_query_suggestions(query)
            msg = "I'm not sure what you're looking for. Did you mean:\n\n"
            for suggestion in suggestions:
                msg += f"• {suggestion}\n"
            update.message.reply_text(msg)

    def _parse_query_intent(self, query: str) -> Dict:
        """Parse natural language query intent."""
        intent = {'type': 'unknown', 'entities': {}}
        price_keywords = ['price', 'cost', 'worth', 'value', 'trading at']
        if any(keyword in query for keyword in price_keywords):
            intent['type'] = 'price'
            for symbol in ['btc', 'eth', 'sol', 'doge', 'xrp', 'ada']:
                if symbol in query:
                    intent['entities']['symbol'] = symbol.upper()
                    break
            if 'today' in query:
                intent['entities']['timeframe'] = 'today'
            elif 'yesterday' in query:
                intent['entities']['timeframe'] = 'yesterday'
            elif 'week' in query:
                intent['entities']['timeframe'] = 'week'
            elif 'month' in query:
                intent['entities']['timeframe'] = 'month'
        elif any(word in query for word in ['whale', 'large', 'big', 'accumulation']):
            intent['type'] = 'whale'
            if 'buying' in query or 'accumulating' in query:
                intent['entities']['action'] = 'buying'
            elif 'selling' in query or 'dumping' in query:
                intent['entities']['action'] = 'selling'
        elif any(word in query for word in ['pattern', 'similar', 'happened', 'historical']):
            intent['type'] = 'pattern'
            date_match = re.search(r'\d{4}-\d{2}-\d{2}', query)
            if date_match:
                intent['entities']['date'] = date_match.group()
        elif any(word in query for word in ['sentiment', 'feeling', 'bullish', 'bearish']):
            intent['type'] = 'sentiment'
        elif any(word in query for word in ['correlation', 'relationship', 'together']):
            intent['type'] = 'correlation'
        return intent

    def _handle_price_query(self, update: Update, symbol: str, params: List[str]):
        """Handle price query."""
        timeframe = 'day'
        start_date = None
        end_date = None
        if 'WEEK' in params:
            timeframe = 'week'
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        elif 'MONTH' in params:
            timeframe = 'month'
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        elif len(params) >= 2:
            start_date = params[0]
            end_date = params[1] if len(params) > 1 else datetime.now().strftime('%Y-%m-%d')
        with sqlite3.connect(self.db_path) as conn:
            if start_date:
                query = (
                    'SELECT timestamp, close, volume FROM prices '
                    'WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp DESC LIMIT 100'
                )
                df = pd.read_sql_query(query, conn, params=(symbol, start_date))
            else:
                query = (
                    'SELECT timestamp, close, volume FROM prices '
                    'WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1'
                )
                df = pd.read_sql_query(query, conn, params=(symbol,))
        if df.empty:
            update.message.reply_text(f"No price data found for {symbol}")
            return
        if len(df) == 1:
            row = df.iloc[0]
            msg = (
                f"*{symbol} Price*\n\n"
                f"💰 ${row['close']:,.2f}\n"
                f"📊 Volume: {row['volume']:,.0f}\n"
                f"🕐 {row['timestamp']}"
            )
            update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        else:
            current = df.iloc[0]['close']
            start = df.iloc[-1]['close']
            change = ((current - start) / start) * 100
            chart = self._create_price_chart(symbol, df)
            msg = (
                f"*{symbol} Price Analysis*\n\n"
                f"Current: ${current:,.2f}\n"
                f"Change: {change:+.2f}%\n"
                f"High: ${df['close'].max():,.2f}\n"
                f"Low: ${df['close'].min():,.2f}\n"
                f"Avg Volume: {df['volume'].mean():,.0f}"
            )
            update.message.reply_photo(photo=chart, caption=msg, parse_mode=ParseMode.MARKDOWN)

    def _handle_whale_query(self, update: Update, params: List[str]):
        """Handle whale movement query."""
        action = 'all'
        symbol = None
        for param in params:
            if param in ['BUYING', 'SELLING']:
                action = param.lower()
            elif param in ['BTC', 'ETH', 'SOL', 'USDT', 'USDC']:
                symbol = param
        with sqlite3.connect(self.db_path) as conn:
            query = (
                'SELECT w.address, w.timestamp, w.token_symbol, w.value, w.tx_type, w.tx_hash '
                'FROM wallets w WHERE w.value > 100000 '
                'AND w.timestamp > datetime("now", "-24 hours")'
            )
            params = []
            if symbol:
                query += ' AND w.token_symbol = ?'
                params.append(symbol)
            if action == 'buying':
                query += ' AND w.tx_type IN ("swap", "transfer") AND w.value > 0'
            elif action == 'selling':
                query += ' AND w.tx_type IN ("swap", "transfer") AND w.value < 0'
            query += ' ORDER BY w.value DESC LIMIT 10'
            df = pd.read_sql_query(query, conn, params=params)
        if df.empty:
            update.message.reply_text("No whale movements found in the last 24 hours")
            return
        msg = "*🐋 Whale Movements (24h)*\n\n"
        for _, row in df.iterrows():
            emoji = "🟢" if row['value'] > 0 else "🔴"
            msg += f"{emoji} `{row['address'][:8]}...{row['address'][-6:]}`\n"
            msg += f"   ${abs(row['value']):,.0f} {row['token_symbol']}\n"
            msg += f"   {row['tx_type'].title()} • {row['timestamp'][:16]}\n\n"
        update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    def _analyze_patterns(self, symbol: str, pattern_type: str) -> Dict:
        """Analyze price patterns for a symbol."""
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                'SELECT timestamp, open, high, low, close, volume FROM prices '
                'WHERE symbol = ? ORDER BY timestamp DESC LIMIT 500',
                conn, params=(symbol,)
            )
        if df.empty:
            return None
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        analysis = {'symbol': symbol, 'patterns': [], 'summary': ''}
        if pattern_type in ['all', 'support_resistance']:
            support, resistance = self._find_support_resistance(df)
            analysis['patterns'].append({'type': 'support_resistance', 'support': support, 'resistance': resistance})
            analysis['summary'] += f"Support: ${support:.2f}, Resistance: ${resistance:.2f}\n"
        if pattern_type in ['all', 'trend']:
            trend = self._analyze_trend(df)
            analysis['patterns'].append({'type': 'trend', 'data': trend})
            analysis['summary'] += f"Trend: {trend['direction']} ({trend['strength']})\n"
        if pattern_type in ['all', 'breakout']:
            breakout = self._detect_breakout(df)
            if breakout:
                analysis['patterns'].append({'type': 'breakout', 'data': breakout})
                analysis['summary'] += f"Breakout detected at ${breakout['level']:.2f}\n"
        return analysis

    def _find_support_resistance(self, df: pd.DataFrame) -> Tuple[float, float]:
        """Find support and resistance levels."""
        recent_lows = df['low'].rolling(window=20).min()
        recent_highs = df['high'].rolling(window=20).max()
        support = recent_lows.dropna().quantile(0.1)
        resistance = recent_highs.dropna().quantile(0.9)
        return support, resistance

    def _analyze_trend(self, df: pd.DataFrame) -> Dict:
        """Analyze price trend."""
        df['ma_short'] = df['close'].rolling(window=20).mean()
        df['ma_long'] = df['close'].rolling(window=50).mean()
        current_price = df['close'].iloc[-1]
        ma_short = df['ma_short'].iloc[-1]
        ma_long = df['ma_long'].iloc[-1]
        if ma_short > ma_long and current_price > ma_short:
            direction = 'UPTREND'
            strength = 'Strong' if (current_price - ma_long) / ma_long > 0.1 else 'Moderate'
        elif ma_short < ma_long and current_price < ma_short:
            direction = 'DOWNTREND'
            strength = 'Strong' if (ma_long - current_price) / ma_long > 0.1 else 'Moderate'
        else:
            direction = 'SIDEWAYS'
            strength = 'Neutral'
        return {'direction': direction, 'strength': strength, 'ma_short': ma_short, 'ma_long': ma_long}

    def _detect_breakout(self, df: pd.DataFrame) -> Optional[Dict]:
        """Detect price breakouts."""
        recent_high = df['high'].tail(50).max()
        recent_low = df['low'].tail(50).min()
        current_close = df['close'].iloc[-1]
        if current_close > recent_high * 0.98:
            return {'type': 'resistance_breakout', 'level': recent_high, 'current': current_close, 'strength': (current_close - recent_high) / recent_high}
        elif current_close < recent_low * 1.02:
            return {'type': 'support_breakdown', 'level': recent_low, 'current': current_close, 'strength': (recent_low - current_close) / recent_low}
        return None

    def _find_similar_patterns(self, symbol: str, target_date: str) -> List[Dict]:
        """Find similar historical patterns."""
        with sqlite3.connect(self.db_path) as conn:
            target_df = pd.read_sql_query(
                'SELECT timestamp, close FROM prices WHERE symbol = ? AND DATE(timestamp) BETWEEN DATE(?, "-30 days") AND DATE(?, "+30 days") ORDER BY timestamp',
                conn, params=(symbol, target_date, target_date)
            )
            if len(target_df) < 20:
                return []
            target_pattern = (target_df['close'] / target_df['close'].iloc[0] - 1).values
            all_prices = pd.read_sql_query(
                'SELECT timestamp, close FROM prices WHERE symbol = ? ORDER BY timestamp',
                conn, params=(symbol,)
            )
            if len(all_prices) < 100:
                return []
            similar_patterns = []
            window_size = len(target_pattern)
            for i in range(len(all_prices) - window_size - 30):
                window = all_prices.iloc[i:i + window_size]
                window_pattern = (window['close'] / window['close'].iloc[0] - 1).values
                if len(window_pattern) == len(target_pattern):
                    correlation = np.corrcoef(target_pattern, window_pattern)[0, 1]
                    if correlation > 0.7:
                        future_data = all_prices.iloc[i + window_size:i + window_size + 30]
                        if len(future_data) > 0:
                            future_return = (future_data['close'].iloc[-1] / window['close'].iloc[-1] - 1)
                            similar_patterns.append({'date': window['timestamp'].iloc[0], 'correlation': correlation, 'similarity': correlation, 'future_return': future_return})
            similar_patterns.sort(key=lambda x: x['correlation'], reverse=True)
            return similar_patterns[:10]

    def _calculate_correlation(self, asset1: str, asset2: str, days: int) -> Dict:
        """Calculate correlation between two assets."""
        with sqlite3.connect(self.db_path) as conn:
            query = (
                'SELECT DATE(timestamp) as date, symbol, AVG(close) as close FROM prices '
                'WHERE symbol IN (?, ?) AND timestamp > datetime("now", ?) GROUP BY date, symbol'
            )
            df = pd.read_sql_query(query, conn, params=(asset1, asset2, f'-{days} days'))
        if df.empty:
            return None
        pivot_df = df.pivot(index='date', columns='symbol', values='close')
        if len(pivot_df) < 10 or asset1 not in pivot_df.columns or asset2 not in pivot_df.columns:
            return None
        returns1 = pivot_df[asset1].pct_change().dropna()
        returns2 = pivot_df[asset2].pct_change().dropna()
        correlation = returns1.corr(returns2)
        covariance = returns1.cov(returns2)
        variance1 = returns1.var()
        beta = covariance / variance1 if variance1 != 0 else 0
        r_squared = correlation ** 2
        return {'correlation': correlation, 'beta': beta, 'r_squared': r_squared, 'data': pivot_df}

    def _create_price_chart(self, symbol: str, df: pd.DataFrame) -> BytesIO:
        """Create price chart."""
        plt.figure(figsize=(10, 6))
        plt.plot(pd.to_datetime(df['timestamp']), df['close'], 'b-', linewidth=2)
        ax2 = plt.gca().twinx()
        ax2.bar(pd.to_datetime(df['timestamp']), df['volume'], alpha=0.3, color='gray')
        plt.title(f'{symbol} Price Chart')
        plt.xlabel('Date')
        plt.ylabel('Price ($)')
        ax2.set_ylabel('Volume')
        plt.tight_layout()
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        return buf

    def _create_correlation_chart(self, asset1: str, asset2: str, data: Dict) -> BytesIO:
        """Create correlation scatter plot."""
        plt.figure(figsize=(10, 6))
        df = data['data']
        returns1 = df[asset1].pct_change().dropna()
        returns2 = df[asset2].pct_change().dropna()
        plt.scatter(returns1 * 100, returns2 * 100, alpha=0.5)
        z = np.polyfit(returns1, returns2, 1)
        p = np.poly1d(z)
        plt.plot(returns1 * 100, p(returns1) * 100, "r--", alpha=0.8)
        plt.xlabel(f'{asset1} Returns (%)')
        plt.ylabel(f'{asset2} Returns (%)')
        plt.title(f'{asset1} vs {asset2} Correlation')
        plt.grid(True, alpha=0.3)
        plt.text(0.05, 0.95, f'Correlation: {data["correlation"]:.3f}', transform=plt.gca().transAxes, verticalalignment='top')
        plt.tight_layout()
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        return buf

    def _validate_wallet_address(self, address: str) -> bool:
        """Validate wallet address format."""
        if address.startswith('0x') and len(address) == 42:
            return True
        if len(address) >= 32 and len(address) <= 44:
            return True
        if (address.startswith('1') or address.startswith('3') or address.startswith('bc1')) and len(address) >= 26:
            return True
        return False

    def _add_wallet_to_tracking(self, address: str, label: str, user_id: int) -> bool:
        """Add wallet to tracking system."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO tracked_wallets (address, label, added_by, added_date, chain) VALUES (?, ?, ?, ?, ?)',
                    (address, label, str(user_id), datetime.now().isoformat(), self._detect_chain(address))
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding wallet: {e}")
            return False

    def _detect_chain(self, address: str) -> str:
        """Detect blockchain from address format."""
        if address.startswith('0x'):
            return 'ethereum'
        elif len(address) >= 32 and len(address) <= 44:
            return 'solana'
        elif address.startswith('1') or address.startswith('3'):
            return 'bitcoin'
        else:
            return 'unknown'
