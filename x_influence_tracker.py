"""Apify integration for tracking X (Twitter) influence on markets."""

import asyncio
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from apify_client import ApifyClient
import re
from textblob import TextBlob

from config import get_config

logger = logging.getLogger(__name__)


class XInfluenceTracker:
    """Track influential X accounts and their market impact."""

    INFLUENCERS = {
        'crypto': [
            'elonmusk',
            'saylor',
            'CathieDWood',
            'APompliano',
            'VitalikButerin',
            'aantonop',
            'cz_binance',
            'brian_armstrong',
            'SBF_FTX',
            'novogratz',
            'RaoulGMI',
            'PeterSchiff',
            'Pentosh1',
            'CryptoHayes',
            'AltcoinSherpa',
        ],
        'stocks': [
            'jimcramer',
            'stoolpresidente',
            'chamath',
            'RyanCohen',
            'KeithGill',
            'alexcutler247',
            'saxena_puru',
            'hmeisler',
            'MarketRebels',
            'OptionsHawk',
        ],
        'general_market': [
            'zerohedge',
            'DeItaone',
            'FirstSquawk',
            'LiveSquawk',
            'FinancialTimes',
            'WSJ',
            'business',
        ]
    }

    ASSET_KEYWORDS = {
        'crypto': {
            'BTC': ['bitcoin', 'btc', '₿', 'sats', 'satoshi'],
            'ETH': ['ethereum', 'eth', 'ether', 'vitalik'],
            'SOL': ['solana', 'sol', 'anatoly'],
            'DOGE': ['doge', 'dogecoin', 'shiba'],
            'XRP': ['xrp', 'ripple'],
            'ADA': ['cardano', 'ada', 'charles'],
            'AVAX': ['avalanche', 'avax'],
            'MATIC': ['polygon', 'matic'],
            'LINK': ['chainlink', 'link'],
            'DOT': ['polkadot', 'dot', 'gavin'],
        },
        'stocks': {
            'TSLA': ['tesla', 'tsla', '$tsla'],
            'GME': ['gamestop', 'gme', '$gme'],
            'AMC': ['amc', '$amc'],
            'AAPL': ['apple', 'aapl', '$aapl'],
            'NVDA': ['nvidia', 'nvda', '$nvda'],
            'SPY': ['spy', 'spx', 's&p'],
            'QQQ': ['qqq', 'nasdaq', 'tech'],
        }
    }

    def __init__(self, db_path: str):
        self.db_path = db_path
        token = get_config("APIFY_TOKEN")
        self.actor_id = get_config(
            "ACTOR_ID",
            "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest",
        )
        if not token:
            logger.warning(
                "APIFY_TOKEN not set. Apify operations will be skipped.")
            self.client = None
        else:
            self.client = ApifyClient(token)
        self.init_db()

    def init_db(self):
        """Initialize X influence tracking tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS x_influencers (
                    username TEXT PRIMARY KEY,
                    user_id TEXT,
                    name TEXT,
                    followers INTEGER,
                    following INTEGER,
                    tweets_count INTEGER,
                    category TEXT,
                    influence_score REAL,
                    avg_engagement REAL,
                    market_mover_score REAL,
                    last_updated TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS x_tweets (
                    tweet_id TEXT PRIMARY KEY,
                    username TEXT,
                    timestamp TEXT,
                    content TEXT,
                    likes INTEGER,
                    retweets INTEGER,
                    replies INTEGER,
                    views INTEGER,
                    engagement_rate REAL,
                    sentiment_score REAL,
                    mentioned_assets TEXT,
                    is_market_moving BOOLEAN,
                    url TEXT
                )
            ''')
            conn.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS x_tweets_fts
                USING fts5(tweet_id UNINDEXED, content)
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS x_market_impact (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT,
                    username TEXT,
                    asset_symbol TEXT,
                    asset_type TEXT,
                    tweet_timestamp TEXT,
                    price_before REAL,
                    price_after_5min REAL,
                    price_after_30min REAL,
                    price_after_1hr REAL,
                    volume_change_pct REAL,
                    impact_score REAL,
                    FOREIGN KEY (tweet_id) REFERENCES x_tweets(tweet_id)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS x_sentiment_history (
                    username TEXT,
                    asset_symbol TEXT,
                    date TEXT,
                    tweet_count INTEGER,
                    avg_sentiment REAL,
                    positive_count INTEGER,
                    negative_count INTEGER,
                    neutral_count INTEGER,
                    total_engagement INTEGER,
                    PRIMARY KEY (username, asset_symbol, date)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS x_coordinated_activity (
                    cluster_id INTEGER,
                    username TEXT,
                    tweet_id TEXT,
                    timestamp TEXT,
                    asset_mentioned TEXT,
                    similarity_score REAL,
                    PRIMARY KEY (cluster_id, username, tweet_id)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_user ON x_tweets(username, timestamp DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_tweets_asset ON x_tweets(mentioned_assets)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_impact_asset ON x_market_impact(asset_symbol, tweet_timestamp DESC)')

    async def fetch_influencer_tweets(self, username: str, max_tweets: int = 100) -> List[Dict]:
        """Fetch recent tweets from an influencer."""
        if not self.client:
            logger.warning("Apify client not configured; skipping fetch for %s", username)
            return []
        try:
            run_input = {
                "searchTerms": [f"from:{username}"],
                "maxTweets": max_tweets,
                "sort": "Latest",
                "tweetLanguage": "en"
            }
            run = self.client.actor(self.actor_id).call(run_input=run_input)
            tweets = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                tweets.append(item)
            return tweets
        except Exception as e:
            logger.error(f"Error fetching tweets for {username}: {e}")
            return []

    async def track_all_influencers(self):
        """Track tweets from all configured influencers."""
        if not self.client:
            logger.warning("Apify client not configured; skipping influencer tracking")
            return []
        all_tweets = []
        for category, usernames in self.INFLUENCERS.items():
            for username in usernames:
                logger.info(f"Fetching tweets from {username} ({category})")
                tweets = await self.fetch_influencer_tweets(username, max_tweets=50)
                if tweets:
                    processed_tweets = self._process_tweets(tweets, username, category)
                    all_tweets.extend(processed_tweets)
                await asyncio.sleep(5)
        await self._analyze_market_impact(all_tweets)
        return all_tweets

    def _process_tweets(self, tweets: List[Dict], username: str, category: str) -> List[Dict]:
        """Process raw tweet data."""
        processed = []
        for tweet in tweets:
            try:
                tweet_data = {
                    'tweet_id': tweet.get('id', ''),
                    'username': username,
                    'timestamp': tweet.get('created_at', ''),
                    'content': tweet.get('full_text', tweet.get('text', '')),
                    'likes': tweet.get('favorite_count', 0),
                    'retweets': tweet.get('retweet_count', 0),
                    'replies': tweet.get('reply_count', 0),
                    'views': tweet.get('view_count', 0),
                    'url': f"https://twitter.com/{username}/status/{tweet.get('id', '')}"
                }
                total_engagement = tweet_data['likes'] + tweet_data['retweets'] + tweet_data['replies']
                tweet_data['engagement_rate'] = total_engagement / max(tweet_data['views'], 1) if tweet_data['views'] else 0
                tweet_data['sentiment_score'] = self._analyze_sentiment(tweet_data['content'])
                mentioned_assets = self._extract_assets(tweet_data['content'])
                tweet_data['mentioned_assets'] = json.dumps(mentioned_assets)
                tweet_data['is_market_moving'] = self._is_market_moving(tweet_data, mentioned_assets)
                processed.append(tweet_data)
                self._store_tweet(tweet_data)
            except Exception as e:
                logger.error(f"Error processing tweet: {e}")
        return processed

    def _analyze_sentiment(self, text: str) -> float:
        """Analyze tweet sentiment."""
        try:
            text = re.sub(r'http\S+', '', text)
            text = re.sub(r'@\w+', '', text)
            text = re.sub(r'#', '', text)
            blob = TextBlob(text)
            sentiment = blob.sentiment.polarity
            bullish_words = ['bullish', 'moon', 'pump', 'buy', 'long', 'accumulate', 'breakout', 'rally']
            bearish_words = ['bearish', 'dump', 'sell', 'short', 'crash', 'collapse', 'bearish']
            text_lower = text.lower()
            for word in bullish_words:
                if word in text_lower:
                    sentiment += 0.2
            for word in bearish_words:
                if word in text_lower:
                    sentiment -= 0.2
            return max(-1, min(1, sentiment))
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return 0.0

    def _extract_assets(self, text: str) -> List[Dict]:
        """Extract mentioned assets from tweet."""
        mentioned = []
        text_lower = text.lower()
        for symbol, keywords in self.ASSET_KEYWORDS.get('crypto', {}).items():
            for keyword in keywords:
                if keyword in text_lower or f'${symbol.lower()}' in text_lower:
                    mentioned.append({'symbol': symbol, 'type': 'crypto'})
                    break
        for symbol, keywords in self.ASSET_KEYWORDS.get('stocks', {}).items():
            for keyword in keywords:
                if keyword in text_lower or f'${symbol.lower()}' in text_lower:
                    mentioned.append({'symbol': symbol, 'type': 'stock'})
                    break
        dollar_mentions = re.findall(r'\$([A-Z]{1,5})\b', text.upper())
        for symbol in dollar_mentions:
            if not any(m['symbol'] == symbol for m in mentioned):
                asset_type = 'crypto' if len(symbol) > 3 else 'stock'
                mentioned.append({'symbol': symbol, 'type': asset_type})
        return mentioned

    def _is_market_moving(self, tweet_data: Dict, mentioned_assets: List[Dict]) -> bool:
        """Determine if tweet is potentially market moving."""
        if tweet_data['engagement_rate'] > 0.05:
            return True
        if mentioned_assets and abs(tweet_data['sentiment_score']) > 0.5:
            return True
        if tweet_data['likes'] > 10000 or tweet_data['retweets'] > 5000:
            return True
        return False

    def _store_tweet(self, tweet_data: Dict):
        """Store tweet in database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO x_tweets
                (tweet_id, username, timestamp, content, likes, retweets, replies,
                 views, engagement_rate, sentiment_score, mentioned_assets,
                 is_market_moving, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tweet_data['tweet_id'], tweet_data['username'], tweet_data['timestamp'],
                tweet_data['content'], tweet_data['likes'], tweet_data['retweets'],
                tweet_data['replies'], tweet_data['views'], tweet_data['engagement_rate'],
                tweet_data['sentiment_score'], tweet_data['mentioned_assets'],
                tweet_data['is_market_moving'], tweet_data['url']
            ))
            conn.execute(
                'INSERT OR REPLACE INTO x_tweets_fts (tweet_id, content) VALUES (?, ?)',
                (tweet_data['tweet_id'], tweet_data['content'])
            )
            conn.commit()

    async def _analyze_market_impact(self, tweets: List[Dict]):
        """Analyze market impact of tweets."""
        for tweet in tweets:
            if not tweet['is_market_moving']:
                continue
            mentioned_assets = json.loads(tweet['mentioned_assets'])
            for asset in mentioned_assets:
                await self._calculate_price_impact(tweet, asset)

    async def _calculate_price_impact(self, tweet: Dict, asset: Dict):
        """Calculate price impact of a tweet on an asset."""
        impact_data = {
            'tweet_id': tweet['tweet_id'],
            'username': tweet['username'],
            'asset_symbol': asset['symbol'],
            'asset_type': asset['type'],
            'tweet_timestamp': tweet['timestamp'],
            'impact_score': tweet['engagement_rate'] * abs(tweet['sentiment_score'])
        }
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR IGNORE INTO x_market_impact
                (tweet_id, username, asset_symbol, asset_type, tweet_timestamp, impact_score)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                impact_data['tweet_id'], impact_data['username'], impact_data['asset_symbol'],
                impact_data['asset_type'], impact_data['tweet_timestamp'], impact_data['impact_score']
            ))
            conn.commit()

    def detect_coordinated_activity(self, time_window_minutes: int = 30) -> pd.DataFrame:
        """Detect coordinated posting activity."""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                WITH tweet_pairs AS (
                    SELECT 
                        t1.tweet_id as tweet1_id,
                        t2.tweet_id as tweet2_id,
                        t1.username as user1,
                        t2.username as user2,
                        t1.content as content1,
                        t2.content as content2,
                        t1.timestamp as time1,
                        t2.timestamp as time2,
                        t1.mentioned_assets as assets1,
                        t2.mentioned_assets as assets2,
                        ABS(julianday(t1.timestamp) - julianday(t2.timestamp)) * 24 * 60 as minutes_apart
                    FROM x_tweets t1
                    JOIN x_tweets t2 ON t1.tweet_id < t2.tweet_id
                    WHERE minutes_apart < ?
                    AND t1.mentioned_assets = t2.mentioned_assets
                    AND t1.mentioned_assets != '[]'
                )
                SELECT * FROM tweet_pairs
                WHERE minutes_apart < ?
            '''
            coordinated = pd.read_sql_query(query, conn, params=(time_window_minutes, time_window_minutes))
            if not coordinated.empty:
                coordinated['similarity_score'] = coordinated.apply(
                    lambda row: self._calculate_similarity(row['content1'], row['content2']),
                    axis=1
                )
                coordinated = coordinated[coordinated['similarity_score'] > 0.7]
                cluster_id = int(datetime.now().timestamp())
                for _, row in coordinated.iterrows():
                    for tweet_id, username in [(row['tweet1_id'], row['user1']), (row['tweet2_id'], row['user2'])]:
                        conn.execute('''
                            INSERT OR IGNORE INTO x_coordinated_activity
                            (cluster_id, username, tweet_id, timestamp, asset_mentioned, similarity_score)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            cluster_id, username, tweet_id,
                            datetime.now().isoformat(), row['assets1'], row['similarity_score']
                        ))
                conn.commit()
            return coordinated

    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity score."""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        return len(intersection) / len(union) if union else 0.0

    def get_influencer_impact_report(self, username: str = None) -> pd.DataFrame:
        """Generate influencer market impact report."""
        with sqlite3.connect(self.db_path) as conn:
            if username:
                where_clause = f"WHERE i.username = '{username}'"
            else:
                where_clause = ""
            query = f'''
                SELECT 
                    i.username,
                    i.asset_symbol,
                    i.asset_type,
                    COUNT(*) as tweet_count,
                    AVG(i.impact_score) as avg_impact_score,
                    AVG(t.sentiment_score) as avg_sentiment,
                    SUM(t.likes) as total_likes,
                    SUM(t.retweets) as total_retweets,
                    AVG(t.engagement_rate) as avg_engagement_rate
                FROM x_market_impact i
                JOIN x_tweets t ON i.tweet_id = t.tweet_id
                {where_clause}
                GROUP BY i.username, i.asset_symbol
                ORDER BY avg_impact_score DESC
            '''
            return pd.read_sql_query(query, conn)

    def get_trending_narratives(self, hours: int = 24) -> Dict[str, pd.DataFrame]:
        """Identify trending narratives and topics."""
        with sqlite3.connect(self.db_path) as conn:
            recent_tweets = pd.read_sql_query('''
                SELECT content, username, likes, retweets, sentiment_score, mentioned_assets
                FROM x_tweets
                WHERE timestamp > datetime('now', ?)
                AND is_market_moving = TRUE
                ORDER BY engagement_rate DESC
            ''', conn, params=(f'-{hours} hours',))
            if recent_tweets.empty:
                return {}
            all_words = []
            for content in recent_tweets['content']:
                words = re.findall(r'\b\w+\b', content.lower())
                all_words.extend(words)
            stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'is', 'are', 'was', 'were'}
            word_freq = pd.Series(all_words).value_counts()
            word_freq = word_freq[~word_freq.index.isin(stop_words)]
            themes = word_freq.head(20)
            asset_sentiment = pd.read_sql_query('''
                SELECT 
                    json_extract(mentioned_assets, '$[0].symbol') as asset,
                    AVG(sentiment_score) as avg_sentiment,
                    COUNT(*) as mention_count,
                    SUM(likes + retweets) as total_engagement
                FROM x_tweets
                WHERE timestamp > datetime('now', ?)
                AND mentioned_assets != '[]'
                GROUP BY asset
                ORDER BY mention_count DESC
            ''', conn, params=(f'-{hours} hours',))
            return {
                'trending_words': themes.to_frame('frequency'),
                'asset_sentiment': asset_sentiment
            }

    def search_recent_tweets(self, term: str, hours: int = 24) -> pd.DataFrame:
        """Search recent tweets mentioning a term."""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT t.*
                FROM x_tweets_fts f
                JOIN x_tweets t ON f.tweet_id = t.tweet_id
                WHERE x_tweets_fts MATCH ?
                AND t.timestamp > datetime('now', ?)
                ORDER BY t.timestamp DESC
            '''
            return pd.read_sql_query(query, conn, params=(term, f'-{hours} hours'))


async def setup_x_influence_tracker(db_path: str) -> XInfluenceTracker:
    """Initialize and return X influence tracker."""
    return XInfluenceTracker(db_path)
