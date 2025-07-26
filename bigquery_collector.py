# Simplified BigQuery data collector for Reddit and blockchain data
import datetime
import logging
import sqlite3
from typing import List
import pandas as pd
from google.cloud import bigquery

from config import get_config

logger = logging.getLogger(__name__)


class BigQueryDataCollector:
    def __init__(self, db_path: str = get_config("DB_PATH", "opus.db"), project_id: str = None):
        self.db_path = db_path
        self.project_id = project_id or get_config('GOOGLE_CLOUD_PROJECT')
        self.client = bigquery.Client(project=self.project_id)
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reddit_posts (
                    id TEXT PRIMARY KEY,
                    subreddit TEXT,
                    title TEXT,
                    created_utc INTEGER,
                    score INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS reddit_posts_fts
                USING fts5(id UNINDEXED, title)
                """
            )
            conn.commit()

    def fetch_reddit_posts(self, subreddits: List[str], days_back: int = 7):
        subreddit_list = ",".join(f"'{s}'" for s in subreddits)
        start = int((datetime.datetime.utcnow() - datetime.timedelta(days=days_back)).timestamp())
        query = f"""
        SELECT id, subreddit, title, created_utc, score
        FROM `fh-bigquery.reddit_posts.*`
        WHERE subreddit IN ({subreddit_list}) AND created_utc >= {start}
        LIMIT 1000
        """
        df = self.client.query(query).to_dataframe()
        with sqlite3.connect(self.db_path) as conn:
            df.to_sql('reddit_posts', conn, if_exists='append', index=False)
            conn.executemany(
                'INSERT OR REPLACE INTO reddit_posts_fts (id, title) VALUES (?, ?)',
                df[['id', 'title']].itertuples(index=False, name=None)
            )
            conn.commit()

    def search_recent_posts(self, term: str, days_back: int = 7) -> pd.DataFrame:
        """Search recent Reddit posts mentioning a term."""
        start = int((datetime.datetime.utcnow() - datetime.timedelta(days=days_back)).timestamp())
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT p.*
                FROM reddit_posts_fts f
                JOIN reddit_posts p ON f.id = p.id
                WHERE reddit_posts_fts MATCH ?
                AND p.created_utc >= ?
                ORDER BY p.created_utc DESC
            '''
            return pd.read_sql_query(query, conn, params=(term, start))

if __name__ == '__main__':
    collector = BigQueryDataCollector()
    collector.fetch_reddit_posts(['bitcoin', 'ethereum'])
