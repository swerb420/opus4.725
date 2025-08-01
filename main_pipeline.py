import asyncio
import logging

from alpha_polygon_analyzer import AlphaPolygonAnalyzer
from bigquery_collector import BigQueryDataCollector
from ccxt_collector import CCXTDataCollector
from dune_collector import DuneAnalyticsCollector
from fred_data_collector import FREDDataCollector
from solana_wallet_tracker import SolanaWalletTracker
from telegram_alerts import TelegramAlertSystem, Alert
from config import get_config
from database_optimizer import setup_database_optimization

logger = logging.getLogger(__name__)

async def run_fred(fred: FREDDataCollector):
    try:
        await fred.fetch_all_series_data()
    except Exception as e:
        logger.error(f"FRED fetch failed: {e}")

async def run_dune(dune: DuneAnalyticsCollector):
    query_id = get_config('DUNE_QUERY_ID')
    if not query_id:
        logger.warning('DUNE_QUERY_ID not set')
        return
    try:
        dune.run_query(query_id)
    except Exception as e:
        logger.error(f"Dune query failed: {e}")

async def run_alpha_polygon(analyzer: AlphaPolygonAnalyzer):
    try:
        await analyzer.fetch_and_store(['AAPL', 'MSFT'])
    except Exception as e:
        logger.error(f"AlphaPolygon fetch failed: {e}")

async def run_bigquery(bq: BigQueryDataCollector):
    try:
        bq.fetch_reddit_posts(['bitcoin', 'ethereum'])
    except Exception as e:
        logger.error(f"BigQuery fetch failed: {e}")

async def run_ccxt(ccxt_col: CCXTDataCollector):
    try:
        await ccxt_col.fetch_ohlcv('BTC/USDT')
    except Exception as e:
        logger.error(f"CCXT fetch failed: {e}")
    finally:
        await ccxt_col.close()

async def run_solana(tracker: SolanaWalletTracker):
    wallet = get_config('SOLANA_WALLET')
    if not wallet:
        logger.warning('SOLANA_WALLET not set')
        return
    try:
        await tracker.track_wallet(wallet)
    except Exception as e:
        logger.error(f"Solana tracking failed: {e}")

async def main():
    fred = FREDDataCollector()
    dune = DuneAnalyticsCollector()
    alpha_poly = AlphaPolygonAnalyzer()
    bigquery = BigQueryDataCollector()
    ccxt_col = CCXTDataCollector()
    solana_tracker = SolanaWalletTracker('solana_wallets.db')
    telegram = TelegramAlertSystem()

    optimizer = setup_database_optimization('crypto_data.db')
    optimizer.enable_wal_mode()
    optimizer.vacuum_and_analyze()

    semaphore = asyncio.Semaphore(3)

    async def limited(coro):
        async with semaphore:
            await coro

    tasks = [
        limited(run_fred(fred)),
        limited(run_dune(dune)),
        limited(run_alpha_polygon(alpha_poly)),
        limited(run_bigquery(bigquery)),
        limited(run_ccxt(ccxt_col)),
        limited(run_solana(solana_tracker)),
    ]

    await asyncio.gather(*tasks)

    await telegram.send_alert(Alert(symbol='SYSTEM', message='Pipeline run completed', severity='info'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main())
