# Opus4.725

This repository contains a collection of experimental data collection
and analysis utilities.  Each module can be used independently and
relies mostly on free API tiers.

## Modules

- **config.py** – simple helper to access configuration values from environment variables.
- **fred_data_collector.py** – asynchronous client for the FRED API capable of storing 10 years of economic data in SQLite.
- **solana_wallet_tracker.py** – very small Solana wallet tracker example.
- **dune_collector.py** – execute Dune Analytics queries and store results.
- **alpha_polygon_analyzer.py** – fetch basic data from Alpha Vantage and Polygon.io.
- **bigquery_collector.py** – read Reddit posts from the public BigQuery dataset.
- **ccxt_collector.py** – example using CCXT to pull crypto OHLCV data.
- **telegram_alerts.py** – minimal Telegram alert sender.
- **main_pipeline.py** – orchestrates all modules for a single run.

## Usage

Set any required API keys via environment variables. At minimum you should
set `FRED_API_KEY` for the FRED collector. Optionally configure
`DUNE_QUERY_ID`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, and others
as needed.

To run the individual modules you can invoke them directly:

```bash
python fred_data_collector.py
```

To run a single orchestrated pipeline that touches each module once use:

```bash
python main_pipeline.py
```

The pipeline uses very small data pulls so it should run with free API
limits. All operations are wrapped in basic error handling so a failure
in one source will not stop the others.
