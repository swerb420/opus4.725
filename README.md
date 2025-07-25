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

## Usage

Set the environment variable `FRED_API_KEY` with your FRED API key and run:

```bash
python fred_data_collector.py
```

The script will download data for a set of predefined series (`GDP`, `CPIAUCSL`, `UNRATE`, `FEDFUNDS`, `DGS10`) and store them in `fred_data.db`.
