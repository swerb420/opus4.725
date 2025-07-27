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
- **database_optimizer.py** – advanced SQLite partitioning and optimization utilities.
- **historical_data_collector.py** – gather long-term crypto data from multiple sources.
- **enhanced_coin_manager.py** – manage coins with auto-discovery and Telegram controls.

- **telegram_alerts.py** – minimal Telegram alert sender.
- **main_pipeline.py** – orchestrates all modules for a single run.

## Setup

Install **Python 3.10 or later** and create a virtual environment. The
examples below use Conda but any `venv` style environment works:

```bash
conda create -n opus python=3.10
conda activate opus
pip install -r requirements.txt
```

Set the following environment variables to enable the various collectors.
Unset values simply disable the related module:

```bash
export FRED_API_KEY=your_fred_key
export DUNE_API_KEY=your_dune_key
export DUNE_QUERY_ID=123456
export ALPHA_VANTAGE_KEY=your_alpha_key
export POLYGON_KEY=your_polygon_key
export GOOGLE_CLOUD_PROJECT=your_gcp_project
export TELEGRAM_BOT_TOKEN=your_bot_token
export TELEGRAM_CHAT_ID=your_chat_id
export APIFY_TOKEN=your_apify_token
export QUICKNODE_RPC=https://api.mainnet-beta.solana.com
export SOLANA_WALLET=YourWalletAddress
```


To run the individual modules you can invoke them directly:

```bash
python fred_data_collector.py
```

On macOS the system Python may require the `python3` command:

```bash
python3 fred_data_collector.py
```

To run a single orchestrated pipeline that touches each module once use:

```bash
python main_pipeline.py
```

On macOS:

```bash
python3 main_pipeline.py
```

The pipeline uses very small data pulls so it should run with free API
limits. All operations are wrapped in basic error handling so a failure
in one source will not stop the others.

Minute level historical data is often restricted on free API tiers. When
requesting long date ranges, especially with the CCXT or market modules,
expect only partial results unless you upgrade the respective services.

Note that free API tiers typically provide only limited historical depth;
fetching years of minute data may require paid plans or looping requests
over small time windows.
