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

## Setup

Create a Python environment (conda or `venv`) and install the
dependencies listed in `requirements.txt`:

```bash
conda create -n opus python=3.10
conda activate opus
pip install -r requirements.txt
```


## Usage

Set the following environment variables to enable the various collectors:

```bash
export FRED_API_KEY=your_fred_key
export DUNE_API_KEY=your_dune_key
export DUNE_QUERY_ID=123456
export ALPHA_VANTAGE_KEY=your_alpha_key
export POLYGON_KEY=your_polygon_key
export GOOGLE_CLOUD_PROJECT=your_gcp_project
export TELEGRAM_BOT_TOKEN=your_bot_token
export TELEGRAM_CHAT_ID=your_chat_id
export QUICKNODE_RPC=https://api.mainnet-beta.solana.com
export SOLANA_WALLET=YourWalletAddress
```

Unset values simply disable the related module.

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

## Setup

1. Install Python 3.10 or later. A Conda environment works well:

```bash
conda create -n opus4 python=3.10
conda activate opus4
pip install -r requirements.txt
```

2. Set the required API keys as environment variables before running:

```
FRED_API_KEY=your_fred_key
DUNE_API_KEY=your_dune_key
ALPHA_VANTAGE_KEY=your_alpha_key
POLYGON_KEY=your_polygon_key
GOOGLE_CLOUD_PROJECT=your_project_id
TELEGRAM_BOT_TOKEN=token
TELEGRAM_CHAT_ID=chat
```

3. Run modules individually or execute `python main_pipeline.py` to call
every collector once.

Note that free API tiers typically provide only limited historical depth.
Fetching years of minute data may require paid plans or looping requests
over small time windows.
