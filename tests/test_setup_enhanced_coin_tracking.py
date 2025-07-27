import asyncio
import unittest
from unittest.mock import patch
import sys
import types

# Stub minimal telegram module before importing enhanced_coin_manager
telegram_stub = types.ModuleType("telegram")
telegram_stub.InlineKeyboardButton = object
telegram_stub.InlineKeyboardMarkup = object
telegram_stub.ParseMode = object
telegram_ext_stub = types.ModuleType("telegram.ext")
telegram_ext_stub.CommandHandler = object
telegram_ext_stub.CallbackQueryHandler = object
sys.modules.setdefault("telegram", telegram_stub)
sys.modules.setdefault("telegram.ext", telegram_ext_stub)

import enhanced_coin_manager

class DummyExchange:
    def __init__(self):
        self.markets = {}
        self.symbols = []
    async def fetch_ticker(self, pair):
        return {}

class DummyCollector:
    def __init__(self):
        self.exchange = DummyExchange()

class DummyManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.called = False
        self.exchanges = None
    async def discover_new_coins(self, exchanges):
        self.called = True
        self.exchanges = exchanges
        return []
    def auto_add_trending(self, min_volume=0, min_exchanges=2):
        pass

class SetupTest(unittest.TestCase):
    def test_setup_uses_exchange_attribute(self):
        collector = DummyCollector()
        with patch.object(enhanced_coin_manager, 'CoinManager', DummyManager):
            manager = asyncio.run(
                enhanced_coin_manager.setup_enhanced_coin_tracking('test.db', collector)
            )
        self.assertTrue(manager.called)
        self.assertIsInstance(manager.exchanges, dict)

if __name__ == '__main__':
    unittest.main()
