# Simplified Telegram alert system
import asyncio
import logging
import sqlite3
from typing import Dict, Any
from dataclasses import dataclass

from telegram import Bot

from config import get_config

logger = logging.getLogger(__name__)

@dataclass
class Alert:
    symbol: str
    message: str
    severity: str = 'info'

class TelegramAlertSystem:
    def __init__(self, db_path: str = get_config("DB_PATH", "opus.db")):
        self.token = get_config('TELEGRAM_BOT_TOKEN')
        self.chat_id = get_config('TELEGRAM_CHAT_ID')
        self.bot = None
        if not self.token or not self.chat_id:
            logger.warning("Telegram credentials not provided; alerts disabled")
        else:
            try:
                self.bot = Bot(self.token)
            except Exception as e:  # pragma: no cover - external lib
                logger.warning(f"Failed to initialize Telegram bot: {e}")
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    message TEXT,
                    severity TEXT,
                    timestamp TEXT
                )"""
            )
            conn.commit()

    async def send_alert(self, alert: Alert):
        if not self.bot:
            logger.warning("Telegram alerts disabled, cannot send alert")
            return
        await self.bot.send_message(chat_id=self.chat_id, text=f"[{alert.severity}] {alert.symbol} - {alert.message}")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO alerts (symbol, message, severity, timestamp) VALUES (?, ?, ?, datetime('now'))",
                (alert.symbol, alert.message, alert.severity)
            )
            conn.commit()


async def main():
    alerts = TelegramAlertSystem()
    await alerts.send_alert(Alert('TEST', 'System initialized'))

if __name__ == '__main__':
    asyncio.run(main())
