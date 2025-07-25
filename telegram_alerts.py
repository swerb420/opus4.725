# Simplified Telegram alert system
import asyncio
import logging
import sqlite3
from typing import Dict, Any
from dataclasses import dataclass

from telegram import Bot

from config import get_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class Alert:
    symbol: str
    message: str
    severity: str = 'info'

class TelegramAlertSystem:
    def __init__(self, db_path: str = 'alerts.db'):
        self.token = get_config('TELEGRAM_BOT_TOKEN')
        self.chat_id = get_config('TELEGRAM_CHAT_ID')
        self.bot = Bot(self.token)
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
