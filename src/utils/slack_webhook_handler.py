# src/utils/slack_webhook_handler.py

import logging
import requests
import time
from datetime import datetime


class SlackWebhookHandler(logging.Handler):
    def __init__(self, webhook_url: str, level: int, environment: str = "prod"):
        super().__init__(level=level)
        self.setLevel(level)
        self.webhook_url = webhook_url
        self.environment = environment.upper()

    def format_monolog_style(self, record: logging.LogRecord) -> str:
        timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname
        message = record.getMessage()
        return f"[{timestamp_str}] {self.environment}.{level}: \n{message}"

    def emit(self, record: logging.LogRecord):

        color_map = {
            "DEBUG": "good",
            "INFO": "good",  # Изменено на зеленый для INFO
            "WARNING": "warning",
            "ERROR": "danger",
            "CRITICAL": "danger",
        }

        color = color_map.get(record.levelname, "#439FE0")

        if self.environment == 'PROD':
            server = '145'
        else:
            server = '183'

        payload = {
            "username": f"{self.environment} Distributor Airflow {server}",
            "icon_emoji": ":robot_face:",
            "text": f"```{self.format_monolog_style(record)}```",
            "attachments": [
                {
                    "fallback": self.format_monolog_style(record),
                    "color": color,
                    "title": f"{self.environment} {record.levelname} LOG",
                    "footer": f"Distributor Airflow {self.environment}",
                    "ts": int(time.time())
                }
            ]
        }

        if record.levelname in ['INFO', 'info']:
            icon_emoji = ":chart_with_upwards_trend:"
            payload = {
                "username": f"{record.levelname} Statistics Distributor EDI {server}",
                "icon_emoji": icon_emoji,
                "text": f"```{self.format_monolog_style(record)}```",
                "attachments": [
                    {
                        "color": color,
                        "title": f"{record.levelname} Airflow",
                        "footer": f"Distributor Airflow {self.environment} Environment",
                        "ts": int(time.time())
                    }
                ]
            }

        requests.post(self.webhook_url, json=payload, timeout=5)

