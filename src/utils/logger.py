# src/utils/logger.py
import logging
import os

from src.utils.slack_webhook_handler import SlackWebhookHandler


def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # INFO handler - ТОЛЬКО INFO/WARNING
        info_url = os.getenv("SLACK_INFO_WEBHOOK_URL")
        if info_url:
            info_handler = SlackWebhookHandler(webhook_url=info_url, level=logging.INFO, environment=os.getenv("APP_ENV"))
            info_handler.addFilter(lambda record: record.levelno < logging.ERROR)
            logger.addHandler(info_handler)

        # ERROR handler - ТОЛЬКО ERROR/CRITICAL
        error_url = os.getenv("SLACK_ERROR_WEBHOOK_URL")
        if error_url:
            error_handler = SlackWebhookHandler(webhook_url=error_url, level=logging.ERROR, environment=os.getenv("APP_ENV"))
            logger.addHandler(error_handler)

    return logger
