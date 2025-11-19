# src/database/connection_factory.py

import psycopg2
from psycopg2 import OperationalError

from src.database.connections.base_connection import BaseConnection
from src.utils.logger import setup_logger
from src.utils.config import Config


class ConnectionFactory:
    @staticmethod
    def create_connection(config: Config) -> BaseConnection:
        return PostgresConnection(config)


class PostgresConnection(BaseConnection):
    def __init__(self, config: Config):
        self.logger = setup_logger(__name__)
        self.config = config
        self.conn = None

    def connect(self):
        if not self.conn:
            try:
                self.conn = psycopg2.connect(
                    dbname=self.config.database_connection_dbname,
                    user=self.config.database_connection_user,
                    password=self.config.database_connection_password,
                    host=self.config.database_connection_host,
                    port=self.config.database_connection_port
                )
            except OperationalError as e:
                self.logger.error(f"{self.config.dag_id} - Ошибка подключения к базе данных: {e}")
                raise RuntimeError(f"{self.config.dag_id} - Не удалось подключиться к базе данных: {e}")

    def cursor(self):
        self.connect()  # Убедимся, что соединение установлено
        return self.conn.cursor()

    def commit(self):
        if self.conn:
            try:
                self.conn.commit()
            except OperationalError as e:
                self.logger.error(f"{self.config.dag_id} - Ошибка при коммите: {e}")
                raise RuntimeError(f"{self.config.dag_id} - Ошибка при коммите: {e}")

    def close(self):
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
            except OperationalError as e:
                self.logger.error(f"{self.config.dag_id} - Ошибка при закрытии подключения: {e}")
                raise RuntimeError(f"{self.config.dag_id} - Ошибка при закрытии подключения: {e}")