# src/validators/database_validator.py
from typing import TYPE_CHECKING
from .base_validator import BaseValidator

if TYPE_CHECKING:
    from src.utils.config import Config


class DatabaseValidator(BaseValidator):
    def validate(self, config: 'Config') -> bool:
        self.errors.clear()

        self._validate_connection(config)

        return len(self.errors) == 0

    def _validate_connection(self, config: 'Config') -> None:
        """Валидация настроек подключения к БД"""
        host = config.database_connection_host
        port = config.database_connection_port
        user = config.database_connection_user
        password = config.database_connection_password
        dbname = config.database_connection_dbname

        if not host:
            self.add_error('database.connection.host', "Поле 'host' обязательно")
        if not port or not (1 <= port <= 65535):
            self.add_error('database.connection.port', "Порт должен быть в диапазоне 1-65535")
        if not user:
            self.add_error('database.connection.user', "Поле 'user' обязательно")
        if not password:
            self.add_error('database.connection.password', "Поле 'password' обязательно")
        if not dbname:
            self.add_error('database.connection.dbname', "Поле 'dbname' обязательно")
