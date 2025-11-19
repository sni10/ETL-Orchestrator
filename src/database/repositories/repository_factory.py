# src/database/repositories/repository_factory.py

from src.utils.config import Config
from src.database.connection_factory import ConnectionFactory
from src.database.repositories.generic_repository import GenericRepository
from src.database.repositories.statistics_repository import StatisticsRepository
from src.database.repositories.trigger_repository import TriggerRepository


class RepositoryFactory:
    @staticmethod
    def create_repository(config: Config) -> GenericRepository:
        connection = ConnectionFactory.create_connection(config)
        return GenericRepository(connection, config)

    @staticmethod
    def create_stat_repository(config: Config):
        connection = ConnectionFactory.create_connection(config)
        return StatisticsRepository(connection, config)

    @staticmethod
    def create_trigger_repository(config: Config):
        connection = ConnectionFactory.create_connection(config)
        return TriggerRepository(connection, config)

