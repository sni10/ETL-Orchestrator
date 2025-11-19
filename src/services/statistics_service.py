# src/services/statistics_service.py

from psycopg2 import connect, DatabaseError
from src.database.repositories.repository_factory import RepositoryFactory
from src.utils.logger import setup_logger
from src.utils.config import Config


class StatisticsService:
    def __init__(self, config: Config):
        self.logger = setup_logger(__name__)
        self.config = config
        self.repository = RepositoryFactory.create_stat_repository(config)

    def run_statistics(self):
        try:
            self.repository.print_daily_stats()
        except DatabaseError as de:
            self.logger.error(f"{self.config.dag_id} - Database error occurred: {de}")
            raise RuntimeError(f"{self.config.dag_id} - Critical Database error: {de}")
