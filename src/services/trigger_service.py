# src/services/trigger_service.py

from psycopg2 import connect, DatabaseError
from src.database.repositories.repository_factory import RepositoryFactory
from src.utils.logger import setup_logger
from src.utils.config import Config


class TriggerService:
    def __init__(self, config: Config):
        self.logger = setup_logger(__name__)
        self.config = config
        self.repository = RepositoryFactory.create_trigger_repository(config)

    def run_trigger(self):
        try:
            self.repository.check_stalled_suppliers()
        except DatabaseError as de:
            self.logger.error(f"Database error occurred: {de}")
            raise RuntimeError(f"Critical Database error: {de}")
