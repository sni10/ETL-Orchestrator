# src/services/loading_service.py
from src.utils.config import Config
from src.database.repositories.generic_repository import GenericRepository
from src.message_broker.i_message_client import IMessageClient
from src.utils.serializer import Serializer
from src.utils.logger import setup_logger
from psycopg2 import DatabaseError


class LoadingService:
    def __init__(self, config: Config, serializer: Serializer, repository: GenericRepository, client: IMessageClient):
        self.logger = setup_logger(__name__)

        self.config: Config = config
        self.serializer: Serializer = serializer
        self.repository: GenericRepository = repository
        self.client: IMessageClient = client

        self.current_version = 0
        self.rows = []

    def load_data(self):
        self.repository.connection.connect()

        rows_to_insert = []  # TODO: need dynamic item Class
        processed = 0

        try:
            messages_to_process = self.client.get_queue_size()
            while processed <= messages_to_process:
                messages = self.client.consume_messages(max_records=self.config.database_batch_size)

                if not messages:
                    break

                rows = self.serializer.message_filter(messages)
                rows = [self.serializer.deserialize(row) for row in rows]
                rows_to_insert.extend(rows)

                processed += len(rows)

            if rows_to_insert:
                self.repository.upsert_data(rows_to_insert)

            total_skipped = self.repository.get_total_skipped_count()
            last_invalid_record = self.repository.get_last_invalid_record()

            if total_skipped > 0:
                self.logger.error(f"{self.config.dag_id} - Total issues encountered: {total_skipped}")
                if last_invalid_record:
                    self.logger.error(f"Example of an invalid record: {last_invalid_record}")

            self.repository.clear_data()
            return self.rows

        except DatabaseError as de:
            self.logger.error(f"{self.config.dag_id} - load_data service - Ошибка при работе с базой данных load_data: {de}")
            raise
        finally:
            self.client.close()
            self.repository.close()
