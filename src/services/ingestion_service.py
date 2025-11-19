# src/services/ingestion_service.py
import time
from psycopg2 import DatabaseError

from src.utils.config import Config
from src.database.repositories.generic_repository import GenericRepository
from src.message_broker.i_message_client import IMessageClient
from src.utils.logger import setup_logger
from src.utils.serializer import Serializer


class IngestionService:
    def __init__(self, config: Config, serializer: Serializer, repository: GenericRepository,
                 client: IMessageClient
                 ):
        self.logger = setup_logger(__name__)
        self.config: Config = config
        self.serializer: Serializer = serializer
        self.repository: GenericRepository = repository
        self.client: IMessageClient = client

    def ingest_data(self):
        self.repository.connection.connect()
        version = int(time.time() * 1e6)
        rows_produced = []

        lag = self.client.get_queue_size()
        available_space = self.config.database_queue_size_limit - lag

        if available_space <= 0:
            return

        batch_limit = min(self.config.database_batch_size, available_space)
        try:
            rows = self.repository.get_data(limit=batch_limit)
            if not rows:
                return

            map_before_kafka_produce_func = self.config.map_before_kafka_produce

            # TODO: separate produce single row and collected mapped data
            if map_before_kafka_produce_func and callable(map_before_kafka_produce_func):
                structured = map_before_kafka_produce_func(rows, version)
                self.client.send_message(structured)
                for row in rows:
                    serialized_row = self.serializer.serialize(row, version)
                    rows_produced.append(self.serializer.deserialize(serialized_row))
            else:
                for row in rows:
                    serialized_row = self.serializer.serialize(row, version)

                    modify_fn = self.config.modify_data_function
                    if callable(modify_fn):
                        record, task = modify_fn(serialized_row)
                        self.client.send_message(task)
                    else:
                        record = serialized_row
                        self.client.send_message(record)

                    rows_produced.append(record)

            if rows_produced:
                self.repository.upsert_data(rows_produced)

        except DatabaseError as de:
            self.logger.error(f"Database error: {de}")
            raise RuntimeError("Critical database error") from de
        finally:
            self.client.close()
            self.repository.close()
