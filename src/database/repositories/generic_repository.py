# src/database/repositories/generic_repository.py
from psycopg2._json import Json
from src.utils.config import Config
from src.database.repositories.base_repository import BaseRepository
from src.utils.logger import setup_logger
from psycopg2.extras import execute_batch
from psycopg2 import DatabaseError
from src.database.query_builder import QueryBuilder


class GenericRepository(BaseRepository):
    def __init__(self, connection, config: Config):
        self.connection = connection
        self.config: Config = config
        self.logger = setup_logger(__name__)
        self.query_builder = QueryBuilder(config)
        self.total_skipped = 0
        self.empty_upc_count = 0
        self.last_invalid_record = None
        self.last_empty_upc_record = None

    def get_total_skipped_count(self):
        return self.total_skipped

    def get_last_invalid_record(self):
        return self.last_invalid_record

    def get_empty_upc_count(self):
        return self.empty_upc_count

    def get_last_empty_upc_record(self):
        return self.last_empty_upc_record

    def clear_data(self):
        self.total_skipped = 0
        self.last_invalid_record = None

    def get_data(self, limit):
        try:
            cursor = self.connection.cursor()
            query = self.query_builder.generate_select_sql(limit)

            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except DatabaseError as e:
            self.logger.error(f"{self.config.dag_id} - get_data - Ошибка при получении данных: {e}")
            raise

    def upsert_data(self, rows):
        """
        Универсальный метод, который заменяет save_data() и update_data().
        Выполняет вставку или обновление (ON CONFLICT).
        """
        if not rows:
            return

        chunk_size = self.config.database_batch_size

        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            valid_chunk = self.process_chunk(chunk)

            if not valid_chunk:
                continue

            query = self.query_builder.generate_upsert_sql(
                table=self.config.database_target_table,
                sample_record=valid_chunk[0]
            )
            print(query)
            cursor = self.connection.cursor()
            try:

                for row in valid_chunk:
                    if isinstance(row.get('asins', None), (list, dict)):
                        row['asins'] = Json(row['asins'])
                    if isinstance(row.get('asin_upcs', None), (list, dict)):
                        row['asin_upcs'] = Json(row['asin_upcs'])

                execute_batch(cursor, query, valid_chunk)
                self.connection.commit()
            except DatabaseError as e:
                self.logger.error(f"{self.config.dag_id} - upsert_data - Ошибка: {e}")
                raise
            finally:
                cursor.close()

    def process_chunk(self, chunk: list[dict]) -> list[dict]:
        """
        Обрабатывает один чанк: фильтрует и валидирует записи
        """
        allowed_columns = set(self.config.get_allowed_columns)
        valid_chunk = []

        for record in chunk:
            if not self.is_valid_record(record, allowed_columns):
                self.total_skipped += 1
                self.last_invalid_record = record
                continue

            if self.is_empty_upc(record):
                self.empty_upc_count += 1
                self.last_empty_upc_record = record
                continue

            filtered = {k: v for k, v in record.items() if k in allowed_columns}
            valid_chunk.append(filtered)

        return valid_chunk

    def is_valid_record(self, record: dict, allowed_columns: set) -> bool:
        """
        Проверяет, содержит ли запись все нужные поля
        """
        return allowed_columns.issubset(record.keys())

    def is_empty_upc(self, record: dict) -> bool:
        """
        Проверяет, является ли поле upc пустым
        """
        return 'upc' in record and (record['upc'] or '').strip() == ""

    def select_raw(self, sql: str, params=None):
        """
        Execute a raw SELECT/CTE query and return all rows.
        Security: only SELECT/WITH statements are allowed.
        """
        if not isinstance(sql, str):
            raise ValueError("SQL must be a string")
        first = sql.lstrip().lower()
        if not (first.startswith("select") or first.startswith("with")):
            raise ValueError("Only SELECT/WITH statements are allowed")
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or None)
            rows = cursor.fetchall()
            return rows
        except DatabaseError as e:
            self.logger.error(f"{self.config.dag_id} - select_raw - Ошибка при выполнении SELECT: {e}")
            raise
        finally:
            cursor.close()

    def select_raw_dicts(self, sql: str, params=None):
        """
        Execute a raw SELECT/CTE query and return list of dicts (column->value).
        """
        if not isinstance(sql, str):
            raise ValueError("SQL must be a string")
        first = sql.lstrip().lower()
        if not (first.startswith("select") or first.startswith("with")):
            raise ValueError("Only SELECT/WITH statements are allowed")
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or None)
            colnames = [desc[0] for desc in cursor.description]
            data = []
            for row in cursor.fetchall():
                data.append({colnames[i]: row[i] for i in range(len(colnames))})
            return data
        except DatabaseError as e:
            self.logger.error(f"{self.config.dag_id} - select_raw_dicts - Ошибка при выполнении SELECT: {e}")
            raise
        finally:
            cursor.close()

    def execute_raw(self, sql: str, params=None):
        """
        Execute raw SQL (DDL/DML). Intended for function installs and schema prep.
        Commits on success.
        """
        if not isinstance(sql, str):
            raise ValueError("SQL must be a string")
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or None)
            self.connection.commit()
        except DatabaseError as e:
            self.logger.error(f"{self.config.dag_id} - execute_raw - Ошибка при выполнении SQL: {e}")
            raise
        finally:
            cursor.close()

    def close(self):
        self.connection.close()
