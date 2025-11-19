from typing import Any


class Config:
    def __init__(self, settings: dict):
        self.settings = settings
        # можно делать парсинг/валидацию или кэшировать результаты

    @property
    def dag_id(self) -> str:
        return self.settings.get('dag_id', '')

    # ========== BROKER METHODS ==========

    @property
    def broker_type(self) -> str:
        """Возвращает тип брокера: 'kafka', 'rabbit', 'rabbitmq', 'redis'"""
        return self.settings.get('broker', {}).get('type', 'kafka')

    @property
    def broker_topic(self) -> str:
        """Возвращает название топика/очереди"""
        return self.settings.get('broker', {}).get('topic', '')

    @property
    def broker_kafka_bootstrap_servers(self) -> list[str]:
        """Возвращает список Kafka серверов (нормализация строка -> список)"""
        servers = self.settings.get('broker', {}).get('kafka_bootstrap_servers', [])
        if isinstance(servers, str):
            return [servers]
        return servers if isinstance(servers, list) else []

    @property
    def broker_kafka_consumer_group(self) -> str:
        """Возвращает Kafka consumer group"""
        return self.settings.get('broker', {}).get('consumer_group', '')

    @property
    def broker_rabbitmq_host(self) -> str:
        """Возвращает RabbitMQ хост"""
        return self.settings.get('broker', {}).get('host', '')

    @property
    def broker_rabbitmq_port(self) -> int:
        """Возвращает RabbitMQ порт"""
        return self.settings.get('broker', {}).get('port', 5672)

    @property
    def broker_rabbitmq_username(self) -> str:
        """Возвращает RabbitMQ username"""
        return self.settings.get('broker', {}).get('username', '')

    @property
    def broker_rabbitmq_password(self) -> str:
        """Возвращает RabbitMQ password"""
        return self.settings.get('broker', {}).get('password', '')

    @property
    def broker_redis_url(self) -> str:
        """Возвращает Redis URL"""
        return self.settings.get('broker', {}).get('redis_url', '')

    # ========== DATABASE CONNECTION METHODS ==========

    @property
    def database_connection_host(self) -> str:
        """Возвращает хост базы данных"""
        return self.settings.get('database', {}).get('connection', {}).get('host', '')

    @property
    def database_connection_port(self) -> int:
        """Возвращает порт базы данных"""
        return self.settings.get('database', {}).get('connection', {}).get('port', 5432)

    @property
    def database_connection_user(self) -> str:
        """Возвращает пользователя базы данных"""
        return self.settings.get('database', {}).get('connection', {}).get('user', '')

    @property
    def database_connection_password(self) -> str:
        """Возвращает пароль базы данных"""
        return self.settings.get('database', {}).get('connection', {}).get('password', '')

    @property
    def database_connection_dbname(self) -> str:
        """Возвращает имя базы данных"""
        return self.settings.get('database', {}).get('connection', {}).get('dbname', '')

    # ========== DATABASE SOURCE/TARGET METHODS ==========

    @property
    def database_source_table(self) -> str:
        """Возвращает source таблицу (используется в ingestion DAG'ах)"""
        return self.settings.get('database', {}).get('source', {}).get('table', '')

    @property
    def database_source_conditions(self) -> list:
        """Возвращает conditions для source таблицы"""
        return self.settings.get('database', {}).get('source', {}).get('conditions', [])

    @property
    def database_source_joins(self) -> list:
        """Возвращает joins для source таблицы"""
        return self.settings.get('database', {}).get('source', {}).get('joins', [])

    @property
    def database_target_table(self) -> str:
        return self.settings['database']['target']['table']

    @property
    def database_conflict_key(self) -> list[str]:
        """
        Возвращает conflict_key, ищет в target, потом в source.
        Нормализует строку (разделенную запятыми) в список.
        """
        # Сначала ищем в target, потом в source
        keys = self.settings.get('database', {}).get('target', {}).get('conflict_key')
        if keys is None:
            keys = self.settings.get('database', {}).get('source', {}).get('conflict_key', [])

        if isinstance(keys, str):
            return [k.strip() for k in keys.split(',')]
        return keys if isinstance(keys, list) else []

    @property
    def database_order_column(self) -> str:
        return self.settings['database']['order'].get('column', 'version')

    @property
    def database_version_column(self) -> str:
        return self.settings['database']['version'].get('column', 'version')

    @property
    def database_version_scale(self) -> int:
        return self.settings['database']['version'].get('scale', 1000000)

    @property
    def default_values(self) -> dict:
        """
        default_values: {
          'is_deleted': 'NULL',
          'updated_at': 'CURRENT_TIMESTAMP'
        }
        """
        return self.settings['database'].get('default_values', {})

    @property
    def database_batch_size(self) -> int:
        return self.settings['database'].get('batch_size', 100)

    @property
    def database_queue_size_limit(self) -> int:
        return self.settings['database'].get('queue_size_limit', 1000)

    @property
    def column_aliases_map(self) -> dict:
        """
        Возвращает mapping: key -> alias (имя в source -> имя в target)
        Пример:
        {
            'supplier_id': 'supplier_id',
            'name': 'name',
            'version': 'version',
            'catalog_id': 'catalog_id'  # из join'а
        }

        Для loading DAG'ов может использовать target.columns если source отсутствует.
        """
        # Пытаемся получить из source (ingestion DAG)
        source_columns = self.settings.get('database', {}).get('source', {}).get('columns', {})
        joins = self.settings.get('database', {}).get('source', {}).get('joins', [])

        result = dict(source_columns)

        for join in joins:
            join_columns = join.get('columns', {})
            result.update(join_columns)

        # Если source columns пустой, пытаемся использовать target.columns (loading DAG)
        if not result:
            target_columns = self.settings.get('database', {}).get('target', {}).get('columns', [])
            if isinstance(target_columns, list):
                # Если это список, создаем mapping column -> column
                result = {col: col for col in target_columns}
            elif isinstance(target_columns, dict):
                # Если это словарь, используем как есть
                result = target_columns

        return result

    def get_column_alias_by_key(self, key: str) -> str:
        """
        Возвращает alias (имя колонки в target) для данного key.
        Например: key='asin' -> 'external_id'
        """
        return self.column_aliases_map.get(key, '')

    @property
    def get_allowed_columns(self) -> dict:
        """
        Возвращает разрешенные колонки из source.columns.
        Для loading DAG'ов может возвращать из target.columns.
        """
        source_columns = self.settings.get('database', {}).get('source', {}).get('columns', {})

        # Если source columns пустой, пытаемся использовать target.columns (loading DAG)
        if not source_columns:
            target_columns = self.settings.get('database', {}).get('target', {}).get('columns', [])
            if isinstance(target_columns, list):
                # Если это список, создаем mapping column -> column
                source_columns = {col: col for col in target_columns}
            elif isinstance(target_columns, dict):
                # Если это словарь, используем как есть
                source_columns = target_columns

        return source_columns

    @property
    def all_column_aliases(self) -> list:
        """Возвращает список всех aliases (значений) из column_aliases_map"""
        return list(self.column_aliases_map.values())

    @property
    def all_column_keys(self) -> list:
        """Возвращает список всех keys из column_aliases_map"""
        return list(self.column_aliases_map.keys())

    # ========== ADDITIONAL METHODS ==========

    @property
    def excluded_columns(self) -> list:
        """Возвращает список исключенных колонок при upsert"""
        return self.settings.get('excluded_columns', [])

    @property
    def map_before_kafka_produce(self):
        """Возвращает функцию map_before_kafka_produce если есть"""
        return self.settings.get('map_before_kafka_produce')

    @property
    def modify_data_function(self):
        """Возвращает функцию modify_data_function если есть"""
        return self.settings.get('modify_data_function')

    @property
    def message_filter(self) -> list:
        """Возвращает список фильтров сообщений"""
        return self.settings.get('message_filter', [])
