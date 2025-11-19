# src/serializer.py

import json
import re
from datetime import datetime, date
from decimal import Decimal
from psycopg2._json import Json

from src.utils.config import Config

DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$')
DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
NUMERIC_PATTERN = re.compile(r'^-?\d+(\.\d+)?$')


class Serializer:
    def __init__(self, config: Config):
        self.config = config

        # можно вытащить эти поля в настройки или сделать свойство
        self.json_fields = {"column_map_rules", "attributes", "asins", "asin_upcs"}
        self.string_fields = {"upc"}

    def serialize(self, data: tuple, version: int) -> dict:
        """
        Преобразует данные из строки БД в словарь для Kafka
        """
        fields = self.config.all_column_aliases
        record = {}

        for i, field in enumerate(fields):
            value = data[i]

            if isinstance(value, datetime):
                record[field] = value.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            elif isinstance(value, date):
                record[field] = value.strftime('%Y-%m-%d')
            elif isinstance(value, Decimal):
                record[field] = float(value)
            elif isinstance(value, str) and field in self.json_fields:
                record[field] = json.loads(value) if value else None
            else:
                record[field] = value

        version_field = self.config.database_version_column
        is_new_version = self.config.settings.get('new_version', True)

        if is_new_version:
            record[version_field] = version

        return record

    def deserialize(self, row_dict: dict) -> dict:
        """
        Преобразует Kafka/JSON payload обратно в Python-формат
        """
        record = {}
        fields = self.config.all_column_keys

        for field in fields:
            alias = self.config.get_column_alias_by_key(field)
            value = row_dict.get(alias)

            if field in self.json_fields:
                if isinstance(value, str):
                    value = json.loads(value)
                elif isinstance(value, dict):
                    value = Json(value)

            elif field in self.string_fields:
                value = str(value)

            elif isinstance(value, str):
                if DATETIME_PATTERN.match(value):
                    value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                elif DATE_PATTERN.match(value):
                    value = datetime.strptime(value, "%Y-%m-%d").date()
                elif NUMERIC_PATTERN.match(value):
                    value = float(value) if '.' in value else int(value)

            record[field] = value

        return record

    def message_filter(self, messages):
        comparison_operations = {
            "not in": lambda value, excluded_values: (
                any(v in excluded_values for v in value) if isinstance(value, list) else value in excluded_values
            )
        }

        message_filters = self.config.message_filter
        valid_columns = set(self.config.get_allowed_columns.keys())

        processed_rows = []

        for message in messages:
            # Если хоть один фильтр срабатывает (то есть сообщение надо ИГНОРИРОВАТЬ), оно НЕ попадает в processed_rows
            if any(self.check_filter_compliance(message, filter_rule, comparison_operations) for filter_rule in
                   message_filters):
                continue  # Пропускаем эту запись

            # Если сообщение прошло фильтры, оставляем только нужные колонки
            filtered_message = {k: v for k, v in message.items() if k in valid_columns}
            if filtered_message:
                processed_rows.append(filtered_message)

        return processed_rows

    def check_filter_compliance(self, message, filter_rule, comparison_operations):
        column = filter_rule["column"]
        message_value = message.get(column)
        operation = filter_rule["operation"]

        # Получаем функцию сравнения из comparison_operations
        comparison_function = comparison_operations.get(operation)

        if comparison_function:
            return comparison_function(message_value, set(filter_rule["values"]))

        return False  # Если операции нет, считаем, что запись проходит