# src/database/query_builder.py
from src.utils.config import Config


class QueryBuilder:
    def __init__(self, config: Config):
        self.config = config

    def generate_select_sql(self, limit: int) -> str:
        table = self.config.database_source_table
        source_alias = 't'
        columns_list = self.generate_select_columns()
        order_column = self.config.database_order_column

        join_sql = self.generate_join_sql(self.config.database_source_joins)
        where_sql = self.generate_condition_sql(self.config.database_source_conditions)

        query = f"""
            SELECT {columns_list}
            FROM {table} {source_alias}
            {join_sql}
            WHERE {where_sql}
            ORDER BY {source_alias}.{order_column} NULLS FIRST
            LIMIT {limit};
        """
        return query.strip()

    def generate_upsert_sql(self, table: str, sample_record: dict) -> str:
        keys = sample_record.keys()
        columns = ', '.join(keys)
        placeholders = ', '.join([f"%({k})s" for k in keys])

        conflict_keys = self.config.database_conflict_key
        conflict_key_str = ', '.join(conflict_keys)

        excluded_columns = set(self.config.excluded_columns)

        update_parts = []
        for k in keys:
            if k not in conflict_keys and k not in excluded_columns:
                update_parts.append(f"{k} = EXCLUDED.{k}")

        for default_col, default_val in self.config.default_values.items():
            update_parts.append(f"{default_col} = {default_val}")

        update_clause = ',\n'.join(update_parts)

        query = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_key_str})
            DO UPDATE SET
            {update_clause};
        """
        return query.strip()

    def generate_select_columns(self) -> str:
        columns = []
        source_table_alias = 't'
        source_columns = self.config.get_allowed_columns

        for column, alias in source_columns.items():
            columns.append(f"{source_table_alias}.{column} AS {alias}")

        joins = self.config.database_source_joins
        for i, join in enumerate(joins):
            join_alias = join.get('alias', f'j{i + 1}')
            for alias, column in join.get('columns', {}).items():
                columns.append(f"{join_alias}.{column} AS {alias}")

        return ", ".join(columns)

    def generate_condition_sql(self, conditions: list) -> str:
        source_alias = 't'
        version_column = self.config.database_version_column
        index_scale = self.config.database_version_scale

        sql_conditions = []

        for i, cond in enumerate(conditions):
            column = cond['column']
            operation = cond['operation']
            value = cond.get('value')
            op = cond.get('op', 'AND') if i > 0 else ''

            column_expr = column if '.' in column else f"{source_alias}.{column}"
            is_version_column = column == version_column or column_expr == f"{source_alias}.{version_column}"

            if is_version_column:
                hours = cond.get('value', 24)
                part = (
                    f"(TO_TIMESTAMP({column_expr} / {index_scale}) {operation} "
                    f"(CURRENT_TIMESTAMP - INTERVAL '{hours} hour') OR ({column_expr} IS NULL))"
                )
            else:
                if isinstance(value, dict) and 'raw' in value:
                    val_str = value['raw']
                elif isinstance(value, str) and value.upper() in {"NULL", "TRUE", "FALSE"}:
                    val_str = value.upper()
                elif operation.lower() in ("in", "not in") and isinstance(cond.get('values'), list):
                    values_list = ", ".join(f"'{v}'" for v in cond['values'])
                    val_str = f"({values_list})"
                else:
                    val_str = f"'{value}'" if isinstance(value, str) else value

                part = f"{column_expr} {operation} {val_str}"

            if op:
                sql_conditions.append(f"{op} {part}")
            else:
                sql_conditions.append(part)

        return " ".join(sql_conditions) if sql_conditions else "1=1"

    def generate_join_sql(self, joins_config: list) -> str:
        if not joins_config:
            return ""

        join_clauses = []
        for i, join in enumerate(joins_config):
            join_type = join.get('type', 'INNER')
            table = join['table']
            alias = join.get('alias', f'j{i + 1}')
            on_conditions = join.get('on', [])

            on_parts = []
            for cond in on_conditions:
                left = f"t.{cond['left']}"
                right = f"{alias}.{cond['right']}"
                on_parts.append(f"{left} = {right}")

            on_clause = " AND ".join(on_parts)
            join_clauses.append(f"{join_type} JOIN {table} {alias} ON {on_clause}")

        return "\n".join(join_clauses)
