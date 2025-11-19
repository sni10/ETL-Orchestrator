import psycopg2
from tabulate import tabulate
from src.utils.logger import setup_logger
from src.utils.config import Config


class TriggerRepository:
    def __init__(self, connection, config: Config):
        self.connection = connection
        self.logger = setup_logger(__name__)
        self.config = config
        self.cursor = connection.cursor()

    def check_stalled_suppliers(self):
        # Для health check используем сырые settings через config.settings
        # так как это специфичные поля для статовых DAGов
        table = self.config.database_source_table
        join_table = self.config.settings.get('database', {}).get('source', {}).get('join', '')
        date_field = self.config.settings.get('database', {}).get('health', {}).get('date_field', '')
        hours = self.config.settings.get('database', {}).get('health', {}).get('hours', 6)
        id_field = self.config.settings.get('database', {}).get('health', {}).get('id_field', '')
        join_field = self.config.settings.get('database', {}).get('health', {}).get('join_field', '')

        query = f"""
        SELECT po.{id_field}, MAX(po.{date_field}) as last_updated
        FROM {table} po
        JOIN {join_table} s ON po.{id_field} = s.{join_field}
        WHERE s.active = TRUE
        GROUP BY po.{id_field}
        HAVING MAX(po.{date_field}) < (NOW() - INTERVAL '{hours} hours')        
        ;
        """
        self.cursor.execute(query)
        stalled_suppliers = self.cursor.fetchall()

        if stalled_suppliers:
            warning_msg = "⚠️ Обнаружены поставщики, у которых нет обновлений более 6 часов:\n"
            warning_msg += tabulate(stalled_suppliers, headers=["Supplier ID", "Last Update"], tablefmt="grid")
            self.logger.error(warning_msg)
