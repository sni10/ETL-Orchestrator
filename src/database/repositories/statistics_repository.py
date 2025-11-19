import psycopg2
from tabulate import tabulate
from datetime import datetime, timedelta
from src.utils.logger import setup_logger
from src.utils.config import Config


class StatisticsRepository:
    def __init__(self, connection, config: Config):
        self.connection = connection
        self.logger = setup_logger(__name__)
        self.config = config
        self.cursor = connection.cursor()

    def fetch_stats(self):
        table = self.config.database_source_table
        query = f"""
        SELECT p.supplier_id AS supplier_id,
               to_char(to_timestamp((p.version / 1000000)::double precision),
                       'YYYY-MM-DD HH24:MI:SS.US'::text) as version_date,
               to_char(p.updated_at, 'YYYY-MM-DD'::text) as upd,
               count(1) filter (where p.is_deleted is not null) as is_deleted,
               count(1) as total_rows
        FROM {table} p
        GROUP BY p.supplier_id, version_date, upd
        ORDER BY version_date DESC;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def print_daily_stats(self):
        results = self.fetch_stats()
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        headers = ["Supplier", "Today Up/Del", "Yesterday Up/Del", "Total Up/Del"]
        table_data = []

        daily_stats = {}

        for supplier_id, version_date, upd, is_deleted, total_rows in results:
            if supplier_id not in daily_stats:
                daily_stats[supplier_id] = {'today': {'new': 0, 'deleted': 0},
                                              'yesterday': {'new': 0, 'deleted': 0},
                                              'total_active': 0,
                                              'total_deleted': 0}
            stats = daily_stats[supplier_id]
            if upd == str(today):
                stats['today']['new'] += total_rows - is_deleted
                stats['today']['deleted'] += is_deleted
            elif upd == str(yesterday):
                stats['yesterday']['new'] += total_rows - is_deleted
                stats['yesterday']['deleted'] += is_deleted
            stats['total_active'] += total_rows - is_deleted
            stats['total_deleted'] += is_deleted

        for supplier, data in daily_stats.items():

            table_data.append([
                supplier, f"{data['today']['new']} / {data['today']['deleted']}", f"{data['yesterday']['new']} / {data['yesterday']['deleted']}", f"{data['total_active']} / {data['total_deleted']}"
            ])

        table = tabulate(table_data, headers=headers, tablefmt="grid")

        self.logger.info("\n" + table)

