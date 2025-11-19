# src/database/repositories/base_repository.py

from src.database.repositories.interfaces.i_repository import IRepository


class BaseRepository(IRepository):
    def get_data(self, limit: int) -> []:
        raise NotImplementedError

    def generate_select_sql(self, limit: int) -> str:
        raise NotImplementedError

    def save_data(self, data):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
