# src/database/connections/base_connection.py

from src.database.connections.interfaces.i_connection import IConnection


class BaseConnection(IConnection):
    def connect(self):
        raise NotImplementedError

    def cursor(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
