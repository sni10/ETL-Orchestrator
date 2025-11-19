# src/database/connections/interfaces/i_connection.py

from abc import ABC, abstractmethod


class IConnection(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def cursor(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def close(self):
        pass
