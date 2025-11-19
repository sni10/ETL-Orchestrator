# src/database/repositories/interfaces/i_repository.py

from abc import ABC, abstractmethod


class IRepository(ABC):
    @abstractmethod
    def get_data(self, limit):
        pass

    @abstractmethod
    def save_data(self, data):
        pass

    @abstractmethod
    def close(self):
        pass
