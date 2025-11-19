# src/factories/service_factory.py
from src.utils.config import Config
from src.database.repositories.generic_repository import GenericRepository
from src.database.repositories.repository_factory import RepositoryFactory

from src.message_broker.factory import MessageClientFactory
from src.message_broker.i_message_client import IMessageClient
from src.utils.serializer import Serializer
from src.services.ingestion_service import IngestionService
from src.services.loading_service import LoadingService
from src.services.statistics_service import StatisticsService
from src.services.trigger_service import TriggerService


class ServiceFactory:

    @staticmethod
    def create_ingestion_service(settings):
        config: Config = Config(settings)
        serializer: Serializer = Serializer(config)
        repository: GenericRepository = RepositoryFactory.create_repository(config)
        client: IMessageClient = MessageClientFactory.create_client(config)
        return IngestionService(config, serializer, repository, client)

    @staticmethod
    def create_loading_service(settings):
        config: Config = Config(settings)
        serializer: Serializer = Serializer(config)
        client: IMessageClient = MessageClientFactory.create_client(config)
        repository: GenericRepository = RepositoryFactory.create_repository(config)
        return LoadingService(config, serializer, repository, client)

    @staticmethod
    def create_stat_service(settings):
        config: Config = Config(settings)
        return StatisticsService(config)

    @staticmethod
    def create_trigger_service(settings):
        config: Config = Config(settings)
        return TriggerService(config)
