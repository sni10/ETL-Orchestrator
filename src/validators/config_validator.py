from src.validators.base_validator import ValidationError
from src.validators.broker_validator import BrokerValidator
from src.validators.database_validator import DatabaseValidator
from src.utils.logger import setup_logger
from src.utils.config import Config
from typing import Dict, List, Any


class ConfigValidator:

    def __init__(self):
        self.logger = setup_logger(__name__)
        self.all_errors: List[ValidationError] = []

    def validate_config(self, config: Config) -> tuple[bool, List[ValidationError]]:
        """Валидирует Config объект"""
        self.all_errors.clear()

        db_validator = DatabaseValidator()
        db_valid = db_validator.validate(config)
        self.all_errors.extend(db_validator.errors)

        broker_validator = BrokerValidator()
        broker_valid = broker_validator.validate(config)
        self.all_errors.extend(broker_validator.errors)

        is_valid = db_valid and broker_valid

        return is_valid, self.all_errors

    def get_validation_report(self) -> str:
        if not self.all_errors:
            return "✅ Конфигурация валидна"

        report = f"❌ Найдено {len(self.all_errors)} ошибок:\n"
        for i, error in enumerate(self.all_errors, 1):
            report += f"{i}. {error.field}: {error.message}\n"

        return report


def validate_settings(settings: Dict[str, Any]) -> None:
    """Принимает сырой dict settings, создает Config и валидирует"""
    config = Config(settings)
    validator = ConfigValidator()
    is_valid, _ = validator.validate_config(config)

    if not is_valid:
        validator.logger.error(validator.get_validation_report())
        raise ValueError("Конфигурация содержит ошибки")
