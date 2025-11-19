# src/validators/base_validator.py
from abc import ABC, abstractmethod
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class ValidationError:
    field: str
    message: str


class BaseValidator(ABC):
    def __init__(self):
        self.errors: List[ValidationError] = []

    @abstractmethod
    def validate(self, config: Dict[str, Any]) -> bool:
        pass

    def add_error(self, field: str, message: str) -> None:
        self.errors.append(ValidationError(field, message))

    def _check_required_fields(self, config: Dict[str, Any], fields: List[str], prefix: str) -> None:
        """Проверка обязательных полей"""
        for field in fields:
            if field not in config or not config[field]:
                self.add_error(f'{prefix}.{field}', f"Поле '{field}' обязательно")

    def _validate_port(self, config: Dict[str, Any], prefix: str) -> None:
        """Валидация порта"""
        if 'port' in config:
            try:
                port = int(config['port'])
                if not (1 <= port <= 65535):
                    self.add_error(f'{prefix}.port', "Порт должен быть в диапазоне 1-65535")
            except (ValueError, TypeError):
                self.add_error(f'{prefix}.port', "Порт должен быть числом")