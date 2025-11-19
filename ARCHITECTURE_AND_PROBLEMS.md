# Архитектура проекта: Анализ принципов и проблем

## Обзор

Данный документ содержит анализ архитектурных решений проекта **Оркестратор ETL-процессов на базе Apache Airflow** (репозиторий `up-distributor-airflow`). Проанализированы использованные паттерны, соблюдение принципов проектирования и выявленные проблемы.

---

## ✅ Использованные архитектурные принципы

### 1. Layered Architecture (Слоистая архитектура)

Проект следует четкому разделению на слои:

- **DAGs** (`dags/`) — оркестрация и конфигурация задач Airflow
- **Services** (`src/services/`) — бизнес-логика (IngestionService, LoadingService, StatisticsService, TriggerService)
- **Repositories** (`src/database/repositories/`) — слой доступа к данным
- **Message Brokers** (`src/message_broker/`) — интеграция с очередями сообщений

**Преимущества:**
- Четкое разделение ответственности
- Упрощенное тестирование
- Возможность замены слоев без влияния на другие части системы

**Примеры:**
```
DAG → ServiceFactory → Service → Repository → Database
DAG → ServiceFactory → Service → MessageClient → Broker
```

---

### 2. Factory Pattern (Фабричный паттерн)

Широко применяется для создания объектов:

- **ServiceFactory** (`src/factories/service_factory.py:18`) — создание сервисов
- **RepositoryFactory** — создание репозиториев
- **MessageClientFactory** — создание клиентов брокеров
- **ConnectionFactory** — создание подключений к БД

**Пример использования:**
```python
# src/factories/service_factory.py:18-23
@staticmethod
def create_ingestion_service(settings):
    config: Config = Config(settings)
    serializer: Serializer = Serializer(config)
    repository: GenericRepository = RepositoryFactory.create_repository(config)
    client: IMessageClient = MessageClientFactory.create_client(config)
    return IngestionService(config, serializer, repository, client)
```

**Преимущества:**
- Централизованная логика создания объектов
- Легкость добавления новых типов
- Скрытие сложности инициализации

---

### 3. Repository Pattern (Паттерн репозиторий)

Инкапсуляция логики доступа к данным:

- Интерфейс `IRepository` (`src/database/repositories/interfaces/i_repository.py:6`)
- Реализации: `GenericRepository`, `StatisticsRepository`, `TriggerRepository`

**Преимущества:**
- Изоляция SQL-кода от бизнес-логики
- Возможность замены источника данных
- Упрощение тестирования (mock репозиториев)

**Файлы:**
- `src/database/repositories/generic_repository.py`
- `src/database/repositories/statistics_repository.py`
- `src/database/repositories/trigger_repository.py`

---

### 4. Dependency Injection (Внедрение зависимостей)

Зависимости передаются через конструкторы:

```python
# src/factories/service_factory.py:18-23
config: Config = Config(settings)
serializer: Serializer = Serializer(config)
repository: GenericRepository = RepositoryFactory.create_repository(config)
client: IMessageClient = MessageClientFactory.create_client(config)
return IngestionService(config, serializer, repository, client)
```

```python
# src/services/ingestion_service.py:13-20
def __init__(self, config: Config, serializer: Serializer,
             repository: GenericRepository, client: IMessageClient):
    self.logger = setup_logger(__name__)
    self.config: Config = config
    self.serializer: Serializer = serializer
    self.repository: GenericRepository = repository
    self.client: IMessageClient = client
```

**Преимущества:**
- Слабая связанность компонентов
- Легкость тестирования
- Гибкость конфигурации

---

### 5. Strategy Pattern (Паттерн стратегия)

Различные реализации `IMessageClient` для работы с разными брокерами:

- `KafkaClient` (`src/message_broker/kafka/kafka_client.py`)
- `RedisClient` (`src/message_broker/redis/redis_client.py`)
- `RabbitMQClient` (`src/message_broker/rabbitmq/rabbitmq_client.py`)

**Интерфейс:**
```python
# src/message_broker/i_message_client.py:6-24
class IMessageClient(ABC):
    @abstractmethod
    def get_queue_size(self) -> int:
        pass

    @abstractmethod
    def consume_messages(self, max_records: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def send_message(self, message: Dict[str, Any]) -> None:
        pass
```

**Выбор стратегии:**
```python
# Через MessageClientFactory на основе config.broker_type
client = MessageClientFactory.create_client(config)
```

---

### 6. Configuration Object Pattern

Класс `Config` (`src/utils/config.py:4`) инкапсулирует доступ к настройкам через property-методы:

```python
@property
def broker_type(self) -> str:
    return self.settings.get('broker', {}).get('type', 'kafka')

@property
def database_source_table(self) -> str:
    return self.settings.get('database', {}).get('source', {}).get('table', '')
```

**Преимущества:**
- Единая точка доступа к конфигурации
- Валидация и нормализация значений
- Изоляция структуры словаря `settings`

---

## ❌ Нарушенные принципы и проблемы

### 1. Single Responsibility Principle (SRP)

**Проблема:** `GenericRepository` имеет слишком много ответственностей.

**Нарушения в `src/database/repositories/generic_repository.py`:**
```python
class GenericRepository:
    # Чтение данных
    def get_data(self, limit): ...                    # строка 38

    # Запись данных
    def upsert_data(self, rows): ...                  # строка 51

    # Валидация
    def is_valid_record(self, record, allowed): ...   # строка 113

    # Бизнес-правила
    def is_empty_upc(self, record): ...               # строка 119

    # Статистика
    self.total_skipped                                # строка 17
    self.empty_upc_count                              # строка 18

    # Raw SQL выполнение
    def select_raw(self, sql, params): ...            # строка 125
    def execute_raw(self, sql, params): ...           # строка 169
```

**Решение:**
- Вынести валидацию в отдельный `RecordValidator`
- Создать `StatisticsCollector` для сбора метрик
- Разделить на `ReadRepository` и `WriteRepository`
- UPC-специфичную логику переместить в бизнес-слой

---

### 2. Liskov Substitution Principle (LSP)

**Проблема:** Интерфейс не соответствует реальной реализации.

**Интерфейс (`src/database/repositories/interfaces/i_repository.py:12`):**
```python
class IRepository(ABC):
    @abstractmethod
    def save_data(self, data):  # Определен метод save_data
        pass
```

**Реализация (`src/database/repositories/generic_repository.py:51`):**
```python
class GenericRepository(BaseRepository):
    def upsert_data(self, rows):  # Но используется upsert_data!
        ...
```

**Проблема:**
- Нарушение контракта интерфейса
- Невозможность подстановки реализаций
- Запутанность API

**Решение:**
- Привести интерфейс в соответствие с реализацией
- Либо переименовать методы в реализации
- Либо обновить интерфейс

---

### 3. Open/Closed Principle (OCP)

**Проблема:** Хардкод специфичных типов данных в generic-классе.

**Нарушение в `src/database/repositories/generic_repository.py:77-80`:**
```python
for row in valid_chunk:
    # Специфичная логика для asins и asin_upcs!
    if isinstance(row.get('asins', None), (list, dict)):
        row['asins'] = Json(row['asins'])
    if isinstance(row.get('asin_upcs', None), (list, dict)):
        row['asin_upcs'] = Json(row['asin_upcs'])
```

**Нарушение в `src/database/repositories/generic_repository.py:119-123`:**
```python
def is_empty_upc(self, record: dict) -> bool:
    """UPC-специфичная логика в generic-репозитории!"""
    return 'upc' in record and (record['upc'] or '').strip() == ""
```

**Решение:**
- Использовать паттерн Chain of Responsibility для преобразования полей
- Создать `FieldTransformerRegistry` с регистрацией типов
- Вынести UPC-валидацию в отдельный валидатор или бизнес-правило

---

### 4. SQL Injection риски

**Проблема:** `QueryBuilder` использует f-strings вместо параметризации.

**Нарушение в `src/database/query_builder.py:18-25`:**
```python
def generate_select_sql(self, limit: int) -> str:
    query = f"""
        SELECT {columns_list}
        FROM {table} {source_alias}
        {join_sql}
        WHERE {where_sql}
        ORDER BY {source_alias}.{order_column} NULLS FIRST
        LIMIT {limit};
    """
    return query.strip()
```

**Риски:**
- Потенциальная SQL-инъекция через `table`, `columns_list`, `order_column`
- Невозможность использования prepared statements
- Усложненная отладка и профилирование

**Частичная защита:**
- `select_raw()` проверяет, что запрос начинается с SELECT/WITH (строка 133)
- Но защиты недостаточно

**Решение:**
- Использовать параметризованные запросы
- Валидировать имена таблиц/колонок через whitelist
- Использовать query builder библиотеки (SQLAlchemy Core)

---

### 5. God Object антипаттерн

**Проблема:** `Config` имеет 50+ методов для всех типов настроек.

**Файл `src/utils/config.py:4-250`:**
- 13 broker-методов (строки 15-61)
- 6 database connection методов (строки 65-88)
- 14+ database source/target методов (строки 92-228)
- 4 дополнительных метода (строки 231-249)

**Проблемы:**
- Сложность понимания и поддержки
- Нарушение SRP
- Трудность тестирования

**Решение:**
- Разбить на композицию объектов:
  - `BrokerConfig`
  - `DatabaseConfig`
  - `SourceConfig`
  - `TargetConfig`

```python
class Config:
    def __init__(self, settings: dict):
        self.broker = BrokerConfig(settings.get('broker', {}))
        self.database = DatabaseConfig(settings.get('database', {}))
        self.dag_id = settings.get('dag_id', '')
```

---

### 6. Magic Numbers и Magic Strings

**Примеры в `src/database/query_builder.py`:**
```python
source_alias = 't'              # строка 11 - почему 't'?
join_alias = f'j{i + 1}'        # строка 67 - магическое 'j'
```

**Примеры в других файлах:**
```python
# Хардкод версий, таймаутов, лимитов без именованных констант
scale = 1000000                  # Что это значит?
default_port = 5432             # Захардкожен
```

**Решение:**
- Вынести в константы:
```python
DEFAULT_SOURCE_ALIAS = 't'
JOIN_ALIAS_PREFIX = 'j'
POSTGRES_DEFAULT_PORT = 5432
VERSION_SCALE_MICROSECONDS = 1_000_000
```

---

### 7. Tight Coupling (Жесткая связанность)

**Проблема 1:** DAG напрямую зависит от `ServiceFactory`.

**`dags/new_asin_keyword/new_asin_keyword_in.py:77-79`:**
```python
def ingest_data():
    service = ServiceFactory.create_ingestion_service(settings)
    service.ingest_data()
```

**Проблема 2:** Дублирование структуры `settings` в каждом DAG.

**Проблемы:**
- Каждый DAG знает о ServiceFactory
- Изменение структуры settings требует правок во всех DAG-ах
- Нет переиспользования логики создания DAG

**Решение:**
- Создать `DAGBuilder` или базовый класс
- Использовать декораторы для стандартизации
- Применить Template Method Pattern

```python
class BaseDagBuilder:
    def create_dag(self, settings):
        service = ServiceFactory.create_ingestion_service(settings)
        return self._build_operators(service)
```

---

### 8. Отсутствие Unit of Work Pattern

**Проблема:** Транзакции управляются в репозитории.

**`src/database/repositories/generic_repository.py:83`:**
```python
def upsert_data(self, rows):
    for i in range(0, len(rows), chunk_size):
        # ...
        try:
            execute_batch(cursor, query, valid_chunk)
            self.connection.commit()  # Коммит внутри репозитория!
        except DatabaseError as e:
            # ...
```

**Проблемы:**
- Невозможность откатить несколько операций атомарно
- Сервис не контролирует границы транзакций
- Нарушение принципа единственной ответственности

**Решение:**
- Реализовать Unit of Work паттерн
- Передавать управление транзакциями в сервисный слой

```python
class UnitOfWork:
    def __enter__(self):
        self.session = Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.session.rollback()
        else:
            self.session.commit()
        self.session.close()
```

---

### 9. Нарушение DRY (Don't Repeat Yourself)

**Проблема:** Повторяющаяся структура `settings` в каждом DAG.

**Примеры дублирования:**
- Структура `broker` секции
- Структура `database` секции
- Логика создания DAG объекта
- Обработчики ошибок

**Количество файлов с дублированием:**
- `dags/edi/edi_in_dag.py`
- `dags/edi/edi_out_dag.py`
- `dags/keepa/keepa_in_dag.py`
- `dags/local_data/local_data_in.py`
- И еще 10+ файлов

**Решение:**
- Создать `SettingsBuilder` с fluent API
- Базовый класс или функцию-шаблон для DAG
- Вынести общие части в конфигурационные файлы (YAML/JSON)

---

### 10. Слабая типизация

**Проблема:** Использование `dict` везде вместо типизированных объектов.

**Примеры:**
```python
def send_message(self, message: Dict[str, Any]) -> None:  # Что в message?
def consume_messages(self, max_records: int) -> List[Dict[str, Any]]:  # Что возвращается?
```

```python
def process_chunk(self, chunk: list[dict]) -> list[dict]:  # Структура dict неизвестна
```

**Проблемы:**
- Отсутствие автодополнения в IDE
- Ошибки обнаруживаются только в runtime
- Сложность рефакторинга

**Решение:**
- Использовать dataclasses:
```python
@dataclass
class Record:
    keyword: str
    brand: str
    version: int
```

- Или Pydantic для валидации:
```python
class Message(BaseModel):
    keyword: str
    brand: str
    version: int

    class Config:
        extra = 'forbid'
```

---

### 11. Отсутствие централизованной обработки ошибок

**Проблема:** Логика обработки ошибок разбросана по всему коду.

**Примеры:**
```python
# src/services/ingestion_service.py:65-67
except DatabaseError as de:
    self.logger.error(f"Database error: {de}")
    raise RuntimeError("Critical database error") from de
```

```python
# src/database/repositories/generic_repository.py:84-86
except DatabaseError as e:
    self.logger.error(f"{self.config.dag_id} - upsert_data - Ошибка: {e}")
    raise
```

**Проблемы:**
- Дублирование логики логирования
- Непоследовательная обработка исключений
- Смешивание технических и бизнес-исключений

**Решение:**
- Создать иерархию кастомных исключений
- Использовать декораторы для обработки ошибок
- Централизованный error handler

---

### 12. Недостаточная валидация

**Проблема:** Валидация выполняется в разных местах с разной степенью строгости.

**Примеры:**
- `BrokerValidator` (`src/validators/broker_validator.py`) — только для broker
- Валидация записей в `GenericRepository.is_valid_record()`
- Нет валидации структуры `settings` при создании DAG

**Решение:**
- Централизованная валидация через Pydantic или similar
- Fail-fast подход: валидация при старте DAG
- Четкие error messages

---

## Итоговая оценка

### Сильные стороны

1. **Архитектурная организация** — четкое разделение на слои
2. **Применение паттернов** — грамотное использование Factory, Repository, Strategy
3. **Dependency Injection** — слабая связанность компонентов
4. **Расширяемость** — легко добавить новый брокер или тип репозитория

### Слабые стороны

1. **GenericRepository** — превратился в God Class
2. **Несоответствие интерфейсов** — IRepository vs реализация
3. **SQL Injection риски** — использование f-strings в QueryBuilder
4. **Слабая типизация** — dict везде вместо typed objects
5. **Дублирование кода** — повторяющиеся settings в DAG-ах
6. **Бизнес-логика в инфраструктуре** — UPC-валидация в репозитории
7. **Отсутствие Unit of Work** — управление транзакциями в репозитории
8. **God Object Config** — 50+ методов в одном классе

---

## Рекомендации по улучшению

### Критичные (высокий приоритет)

1. **Исправить интерфейс IRepository**
   - Привести в соответствие с GenericRepository
   - Документировать контракт

2. **Усилить защиту от SQL-инъекций**
   - Использовать параметризованные запросы
   - Валидировать имена таблиц/колонок

3. **Разбить GenericRepository**
   - Вынести валидацию в отдельный класс
   - Создать StatisticsCollector
   - Убрать бизнес-логику (is_empty_upc)

4. **Убрать хардкод из generic-кода**
   - Удалить специфичную логику для asins/asin_upcs
   - Использовать регистрируемые трансформеры

### Средний приоритет

5. **Добавить типизацию**
   - Использовать dataclasses или Pydantic
   - Typed objects вместо Dict[str, Any]

6. **Разбить Config**
   - BrokerConfig, DatabaseConfig как композиция
   - Уменьшить количество методов

7. **Централизовать обработку ошибок**
   - Кастомные исключения
   - Декораторы для error handling

8. **Устранить дублирование в DAG-ах**
   - Базовый класс или template
   - SettingsBuilder с fluent API

### Низкий приоритет

9. **Вынести magic strings в константы**
10. **Реализовать Unit of Work паттерн**
11. **Улучшить валидацию конфигурации**
12. **Добавить типы возвращаемых значений везде**

---

## Заключение

Проект имеет хорошую архитектурную основу с правильным применением классических паттернов проектирования. Основные проблемы связаны с накоплением технического долга в процессе развития:

- **GenericRepository** разросся и потерял фокус
- Интерфейсы отстали от реализации
- Появились специфичные бизнес-правила в generic-коде
- Недостаточная типизация усложняет поддержку

При последовательном устранении выявленных проблем проект может стать образцом чистой архитектуры для ETL-оркестрации.

---

**Дата анализа:** 2025-11-18
**Проанализировано файлов:** 15+
**Выявлено критичных проблем:** 4
**Выявлено проблем средней важности:** 4
**Выявлено некритичных проблем:** 4
