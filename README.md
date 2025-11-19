# Оркестратор ETL‑процессов на базе Apache Airflow

> Дисклеймер. Этот репозиторий описывает внутренний оркестратор ETL‑процессов на базе Apache Airflow. Примеры конфигураций и DAG‑ов не являются универсальным рецептом и требуют адаптации под конкретную инфраструктуру. Все секреты и данные доступа должны задаваться через переменные окружения или секрет‑хранилища, а не в коде и не в README.

## Start production
 
Создайте `.env.prod` на корне проекта на основе шаблона `.env` и запустите стек:

```powershell
docker compose build

docker compose up -d
```

## Start test/dev

Создайте `.env.test` или `.env.dev` на корне проекта на базе `.env` и используйте test‑override:

```powershell
docker compose --env-file .env.test -f docker-compose.yml -f "docker/config-envs/test/docker-compose.override.yml" build

docker compose --env-file .env.test -f docker-compose.yml -f "docker/config-envs/test/docker-compose.override.yml" up -d
```

## Переменные окружения

Фактический набор переменных окружения зависит от вашего окружения Airflow и инфраструктуры данных, но логика проекта
опирается на несколько стабильных групп. Базовый шаблон и актуальный список смотрите в `.env` на корне репозитория,
а ниже — сводка по ключевым группам.

### PostgreSQL (источники/витрины данных)

Для каждого логического источника (EDI, KEEPA, LOCAL_DATA, NEW_ASIN и т.п.) используются однотипные наборы переменных:

- `DB_*_HOST` — hostname PostgreSQL;
- `DB_*_PORT` — порт PostgreSQL;
- `DB_*_USER` — пользователь БД;
- `DB_*_PASSWORD` — пароль пользователя БД;
- `DB_*_NAME` — имя базы данных;
- `DB_*_SCHEMA_NAME` — схема по умолчанию для данного источника.

Эти значения читаются в DAG‑ах (через `src/utils/env.py`) и используются `Config` для построения connection‑строк и
полных имён таблиц (schema.table) в блоке `database`.

### Брокеры сообщений (RabbitMQ/Kafka/Redis)

В зависимости от выбранного брокера в `settings['broker']['type']` используются разные env‑переменные:

- RabbitMQ‑/AMQP‑совместимые брокеры:
  - `RABBITMQ_*_HOST` — hostname брокера;
  - `RABBITMQ_*_PORT` — порт;
  - `RABBITMQ_*_USER` — логин;
  - `RABBITMQ_*_PASS` — пароль.
- Kafka:
  - `KAFKA_*_HOST` / `KAFKA_*_PORT` или агрегированный `KAFKA_BOOTSTRAP_SERVERS` — адрес(а) брокера;
  - при необходимости — дополнительные переменные для security/protocol (SASL/SSL и т.п.), если это предусмотрено
    вашим окружением.
- Redis‑брокеры:
  - `REDIS_*_URL` — полная connection‑строка (`redis://user:pass@host:port/db`).

Конкретные суффиксы (`*_EDI`, `*_LOCAL_DATA` и т.д.) зависят от окружения и должны быть согласованы с вашими DAG‑ами.

### Прочие переменные

- Airflow‑специфичные параметры (`AIRFLOW__*`) — версию Airflow, executor, строки подключения и прочее лучше
  настраивать в `.env`/`airflow.cfg` и не трогать без необходимости.
- `APP_ENV`, `ENVIRONMENT` — логические флаги окружения (prod/test/dev), могут использоваться в DAG‑ах и сервисах
  для условной логики.
- Переменные для логирования и интеграций (например, `SLACK_*`) — используются утилитами из `src/utils/` и DAG‑ами
  для уведомлений.

При добавлении новых DAG‑ов или сервисов старайтесь придерживаться существующей схемы именования env‑переменных и
использовать функции `get_str`/`get_int` из `src/utils/env.py` вместо прямого `os.getenv`.

## Конфигурация DAG и settings

Каждый DAG описывает словарь `settings`, который дальше оборачивается в `Config` (`src/utils/config.py`) и используется сервисами/репозиториями. Ниже — сводка всех ключевых параметров.

### Общие поля settings

```python
settings = {
    'project_name': 'Local Data',          # Человеческое имя проекта
    'dag_id': 'zero_local_data',           # Используется как dag_id в Airflow
    'dag_description': '...',              # Описание DAG
    'schedule': '*/15 * * * *',            # CRON‑расписание
    'source': 'edi',                       # Произвольный тег источника (используется в мониторинге/логах)

    'new_version': True,                   # Управление версионностью в Serializer
    'use_offset_tracking': False,          # Зарезервировано под трекинг смещений (offsets)

    'broker': { ... },                     # Настройки брокера сообщений
    'database': { ... },                   # Настройки подключения к БД и query builder

    'message_filter': [ ... ],             # Фильтры на уровне сообщений (см. ниже)
    'excluded_columns': [...],             # Список колонок, которые нельзя обновлять в ON CONFLICT DO UPDATE
    'map_before_kafka_produce': func,      # Опциональная функция агрегации/маппинга перед отправкой пачки
    'modify_data_function': func,          # Опциональная функция модификации/сплита сообщений
}
```

### Broker configs examples

Всегда используйте `get_str`/`get_int` из `src/utils/env.py` для чтения env‑переменных.

```python
from src.utils.env import get_int, get_str

settings = {
    "broker": {
        'type': 'rabbit',  # или rabbitmq‑совместимый брокер
        "host": get_str("RABBITMQ_EDI_HOST"),
        "port": get_int("RABBITMQ_EDI_PORT"),
        "username": get_str("RABBITMQ_EDI_USER"),
        "password": get_str("RABBITMQ_EDI_PASS"),
        "topic": "zero_local_data",
    }
}

settings = {
    "broker": {
        'type': 'kafka',
        # Либо kafka_bootstrap_servers: "host:port" или ["host1:port1", "host2:port2"]
        'kafka_bootstrap_servers': get_str("KAFKA_BOOTSTRAP_SERVERS"),
        'consumer_group': 'my-consumer-group',
        'topic': 'zero_local_data',
    }
}

settings = {
    "broker": {
        'type': 'redis',
        "redis_url": get_str("REDIS_DOB_URL"),
        "topic": "zero_local_data",
    }
}
```

### Блок database: все параметры

Секция `database` описывает, откуда читаем данные (`source`), куда пишем (`target`), схему версионирования и поведение query builder'а.

```python
"database": {
    # Значения по умолчанию при upsert (используются в ON CONFLICT DO UPDATE)
    'default_values': {
        # 'is_deleted': 'NULL',
        # 'updated_at': 'CURRENT_TIMESTAMP',
    },

    # Размер батча при чтении из source и записи в target
    'batch_size': 1000,

    # Лимит размера очереди брокера. IngestionService учитывает lag и не будет переполнять очередь.
    'queue_size_limit': 10000,

    # Параметры сортировки/версии для инкрементальной выборки
    'order': {
        'column': 'version',      # колонка сортировки в source, по умолчанию 'version'
    },
    'version': {
        'column': 'version',      # колонка, где хранится индекс‑версия
        'scale': 1000000,         # масштаб для преобразования timestamp -> BIGINT
    },

    # SOURCE: откуда читаем
    'source': {
        'table': 'schema.source_table',

        # conflict_key можно задавать как список или строку через запятую.
        'conflict_key': ['id', 'region_id'],

        # Маппинг колонок source -> aliases, которые попадут в сообщение/target
        'columns': {
            'asin': 'external_id',
            'id_region': 'id_region',
            'marketplaceid': 'marketplaceid',
            'version': 'version',
        },

        # Условия выборки (WHERE). Обрабатываются QueryBuilder.generate_condition_sql
        'conditions': [
            {
                'column': 'version',   # либо "t.version" или колонка из join'а: "my_join.col"
                'operation': '<',      # любой SQL‑оператор: =, <>, <, >, <=, >=, IN, NOT IN, IS, IS NOT и т.п.
                'value': 24,           # либо значение, либо dict с {'raw': 'SQL_FRAGMENT'}
                # опционально: 'op': 'AND' / 'OR' — логический оператор для связывания с предыдущим условием
            },
            {
                'column': 'status',
                'operation': 'in',
                'values': ['NEW', 'UPDATED'],  # для IN / NOT IN используйте поле `values`
                'op': 'AND',
            },
            {
                'column': 't.version',
                'operation': '>=',
                'value': {
                    'raw': "EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '24 hour')) * 1000000",
                },
                'op': 'AND',
            },
        ],

        # JOIN'ы, которые query builder добавит к SELECT
        'joins': [
            {
                'type': 'LEFT',
                'alias': 'my_shop',
                'table': 'schema.shop',

                # Колонки join'а, которые попадут в SELECT (alias -> column)
                'columns': {
                    'catalog_id': 'catalog_id',
                },

                # ON‑условия для связи с source (alias "t")
                'on': [
                    {'left': 'id_region', 'right': 'id'},  # t.id_region = my_shop.id
                ],
            },
        ],
    },

    # TARGET: куда пишем upsert'ом
    'target': {
        'table': 'schema.target_table',

        # conflict_key для ON CONFLICT. Если не задано в target,
        # будет использовано значение из source.conflict_key.
        'conflict_key': ['catalog_id', 'external_id'],

        # Колонки target. Используется в loading DAG'ах как источник column->alias,
        # если source.columns отсутствует.
        'columns': {
            'external_id': 'external_id',
            'catalog_id': 'catalog_id',
            'marketplaceid': 'marketplaceid',
            'version': 'version',
        },
    },

    # Настройки подключения к PostgreSQL
    'connection': {
        'host': get_str('DB_EDI_HOST'),
        'port': get_int('DB_EDI_PORT'),
        'user': get_str('DB_EDI_USER'),
        'password': get_str('DB_EDI_PASSWORD'),
        'dbname': get_str('DB_EDI_NAME'),
    },
}
```

#### Column mappings и aliases

Config агрегирует маппинги колонок из:

- `database.source.columns` — основная таблица;
- `database.source.joins[*].columns` — дополнительные поля из join'ов;
- если `source.columns` пустой, используются `database.target.columns` (список или dict).

`Serializer` использует:

- `Config.all_column_aliases` — список alias'ов, которые попадут в сообщения брокеру;
- `Config.all_column_keys` / `get_column_alias_by_key` — для обратной десериализации из брокера в формат БД.

#### Conditions и поведение query builder

Query builder (`src/database/query_builder.py`) строит SELECT: 

```sql
SELECT <columns>
FROM <source_table> t
<JOIN CLAUSES>
WHERE <conditions>
ORDER BY t.<order.column> NULLS FIRST
LIMIT <limit>;
```

Поддерживаются:

- обычные условия: `column`, `operation`, `value`;
- "сырые" значения: `{'value': {'raw': 'SOME_SQL'}}` — подставляются в SQL как есть;
- IN/NOT IN: `operation` in ("in", "not in") + список `values`;
- специальные условия по `version.column` с `version.scale`: если в condition указан version‑столбец,
  builder строит выражение вида
  `TO_TIMESTAMP(t.version / scale) < CURRENT_TIMESTAMP - INTERVAL 'N hour' OR t.version IS NULL`.

Поле `op` в каждом condition (кроме первого) задаёт, чем соединять условие с предыдущими: `AND` / `OR`.

#### JOIN‑ы и дополнительные колонки

Каждый элемент в `database.source.joins` имеет структуру:

```python
{
    'type': 'LEFT',                         # тип JOIN: INNER/LEFT/RIGHT/FULL и т.п.
    'table': 'schema.table_name',
    'alias': 'j1',                          # alias join'а (по умолчанию j1, j2, ...)
    'columns': {                            # alias -> column в join'е
        'catalog_id': 'catalog_id',
        # ...
    },
    'on': [                                 # список ON‑условий
        {'left': 'id_region', 'right': 'id'},   # t.id_region = j1.id
        # ...
    ],
}
```

Query builder:

- добавляет JOIN‑ы в секцию FROM;
- включает `columns` join'ов в SELECT как `<alias>.<column> AS <alias_name>`;
- позволяет ссылаться на поля join'ов в `conditions` через `alias.column`.

### Message filter и post‑processing

Опциональная секция `message_filter` внутри `settings` позволяет на уровне сервиса/serializer
отфильтровывать нежелательные сообщения после десериализации.

Формат элемента `message_filter` (используется в `Serializer.message_filter`):

```python
'message_filter': [
    {
        'column': 'upc',
        'operation': 'not in',
        'values': ['0000000000000', ''],
    },
]
```

Если хотя бы один фильтр "срабатывает" для записи, эта запись будет исключена из дальнейшей обработки.

### map_before_kafka_produce и modify_data_function

Дополнительное поведение в `IngestionService` управляется функциями из root‑уровня `settings`:

- `map_before_kafka_produce(rows, version)` — если задана, вызывается один раз на пачку `rows` перед отправкой
  сообщений. Можно агрегировать несколько строк в одно сообщение.
- `modify_data_function(serialized_row)` — если задана, вызывается для каждой строки после сериализации и должна
  вернуть кортеж `(record, task)`, где `task` будет отправлен в брокер, а `record` — пойдёт в upsert в БД.


## File structure
```
├── .dockerignore
├── .env
├── .env.prod
├── .env.test
├── .gitignore
├── Dockerfile
├── README.md
├── dags
│   ├── edi
│   ├── local_data
│   │   ├── local_data_in.py
│   │   ├── local_data_out.py
│   │   └── local_data_zero.py
│   ├── multi_asins
│   ├── new_asin_keyword
│   │   ├── new_asin_keyword_in.py
│   │   └── new_asin_keyword_out.py
│   ├── stat
│   └── upc
├── docker-compose.yml
├── requirements.txt
└── src
    ├── database
    │   ├── connection_factory.py
    │   ├── connections
    │   │   ├── base_connection.py
    │   │   └── interfaces
    │   │       └── i_connection.py
    │   └── repositories
    │       ├── base_repository.py
    │       ├── generic_repository.py
    │       ├── interfaces
    │       │   └── i_repository.py
    │       ├── repository_factory.py
    │       ├── statistics_repository.py
    │       └── trigger_repository.py
    ├── factories
    │   └── service_factory.py
    ├── kafka_client
    │   └── interfaces
    ├── message_broker
    │   ├── factory.py
    │   ├── i_message_client.py
    │   ├── kafka
    │   │   └── kafka_client.py
    │   └── redis
    │       └── redis_client.py
    ├── services
    │   ├── ingestion_service.py
    │   ├── loading_service.py
    │   ├── statistics_service.py
    │   └── trigger_service.py
    ├── tools
    │   └── validate.py
    ├── utils
    │   ├── config.py
    │   ├── env.py
    │   ├── logger.py
    │   ├── serializer.py
    │   └── slack_webhook_handler.py
    └── validators
        ├── base_validator.py
        ├── broker_validator.py
        ├── config_validator.py
        └── database_validator.py

```