ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}-python3.10

ARG APP_ENV
ARG PYTHONPATH
RUN echo "BUILDING FOR APP_ENV = ${APP_ENV}"

ARG PYTHONPATH
ENV PYTHONPATH="/opt/airflow:/opt/airflow/src:${PYTHONPATH}"

RUN mkdir -p /opt/airflow/logs /opt/airflow/src && \
    chmod -R 777 /opt/airflow/logs && \
    chmod -R 777 /opt/airflow/src

# Копируем requirements.txt в контейнер
COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /opt/airflow/requirements.txt

USER airflow
