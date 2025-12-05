FROM apache/airflow:2.10.2

# Já usa direto o usuário airflow
USER airflow

RUN pip install --no-cache-dir clickhouse-connect
# não precisa instalar pandas, a imagem do Airflow já traz

# se um dia precisar rodar algo como root, aí sim:
# USER root
# ... comandos root ...
# USER airflow
