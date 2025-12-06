# src/load.py
import clickhouse_connect
import pandas as pd


def get_clickhouse_client():
    """
    Cria o cliente de conexão com ClickHouse.
    Host: nome do serviço no docker-compose (clickhouse).
    """
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="",
        secure=False,
    )
    return client


def criar_tabela_se_nao_existir(client, table_name: str = "customers"):
    """
    Cria a tabela no ClickHouse se ela não existir.
    Ajuste os tipos conforme seu CSV real.
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        customer_id UInt32,
        first_name String,
        last_name String,
        full_name String,
        company String,
        city String,
        country String,
        phone_1 String,
        phone_2 String,
        email String,
        subscription_date DateTime,
        website String
    )
    ENGINE = MergeTree()
    ORDER BY customer_id
    """
    client.command(create_table_sql)


def load_to_clickhouse(df: pd.DataFrame, table_name: str = "customers"):
    """
    Carrega um DataFrame no ClickHouse.
    - Garante que a tabela existe.
    - Insere os dados.
    - Faz um teste simples (COUNT) para validar.
    """

    # ✅ Sad path explícito: DataFrame vazio
    if df is None or df.empty:
        raise ValueError(
            "DataFrame vazio, nada para carregar. Verifique sua etapa de transformação."
        )

    client = get_clickhouse_client()
    criar_tabela_se_nao_existir(client, table_name=table_name)

    # Garante que as colunas necessárias existem (ajuste conforme sua realidade)
    expected_cols = [
        "customer_id",
        "first_name",
        "last_name",
        "full_name",
        "company",
        "city",
        "country",
        "phone_1",
        "phone_2",
        "email",
        "subscription_date",
        "website",
    ]

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no DataFrame para carga: {missing}")

    # Converte subscription_date para datetime, se necessário
    if not pd.api.types.is_datetime64_any_dtype(df["subscription_date"]):
        df["subscription_date"] = pd.to_datetime(
            df["subscription_date"], errors="coerce"
        )

    rows = df[expected_cols].to_numpy().tolist()

    client.insert(
        table_name,
        rows,
        column_names=expected_cols,
    )

    # Teste integrado simples: contar registros
    result = client.query(f"SELECT count(*) AS cnt FROM {table_name}")
    count_rows = result.result_rows[0][0]
    print(f"[LOAD] Linhas totais na tabela '{table_name}': {count_rows}")

    return count_rows
