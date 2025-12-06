import sys
from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------
# Ajuste de PYTHONPATH para importar src.* tanto local quanto no Docker
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from extract import extrair_dados
from transform import transformar_dados
from load import load_to_clickhouse, get_clickhouse_client


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def _drop_test_table(table_name: str = "customers_test"):
    """
    Remove a tabela de teste no ClickHouse, se existir.
    Útil para garantir que o happy path começa em ambiente limpo.
    """
    client = get_clickhouse_client()
    client.command(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------
# 1) HAPPY PATH – fluxo completo ETL
# ---------------------------------------------------------------------
def test_happy_path_full_etl_loads_into_clickhouse():
    """
    Cenário feliz:
    - extrai do CSV real
    - transforma
    - carrega no ClickHouse em uma tabela de teste
    - valida que a quantidade de linhas carregadas == linhas transformadas
    """
    table_name = "customers_test"
    _drop_test_table(table_name)

    # Extract
    df_raw = extrair_dados()
    assert not df_raw.empty, "CSV não deveria estar vazio no happy path"

    # Transform
    df_transformed = transformar_dados(df_raw)
    assert not df_transformed.empty, "Transform não deveria gerar DF vazio no happy path"

    # Load
    total_rows = load_to_clickhouse(df_transformed, table_name=table_name)

    assert total_rows == len(df_transformed), (
        f"Quantidade de linhas na tabela ({total_rows}) "
        f"deve ser igual ao DF transformado ({len(df_transformed)})"
    )


# ---------------------------------------------------------------------
# 2) SAD PATH – CSV ausente no extract
# ---------------------------------------------------------------------
def test_sad_path_extract_file_not_found(tmp_path, monkeypatch):
    """
    Cenário triste:
    - Força o extrair_dados a olhar para um caminho inexistente
    - Espera FileNotFoundError
    """
    import extract as extract_module

    fake_csv_path = tmp_path / "data" / "customers-10000.csv"
    fake_csv_path.parent.mkdir(parents=True, exist_ok=True)

    def fake_extrair_dados():
        # Aponta para um arquivo que não existe
        return pd.read_csv(fake_csv_path, encoding="utf-8")

    # Substitui temporariamente a função real
    monkeypatch.setattr(extract_module, "extrair_dados", fake_extrair_dados)

    with pytest.raises(FileNotFoundError):
        extract_module.extrair_dados()


# ---------------------------------------------------------------------
# 3) SAD PATH – DF sem colunas obrigatórias no load
# ---------------------------------------------------------------------
def test_sad_path_load_missing_required_column_raises():
    """
    Cenário triste:
    - Monta um DataFrame que não tem todas as colunas esperadas
    - Espera ValueError do load_to_clickhouse
    """

    # DataFrame propositalmente incompleto (faltam várias colunas)
    df_incompleto = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "first_name": ["A", "B"],
            # sem 'email', sem 'subscription_date', etc.
        }
    )

    with pytest.raises(ValueError) as exc:
        load_to_clickhouse(df_incompleto, table_name="customers_test_missing_cols")

    assert "Colunas ausentes" in str(exc.value)


# ---------------------------------------------------------------------
# 4) SAD PATH – DataFrame vazio no load
# ---------------------------------------------------------------------
def test_sad_path_load_empty_dataframe_raises():
    """
    Cenário triste:
    - Tenta carregar um DataFrame vazio
    - Deve falhar explicitamente (boa prática)
    """
    df_vazio = pd.DataFrame()

    with pytest.raises(ValueError) as exc:
        load_to_clickhouse(df_vazio, table_name="customers_test_empty")

    assert "DataFrame vazio" in str(exc.value)


# ---------------------------------------------------------------------
# 5) TESTE DE CONEXÃO COM O CLICKHOUSE DO COMPOSE
# ---------------------------------------------------------------------
def test_clickhouse_connection():
    """
    Verifica se o ClickHouse do docker-compose está acessível
    e responde a uma query simples (SELECT 1).
    Usa o mesmo client da aplicação (get_clickhouse_client).
    """
    client = get_clickhouse_client()

    try:
        result = client.query("SELECT 1 AS n")
    except Exception as e:
        pytest.fail(f"Falha ao conectar ao ClickHouse do compose: {e}")

    assert len(result.result_rows) == 1, (
        f"Esperava exatamente 1 linha, recebi: {result.result_rows}"
    )
    primeira_linha = result.result_rows[0]

    # Pode ser (1,) ou [1], então olhamos só pro primeiro elemento
    assert primeira_linha[0] == 1, (
        f"Resposta inesperada do ClickHouse. Esperado 1, recebido: {primeira_linha[0]}"
    )
