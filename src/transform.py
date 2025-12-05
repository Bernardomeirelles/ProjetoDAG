import pandas as pd
import numpy as np


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa, padroniza e estrutura o DataFrame para garantir:
    - Compatibilidade total com ClickHouse
    - Boas práticas de modelagem
    - Eliminação de colunas ruins
    - Tipos consistentes
    - Geração de surrogate key numérica segura (customer_id)
    """

    # -------------------------------------------------------------------------
    # 1. Trabalhar em uma cópia (boa prática)
    # -------------------------------------------------------------------------
    df = df.copy()

    # -------------------------------------------------------------------------
    # 2. Remover colunas inúteis / não desejadas
    # -------------------------------------------------------------------------
    colunas_para_remover = ["Index", "Unnamed: 0", "id", "uuid", "zip", "address"]
    df = df.drop(columns=[c for c in colunas_para_remover if c in df.columns], errors="ignore")

    # -------------------------------------------------------------------------
    # 3. Padronizar nomes das colunas para snake_case
    # -------------------------------------------------------------------------
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # -------------------------------------------------------------------------
    # 4. Ajustar tipos e tratar inconsistências
    # -------------------------------------------------------------------------

    # 4.1 subscription_date → datetime
    if "subscription_date" in df.columns:
        df["subscription_date"] = pd.to_datetime(df["subscription_date"], errors="coerce")

    # Sanity check: remove linhas SEM data
    if "subscription_date" in df.columns:
        df = df[df["subscription_date"].notna()]

    # 4.2 strings → strip e lower onde faz sentido
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # -------------------------------------------------------------------------
    # 5. Criar FULL NAME (boa prática de enriquecimento)
    # -------------------------------------------------------------------------
    if "first_name" in df.columns and "last_name" in df.columns:
        df["full_name"] = df["first_name"].astype(str) + " " + df["last_name"].astype(str)
    else:
        df["full_name"] = np.nan

    # -------------------------------------------------------------------------
    # 6. PRESERVAR o identificador original
    # -------------------------------------------------------------------------
    if "customer_id" in df.columns:
        df["customer_id_original"] = df["customer_id"].astype(str)
    else:
        df["customer_id_original"] = np.nan

    # -------------------------------------------------------------------------
    # 7. Criar surrogate key *numérica estável* para uso no ClickHouse
    # -------------------------------------------------------------------------
    # Isso resolve DEFINITIVAMENTE o problema do UInt32
    codes, uniques = pd.factorize(df["customer_id_original"])
    df["customer_id"] = codes.astype("int64")  # seguro e sequencial

    # -------------------------------------------------------------------------
    # 8. Remover duplicatas (boa prática)
    # -------------------------------------------------------------------------
    df = df.drop_duplicates()

    # -------------------------------------------------------------------------
    # 9. Ordenar colunas para um layout limpo
    # -------------------------------------------------------------------------
    colunas_ordenadas = [
        "customer_id",
        "customer_id_original",
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

    # Garante que só colunas existentes entrem
    colunas_finais = [c for c in colunas_ordenadas if c in df.columns]

    df = df[colunas_finais]

    return df


def main():
    df_raw = pd.read_csv("data/customers-10000.csv", encoding="utf-8")
    df_t = transformar_dados(df_raw)
    print(df_t.head())
    print(df_t.dtypes)


if __name__ == "__main__":
    main()
