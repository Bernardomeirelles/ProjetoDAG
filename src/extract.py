import pandas as pd
from pathlib import Path

def extrair_dados():
    """
    Lê o arquivo CSV local e retorna um DataFrame do pandas.
    """
    csv_path = Path("data") / "customers-10000.csv"  # funciona em Linux e Windows

    df = pd.read_csv(csv_path, encoding="utf-8")
    return df


if __name__ == "__main__":
    data = extrair_dados()
    print(data.head())
