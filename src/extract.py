import pandas as pd

def extrair_dados():
    """
    Lê o arquivo CSV local e retorna um DataFrame do pandas.
    """
    df = pd.read_csv("src/customers-10000.csv", encoding="utf-8")
    return df

df = extrair_dados()
print(df.head())
