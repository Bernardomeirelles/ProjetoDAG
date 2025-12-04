def extracao_dados():
    with open('src\customers-10000.csv', 'r', encoding='utf-8') as arquivo:
        dados = arquivo.readlines()
        print(dados)
    return dados

extracao_dados()

