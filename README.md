# ETL Pipeline com Airflow, ClickHouse, Docker e Testes Automatizados

Este projeto implementa um pipeline de dados completo no modelo **Extract → Transform → Load (ETL)**, utilizando Apache Airflow como orquestrador, ClickHouse como banco analítico columnar, Pandas para manipulação de dados e TestContainers para testes funcionais com containers efêmeros.

Trata-se de uma arquitetura profissional, totalmente conteinerizada e facilmente reproduzível em qualquer máquina com Docker.

---

# Sumário
1. Arquitetura Geral  
2. Tecnologias Utilizadas  
3. Descrição do ETL  
4. Como Executar o Projeto  
5. Execução dos Testes Automatizados  
6. Estrutura de Pastas  
7. Funcionamento da DAG  
8. Detalhamento dos Módulos  
9. Melhorias Futuras  
10. Conclusão

---

# 1. Arquitetura Geral

A organização principal do projeto é a seguinte:
```


ProjetoDAG/
│
├── dags/
│ └── etl_clickhouse_dag.py
│
├── src/
│ ├── extract.py
│ ├── transform.py
│ └── load.py
│
├── tests/
│ └── test_integration_etl.py
│
├── data/
│ └── customers-10000.csv
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```


O ecossistema fornecido pelo Docker Compose inicia:

- Airflow Webserver  
- Airflow Scheduler  
- Banco ClickHouse  
- Banco Postgres (metadados do Airflow)  
- Estrutura de pastas montadas como volumes persistentes  

---

# 2. Tecnologias Utilizadas

| Tecnologia          | Finalidade |
|---------------------|------------|
| Apache Airflow      | Orquestração do pipeline ETL |
| Python + Pandas     | Manipulação e limpeza dos dados |
| ClickHouse          | Armazenamento analítico columnar |
| Postgres            | Backend operacional do Airflow |
| Docker + Docker Compose | Infraestrutura reprodutível |
| Pytest              | Testes automatizados |
| TestContainers      | Testes com bancos reais instanciados em containers efêmeros |

---

# 3. Descrição do ETL

O pipeline executa três etapas principais:

## Extract
Localizado em `src/extract.py`.  
Responsável por:

- Ler o arquivo `customers-10000.csv`
- Validar existência e integridade
- Carregar o conteúdo em um DataFrame Pandas

## Transform
Localizado em `src/transform.py`.  
Etapas aplicadas:

- Padronização do nome das colunas em `snake_case`
- Remoção de colunas desnecessárias
- Conversão de datas para o tipo correto
- Enriquecimento dos dados com o campo `full_name`
- Normalização de tipos e limpeza de inconsistências
- Remoção de duplicatas

Todas as transformações seguem boas práticas: imutabilidade (trabalha-se com uma cópia) e validação defensiva.

## Load
Localizado em `src/load.py`.  
Funções executadas:

- Conexão com ClickHouse via `clickhouse_connect`
- Criação automática da tabela caso não exista (engine: MergeTree)
- Validação de colunas obrigatórias antes da inserção
- Inserção em lote
- Execução de `SELECT COUNT(*)` para confirmar a carga

A função retorna o número total de linhas inseridas.

---

# 4. Como Executar o Projeto

## Pré-requisitos
- Docker 


Clone o repositório:

```bash
git clone <URL>
cd ProjetoDAG

```
## 4.1 Subir a infraestrutura completa
```

docker compose up --build -d
```

Serviços principais:

Airflow Webserver: http://localhost:8080

ClickHouse: porta 8123 (HTTP)

Airflow Scheduler

Postgres interno

## 4.2 Criar o usuário do Airflow
Caso necessário:

```

docker compose run airflow-webserver airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com

```
Acessar:

http://localhost:8080
Login:

username: admin
password: admin

## 4.3 Executar a DAG

No Airflow:

Ativar a DAG etl_clickhouse_dag

Executar manualmente via "Trigger DAG"

Ao final, a tabela customers estará criada e carregada no ClickHouse.

5. Execução dos Testes Automatizados
Acesse o container do Airflow Webserver:

```

docker exec -it projetodag-airflow-webserver-1
```
Execute:


```
pytest -vv
```



## 6. Estrutura de Pastas Detalhada

```
src/
│
├── extract.py
│   Contém a função extrair_dados() responsável pela leitura segura do CSV.
│
├── transform.py
│   Implementa toda a lógica de padronização, limpeza e enriquecimento dos dados.
│
└── load.py
    Faz a carga no ClickHouse, valida schemas e confirma o resultado da inserção.
markdown

dags/
└── etl_clickhouse_dag.py
    Define o pipeline, dependências e operadores.
lua

tests/
└── test_integration_etl.py
    Contém cenários happy path, sad path e testes de conectividade.
```
## 7. Funcionamento da DAG

A DAG segue a sequência:

```
extract_task → transform_task → load_task

```
Comunicação entre etapas ocorre por meio de arquivos intermediários armazenados em:
```
/opt/airflow/data/extracted.csv
/opt/airflow/data/transformed.csv

```
Cada tarefa é isolada, idempotente e validada individualmente.
A DAG está configurada para rodar uma vez ao dia (schedule diário), mas pode ser ajustada conforme necessidade.


## 8. Detalhamento dos Módulos

**extract.py**
Utiliza Pandas para leitura de arquivo

Implementa validações defensivas

É usado tanto pela DAG quanto pelos testes

**transform.py**
Aplica boas práticas de limpeza

Padroniza nomes

Converte datas corretamente

Cria novas features

Remove inconsistências de tipo e duplicatas

**load.py**
Garante que a tabela exista antes de inserir

Verifica se todas as colunas obrigatórias estão presentes

Insere os dados em formato eficiente

Consulta o ClickHouse após a inserção para confirmar o número de linhas carregadas

## 9. Melhorias Futuras
Separação dos dados em camadas (Bronze, Silver, Gold)

Validação de schema utilizando Pydantic ou Great Expectations

Adição de logs estruturados e monitoramento via Grafana

Conexões com múltiplas fontes externas (APIs, bancos transacionais)

Implementação de CI/CD com execução automática de testes

Incremento da DAG com mecanismo de retry e alertas automáticos

## 10. Conclusão
Este projeto demonstra um pipeline completo e robusto, que inclui:

Orquestração com Airflow

Processamento com Python e Pandas

Armazenamento analítico em ClickHouse

Testes automatizados com TestContainers

Execução totalmente reprodutível utilizando Docker