# Desafio Técnico – Engenheiro de Dados

Este projeto implementa um pipeline de dados completo, simulando um ambiente corporativo com uma arquitetura em camadas de Data Lake: Bronze, Silver, e Gold, culminando na persistência em um banco de dados relacional. O pipeline é conteinerizado usando Docker para garantir portabilidade e reprodutibilidade.

## Objetivo do Projeto

Demonstrar a capacidade de projetar, implementar e gerenciar um pipeline de dados ETL (Extract, Transform, Load) com foco em:

Arquitetura em Camadas:

Bronze: Ingestão bruta.

Silver: Limpeza e transformação.

Gold: Enriquecimento.

Manipulação de Dados: uso de Pandas para transformação de dados.

Armazenamento Intermediário: uso do formato Parquet entre as camadas para performance e eficiência.

Persistência: gravação dos dados finais em um banco SQLite.

Conteinerização: uso de Docker para empacotar o pipeline.

### Arquitetura do Pipeline

#### Bronze (Ingestão)
Fonte: Arquivo movies.csv na pasta dados/.

Processamento: Cópia direta dos dados brutos.

Destino: datalake/bronze/ (formato CSV).

#### Silver (Limpeza e Transformação)
Processamento:

Extração de id, filme, ano com expressões regulares.

Criação de DataFrame com Pandas.

Destino: datalake/silver/ (formato Parquet).

#### Gold (Enriquecimento e Agregação)
Processamento:

Categorização dos filmes com base no título:

Documentário, Animação, Sequel/Série, Filme.

Destino: datalake/gold/movies_by_category/ (Parquet por categoria).


#### Persistência (Banco de Dados Relacional)
Banco: SQLite (movies.db)

Processamento: Cada categoria é armazenada em uma tabela separada.

### Tecnologias Utilizadas

Linguagem: Python 3.12

Processamento: Pandas

Armazenamento: Parquet

Banco de Dados: SQLite

Conteinerização: Docker

## Estrutura do Projeto

```
├── dados/                         # Dados brutos (entrada)
│   └── movies.csv
├── datalake/
│   ├── bronze/                    # Dados brutos copiados
│   ├── silver/                    # Dados limpos
│   └── gold/                      # Dados enriquecidos por categoria
├── database/
│   └── movies.db                  # Banco
├── src/
│   ├── analise_exploratoria.ipynb # Notebook apenas para análise dos dados
│   ├── bronze.py                  # Camada Bronze
│   ├── silver.py                  # Camada Silver
│   ├── gold.py                    # Camada Gold
│   └── persistence.py             # Persistência no banco
├── run_pipeline.py                # Orquestrador do pipeline
├── Dockerfile                     # Docker build
└── requirements.txt               # Dependências Python
```

## Como executar o pipeline


### Pré-requisitos

Docker instalado e em execução.

Python 3.12

### Construir a Imagem Docker

No terminal, execute na raiz do projeto:

```
docker build -t (aqui pode nomear da forma desejada) .
```

Executar o Pipeline

```
docker run --rm \
  --name my-data-pipeline \
  -v "$(pwd)/dados:/app/dados" \
  -v "$(pwd)/datalake:/app/datalake" \
  -v "$(pwd)/database:/app/database" \
  (nome que foi inserido na etapa anterior)
```
