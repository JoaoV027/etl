# Projeto ETL - AdventureWorks Data Warehouse

Pipeline de extração, transformação e carga de dados do banco AdventureWorks2022 para um Data Warehouse dimensional usando Apache Airflow.

## O que é

Sistema automatizado de ETL que processa dados de vendas do SQL Server AdventureWorks2022 e carrega em um Data Warehouse PostgreSQL com modelo dimensional (esquema estrela).

O pipeline extrai 12 tabelas de origem, aplica transformações de negócio e carrega em 1 tabela fato e 6 dimensões, possibilitando análise de 121.317 transações de vendas totalizando R$ 109,8 milhões.

## Arquitetura

- **Origem**: SQL Server 2022 (AdventureWorks2022)
- **Destino**: PostgreSQL 15 (Data Warehouse dimensional)
- **Orquestração**: Apache Airflow com executor Celery
- **Infraestrutura**: Docker Compose (8 containers)
- **Linguagem**: Python 3.12 com pandas e SQLAlchemy

### Containers

```
mssql              - SQL Server 2022 (porta 1433)
adventureworks_dw  - PostgreSQL 15 (porta 5433)
airflow-webserver  - Interface web Airflow (porta 8080)
airflow-scheduler  - Scheduler de DAGs
airflow-worker     - Worker para execução de tasks
airflow-flower     - Monitor de tasks (porta 5555)
redis              - Message broker
postgres           - Metabase do Airflow
```

## Modelo de Dados

### Tabela Fato
- `fact_sales` - 121.317 registros de vendas (2011-2014)

### Dimensões
- `dim_date` - 6.575 datas (2008-2025)
- `dim_customer` - 19.820 clientes
- `dim_product` - 504 produtos
- `dim_territory` - 10 territórios
- `dim_sales_person` - 17 vendedores
- `dim_promotion` - 16 promoções

## Performance

- Tempo total: 77 segundos
- Throughput: 1.575 registros/segundo
- Extração: 67s (87% do tempo)
- Transformação: 8,5s

## Como usar

### Pré-requisitos
- Docker Desktop
- 8 GB RAM disponível
- 10 GB espaço em disco

### Iniciar ambiente

```powershell
# Subir todos os containers
docker compose up -d

# Aguardar inicialização (~30 segundos)
Start-Sleep -Seconds 30
```

### Acessar Airflow

URL: http://localhost:8080  
Login: `admin`  
Senha: `admin`

### Executar pipeline

```powershell
# Via comando
docker exec etl-airflow-webserver-1 airflow dags trigger etl_adventureworks_dw

# Ou pela interface web:
# 1. Acessar http://localhost:8080
# 2. Localizar DAG "etl_adventureworks_dw"
# 3. Clicar no botão "play" para disparar
```

### Consultar dados

```powershell
# PostgreSQL Data Warehouse (porta 5433)
docker exec adventureworks_dw psql -U dw_user -d dw_adventureworks

# Exemplo de query
docker exec adventureworks_dw psql -U dw_user -d dw_adventureworks -c "SELECT COUNT(*) FROM dw.fact_sales"
```

## Estrutura do projeto

```
.
├── docker-compose.yml              # Orquestração dos containers
├── requirements.txt                # Dependências Python
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   └── etl_adventureworks_dw.py    # DAG principal
│   └── requirements/
│       └── requirements.txt
├── sql/
│   ├── create_dw_schema.sql        # Estrutura do DW
│   ├── kpi_queries.sql             # Consultas de análise
│   └── data_quality_checks.sql     # Testes de qualidade
└── mssql/
    ├── backup/
    │   └── AdventureWorks2022.bak  # Backup do banco origem
    └── scripts/
        └── restore_adventureworks.sql
```

## Principais descobertas

Análise dos dados revelou:

- Mountain-200 Black, 38 é o produto top de vendas (R$ 4,4M)
- 49,8% das vendas não têm vendedor associado (canal direto/online)
- 24% das transações apresentam margem negativa
- Tires and Tubes tem a maior margem (62,6%)
- Descontos acima de 10% destroem lucratividade

Scripts SQL com todas as análises estão em `sql/kpi_queries.sql`.

## Problemas resolvidos

Durante o desenvolvimento foram resolvidos:

1. **XCom size limit**: Combinação de extract+load numa única função escrevendo direto no PostgreSQL
2. **Case-sensitive columns**: Queries SQL atualizadas para usar nomes quoted (PascalCase)
3. **Type mismatch em COALESCE**: Cast explícito de TEXT para NUMERIC
4. **Dimensão data incompleta**: População manual de datas 2008-2022

## Tecnologias

- Python 3.12
- Apache Airflow 2.9.2
- PostgreSQL 15
- SQL Server 2022
- Docker Compose
- pandas, SQLAlchemy, psycopg2, pyodbc

## Licença

MIT
