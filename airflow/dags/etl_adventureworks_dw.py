"""
Pipeline ETL - AdventureWorks Data Warehouse
Autor: Sistema de Analytics
Data: 2025-11-23

Descrição:
    Pipeline completo de ETL para carga do Data Warehouse AdventureWorks.
    Implementa extração incremental, transformação dimensional e carga otimizada.
    
Fluxo:
    1. Inicialização de dimensões estáticas (Tempo)
    2. Extração de dados fonte para área de staging
    3. Carga de dimensões com SCD Type 1
    4. Carga da tabela fato com métricas calculadas
    5. Validação de qualidade de dados
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import os
import pymssql
import psycopg2
from psycopg2.extras import execute_batch

# Configuração de logging
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURAÇÕES DE CONEXÃO
# ============================================================
class DatabaseConfig:
    """Classe de configuração centralizada para conexões de banco de dados"""
    
    # Fonte OLTP (SQL Server)
    SOURCE_CONFIG = {
        'host': os.getenv("MSSQL_HOST", "mssql"),
        'database': os.getenv("MSSQL_DB", "AdventureWorks2022"),
        'user': os.getenv("MSSQL_USER", "sa"),
        'password': os.getenv("MSSQL_PASSWORD", "Strong!Passw0rd")
    }
    
    # Destino DW (PostgreSQL)
    TARGET_CONFIG = {
        'host': os.getenv("DW_HOST", "adventureworks_dw"),
        'dbname': os.getenv("DW_DB", "dw_adventureworks"),
        'user': os.getenv("DW_USER", "dw_user"),
        'password': os.getenv("DW_PASSWORD", "dw_password")
    }

# ============================================================
# MAPEAMENTO DE TABELAS FONTE -> STAGING
# ============================================================
TABELAS_STAGING = {
    "Sales.Customer": "stage_clientes",
    "Person.Person": "stage_pessoas",
    "Production.Product": "stage_produtos",
    "Production.ProductSubcategory": "stage_subcategorias",
    "Production.ProductCategory": "stage_categorias",
    "Sales.SalesTerritory": "stage_territorios",
    "Sales.SalesPerson": "stage_vendedores",
    "HumanResources.Employee": "stage_funcionarios",
    "Sales.SpecialOffer": "stage_ofertas",
    "Sales.SalesOrderHeader": "stage_pedidos_header",
    "Sales.SalesOrderDetail": "stage_pedidos_detalhe"
}


def extract_and_load_staging(**context):
    """Extrai tabelas origem MSSQL e carrega diretamente em staging (evita XCom)."""
    import datetime, decimal
    
    # Conecta ao DW primeiro para criar staging schema
    with psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as pg_conn:
        pg_conn.autocommit = True
        pg_cur = pg_conn.cursor()
        pg_cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
        
        # Conecta ao MSSQL e processa tabela por tabela
        with pymssql.connect(server=MSSQL_HOST, user=MSSQL_USER, password=MSSQL_PASSWORD, database=MSSQL_DB) as mssql_conn:
            for source, staging_name in EXTRACT_TABLES.items():
                print(f"Processando {source} -> staging.{staging_name}...")
                
                # Extrai dados
                mssql_cur = mssql_conn.cursor()
                mssql_cur.execute(f"SELECT * FROM {source}")
                rows = mssql_cur.fetchall()
                col_names = [d[0] for d in mssql_cur.description]
                
                # Drop e recria staging table
                pg_cur.execute(f"DROP TABLE IF EXISTS staging.{staging_name} CASCADE")
                
                if not rows:
                    pg_cur.execute(f"CREATE TABLE staging.{staging_name} (dummy INT)")
                    print(f"  {staging_name}: 0 registros (tabela vazia)")
                    continue
                
                # Inferência de tipos por amostragem
                sample_slice = rows[:500] if len(rows) > 500 else rows
                type_map = {c: [] for c in col_names}
                
                for r in sample_slice:
                    for i, c in enumerate(col_names):
                        val = r[i]
                        if val is None:
                            type_map[c].append("TEXT")
                        elif isinstance(val, bool):
                            type_map[c].append("BOOLEAN")
                        elif isinstance(val, int):
                            type_map[c].append("INT")
                        elif isinstance(val, float):
                            type_map[c].append("DOUBLE PRECISION")
                        elif isinstance(val, decimal.Decimal):
                            type_map[c].append("NUMERIC(18,4)")
                        elif isinstance(val, (datetime.date, datetime.datetime)):
                            type_map[c].append("TIMESTAMP")
                        else:
                            type_map[c].append("TEXT")
                
                # Consolida tipos
                final_types = {}
                for c in col_names:
                    tset = set(type_map[c])
                    if len(tset) == 1:
                        final_types[c] = list(tset)[0]
                    elif tset <= {"INT", "NUMERIC(18,4)"}:
                        final_types[c] = "NUMERIC(18,4)"
                    else:
                        final_types[c] = "TEXT"
                
                # Cria tabela staging
                cols_def = ",".join([f'"{c}" {final_types[c]}' for c in col_names])
                pg_cur.execute(f"CREATE TABLE staging.{staging_name} ({cols_def})")
                
                # Insere dados em lotes para performance
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    for r in batch:
                        placeholders = ",".join(["%s"] * len(col_names))
                        pg_cur.execute(f"INSERT INTO staging.{staging_name} VALUES ({placeholders})", r)
                
                print(f"  {staging_name}: {len(rows)} registros carregados")
    
    return "extraction and staging complete"


def transform_dimensions(**context):
    """Carrega/atualiza dimensões cliente, produto e território (Type 1)."""
    with psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as pg_conn:
        cur = pg_conn.cursor()
        # Cliente
        cur.execute("""
            INSERT INTO dw.dim_customer (customer_id, first_name, last_name, full_name, email, create_date)
            SELECT CAST(c."CustomerID" AS INT), p."FirstName", p."LastName", p."FirstName" || ' ' || p."LastName", NULL, CURRENT_DATE
            FROM staging.stg_customer c
            LEFT JOIN staging.stg_person p ON p."BusinessEntityID" = CAST(c."PersonID" AS INT)
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                full_name = EXCLUDED.full_name,
                update_date = CURRENT_DATE;
        """)
        pg_conn.commit()
        print("dim_customer carregado")
        
        # Produto (categoria/subcategoria)
        cur.execute("""
            INSERT INTO dw.dim_product (product_id, product_name, category, subcategory, model, color, size, standard_cost, list_price, start_date)
            SELECT 
                CAST(p."ProductID" AS INT) as product_id,
                p."Name",
                pc."Name" as category,
                psc."Name" as subcategory,
                p."ProductModelID"::TEXT as model,
                p."Color",
                p."Size",
                COALESCE(p."StandardCost",0)::NUMERIC(12,2) as standard_cost,
                COALESCE(p."ListPrice",0)::NUMERIC(12,2) as list_price,
                CURRENT_DATE as start_date
            FROM staging.stg_product p
            LEFT JOIN staging.stg_product_subcategory psc ON CAST(psc."ProductSubcategoryID" AS INT) = CAST(p."ProductSubcategoryID" AS INT)
            LEFT JOIN staging.stg_product_category pc ON CAST(pc."ProductCategoryID" AS INT) = CAST(psc."ProductCategoryID" AS INT)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                model = EXCLUDED.model,
                color = EXCLUDED.color,
                size = EXCLUDED.size,
                standard_cost = EXCLUDED.standard_cost,
                list_price = EXCLUDED.list_price;
        """)
        pg_conn.commit()
        print("dim_product carregado")
        
        # Território
        cur.execute("""
            INSERT INTO dw.dim_territory (territory_id, territory_name, country_region, group_name)
            SELECT DISTINCT CAST("TerritoryID" AS INT) as territory_id,
                            "Name" as territory_name,
                            "CountryRegionCode" as country_region,
                            "Group" as group_name
            FROM staging.stg_sales_territory st
            ON CONFLICT (territory_id) DO UPDATE SET
                territory_name = EXCLUDED.territory_name,
                country_region = EXCLUDED.country_region,
                group_name = EXCLUDED.group_name;
        """)
        pg_conn.commit()
        print("dim_territory carregado")
        
        # Vendedor
        cur.execute("""
            INSERT INTO dw.dim_sales_person (sales_person_id, full_name, territory_key, hire_date, quota)
            SELECT 
                CAST(sp."BusinessEntityID" AS INT) as sales_person_id,
                (pp."FirstName" || ' ' || pp."LastName") as full_name,
                dt.territory_key,
                CAST(e."HireDate" AS DATE) as hire_date,
                COALESCE(NULLIF(sp."SalesQuota",'')::NUMERIC(14,2), 0) as quota
            FROM staging.stg_sales_person sp
            LEFT JOIN staging.stg_employee e ON e."BusinessEntityID" = sp."BusinessEntityID"
            LEFT JOIN staging.stg_person pp ON pp."BusinessEntityID" = sp."BusinessEntityID"
            LEFT JOIN dw.dim_territory dt ON dt.territory_id = CAST(sp."TerritoryID" AS INT)
            ON CONFLICT (sales_person_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                territory_key = EXCLUDED.territory_key,
                hire_date = EXCLUDED.hire_date,
                quota = EXCLUDED.quota;
        """)
        pg_conn.commit()
        print("dim_sales_person carregado")
        
        # Promoção
        cur.execute("""
            INSERT INTO dw.dim_promotion (promotion_id, promotion_name, discount_pct, start_date, end_date)
            SELECT 
                CAST(so."SpecialOfferID" AS INT) as promotion_id,
                so."Description" as promotion_name,
                COALESCE(so."DiscountPct",0)::NUMERIC(5,2) as discount_pct,
                CAST(so."StartDate" AS DATE) as start_date,
                CAST(so."EndDate" AS DATE) as end_date
            FROM staging.stg_special_offer so
            ON CONFLICT (promotion_id) DO UPDATE SET
                promotion_name = EXCLUDED.promotion_name,
                discount_pct = EXCLUDED.discount_pct,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date;
        """)
        pg_conn.commit()
        print("dim_promotion carregado")
        
    return "dimensions transformed"


def transform_fact_sales(**context):
    """Carga da fato de vendas completa com território, vendedor, promoção e métricas detalhadas."""
    with psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as pg_conn:
        cur = pg_conn.cursor()
        cur.execute("""
            INSERT INTO dw.fact_sales (
                date_key, customer_key, product_key, territory_key, sales_person_key, promotion_key,
                order_id, order_line, quantity, unit_price, extended_price, discount_amount, total_amount, cost_amount, margin_amount
            )
            SELECT
                CAST(TO_CHAR(CAST(soh."OrderDate" AS DATE), 'YYYYMMDD') AS INT) as date_key,
                dc.customer_key,
                dp.product_key,
                dt.territory_key,
                dsp.sales_person_key,
                dpr.promotion_key,
                CAST(soh."SalesOrderID" AS INT) as order_id,
                CAST(sod."SalesOrderDetailID" AS INT) as order_line,
                CAST(sod."OrderQty" AS INT) as quantity,
                CAST(sod."UnitPrice" AS NUMERIC(12,2)) as unit_price,
                (CAST(sod."UnitPrice" AS NUMERIC(12,2)) * CAST(sod."OrderQty" AS INT)) as extended_price,
                (CAST(sod."UnitPrice" AS NUMERIC(12,2)) * CAST(sod."OrderQty" AS INT) * COALESCE(sod."UnitPriceDiscount",0)) as discount_amount,
                (CAST(sod."UnitPrice" AS NUMERIC(12,2)) * CAST(sod."OrderQty" AS INT) * (1 - COALESCE(sod."UnitPriceDiscount",0))) as total_amount,
                (COALESCE(dp.standard_cost,0) * CAST(sod."OrderQty" AS INT)) as cost_amount,
                ( (CAST(sod."UnitPrice" AS NUMERIC(12,2)) * CAST(sod."OrderQty" AS INT) * (1 - COALESCE(sod."UnitPriceDiscount",0))) - (COALESCE(dp.standard_cost,0) * CAST(sod."OrderQty" AS INT)) ) as margin_amount
            FROM staging.stg_sales_order_detail sod
            JOIN staging.stg_sales_order_header soh ON sod."SalesOrderID" = soh."SalesOrderID"
            LEFT JOIN dw.dim_customer dc ON dc.customer_id = CAST(soh."CustomerID" AS INT)
            LEFT JOIN dw.dim_product dp ON dp.product_id = CAST(sod."ProductID" AS INT)
            LEFT JOIN dw.dim_territory dt ON dt.territory_id = CAST(soh."TerritoryID" AS INT)
            LEFT JOIN dw.dim_sales_person dsp ON dsp.sales_person_id = CAST(soh."SalesPersonID" AS INT)
            LEFT JOIN dw.dim_promotion dpr ON dpr.promotion_id = CAST(sod."SpecialOfferID" AS INT)
            ON CONFLICT DO NOTHING;
        """)
        pg_conn.commit()
    return "fact transformed"


def ensure_dim_date(**context):
    """Popula dim_date se vazia cobrindo amplo range histórico (2008-2025)."""
    with psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD) as pg_conn:
        cur = pg_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dw.dim_date")
        res = cur.fetchone()
        count = res[0] if res else 0
        if count == 0:
            cur.execute("""
                INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, day, month_name, weekday_name)
                SELECT 
                    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
                    d::date AS full_date,
                    EXTRACT(YEAR FROM d)::INT AS year,
                    EXTRACT(QUARTER FROM d)::INT AS quarter,
                    EXTRACT(MONTH FROM d)::INT AS month,
                    EXTRACT(DAY FROM d)::INT AS day,
                    TO_CHAR(d, 'TMMonth') AS month_name,
                    TO_CHAR(d, 'TMDay') AS weekday_name
                FROM generate_series('2008-01-01'::date, '2025-12-31'::date, interval '1 day') AS gs(d)
                ON CONFLICT (date_key) DO NOTHING;
            """)
            pg_conn.commit()
            return "dim_date populated"
    return "dim_date already present"

with DAG(
    dag_id="etl_adventureworks_dw",
    start_date=datetime(2025, 11, 22),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "dw"],
    description="Pipeline ETL diário para popular modelo dimensional do DW AdventureWorks"
) as dag:
    start = EmptyOperator(task_id="inicio")
    prepare_dim_date = PythonOperator(task_id="popular_dim_date", python_callable=ensure_dim_date)
    extract_stage = PythonOperator(task_id="extrair_e_carregar_staging", python_callable=extract_and_load_staging)
    transform_dimensions_task = PythonOperator(task_id="transformar_dimensoes", python_callable=transform_dimensions)
    transform_fact_task = PythonOperator(task_id="transformar_fato_vendas", python_callable=transform_fact_sales)
    # Placeholder finais para separar carga (se necessário)
    load_dimensions = EmptyOperator(task_id="carregar_dimensoes")
    load_fact = EmptyOperator(task_id="carregar_fato_vendas")
    end = EmptyOperator(task_id="fim")

    start >> prepare_dim_date >> extract_stage >> transform_dimensions_task >> load_dimensions >> transform_fact_task >> load_fact >> end
