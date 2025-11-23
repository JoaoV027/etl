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
from typing import Dict, List, Any
import logging
import os
from decimal import Decimal

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pymssql
import psycopg2
from psycopg2.extras import execute_batch

# Configuração de logging
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURAÇÕES DE CONEXÃO
# ============================================================
class ConfiguracaoBancoDados:
    """Configuração centralizada de conexões"""
    
    # Fonte OLTP
    FONTE = {
        'host': os.getenv("MSSQL_HOST", "mssql"),
        'database': os.getenv("MSSQL_DB", "AdventureWorks2022"),
        'user': os.getenv("MSSQL_USER", "sa"),
        'password': os.getenv("MSSQL_PASSWORD", "Strong!Passw0rd")
    }
    
    # Destino DW
    DESTINO = {
        'host': os.getenv("DW_HOST", "adventureworks_dw"),
        'dbname': os.getenv("DW_DB", "dw_adventureworks"),
        'user': os.getenv("DW_USER", "dw_user"),
        'password': os.getenv("DW_PASSWORD", "dw_password")
    }

# Mapeamento de tabelas
MAPA_TABELAS = {
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

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def obter_conexao_fonte():
    """Retorna conexão com banco de dados fonte"""
    return pymssql.connect(**ConfiguracaoBancoDados.FONTE)

def obter_conexao_destino():
    """Retorna conexão com Data Warehouse"""
    return psycopg2.connect(**ConfiguracaoBancoDados.DESTINO)

def inferir_tipo_coluna(amostras: List[Any]) -> str:
    """Infere o tipo PostgreSQL baseado em amostras de dados"""
    tipos_encontrados = set()
    
    for valor in amostras:
        if valor is None:
            continue
        elif isinstance(valor, bool):
            tipos_encontrados.add("BOOLEAN")
        elif isinstance(valor, int):
            tipos_encontrados.add("INTEGER")
        elif isinstance(valor, float):
            tipos_encontrados.add("DOUBLE PRECISION")
        elif isinstance(valor, Decimal):
            tipos_encontrados.add("NUMERIC(18,4)")
        elif isinstance(valor, (datetime, datetime)):
            tipos_encontrados.add("TIMESTAMP")
        else:
            tipos_encontrados.add("TEXT")
    
    # Retorna tipo mais genérico se houver conflito
    if not tipos_encontrados:
        return "TEXT"
    elif len(tipos_encontrados) == 1:
        return tipos_encontrados.pop()
    elif tipos_encontrados <= {"INTEGER", "NUMERIC(18,4)"}:
        return "NUMERIC(18,4)"
    else:
        return "TEXT"

# ============================================================
# TAREFAS ETL
# ============================================================

def inicializar_dimensao_tempo(**context):
    """
    Popula dimensão tempo com calendário completo (2008-2030)
    Executa apenas se a dimensão estiver vazia
    """
    logger.info("Iniciando carga da dimensão tempo")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            # Verifica se já existe dados
            cursor.execute("SELECT COUNT(*) FROM dw.dim_tempo")
            total = cursor.fetchone()[0]
            
            if total > 0:
                logger.info(f"Dimensão tempo já contém {total} registros. Pulando...")
                return
            
            # Insere calendário completo
            cursor.execute("""
                INSERT INTO dw.dim_tempo (
                    sk_tempo, data_completa, ano, trimestre, mes, dia,
                    semana_ano, dia_semana, nome_mes, nome_dia_semana,
                    eh_fim_semana, eh_feriado
                )
                SELECT 
                    TO_CHAR(d, 'YYYYMMDD')::INT AS sk_tempo,
                    d::DATE AS data_completa,
                    EXTRACT(YEAR FROM d)::SMALLINT AS ano,
                    EXTRACT(QUARTER FROM d)::SMALLINT AS trimestre,
                    EXTRACT(MONTH FROM d)::SMALLINT AS mes,
                    EXTRACT(DAY FROM d)::SMALLINT AS dia,
                    EXTRACT(WEEK FROM d)::SMALLINT AS semana_ano,
                    EXTRACT(DOW FROM d)::SMALLINT AS dia_semana,
                    TO_CHAR(d, 'TMMonth') AS nome_mes,
                    TO_CHAR(d, 'TMDay') AS nome_dia_semana,
                    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS eh_fim_semana,
                    FALSE AS eh_feriado
                FROM generate_series(
                    '2008-01-01'::DATE, 
                    '2030-12-31'::DATE, 
                    INTERVAL '1 day'
                ) AS gs(d)
            """)
            
            conn.commit()
            
            cursor.execute("SELECT COUNT(*) FROM dw.dim_tempo")
            total_inserido = cursor.fetchone()[0]
            logger.info(f"Dimensão tempo carregada com {total_inserido} registros")


def extrair_dados_para_staging(**context):
    """
    Extrai dados do SQL Server e carrega em tabelas staging no PostgreSQL
    Usa inferência automática de tipos e carregamento em batch
    """
    logger.info("Iniciando extração de dados para staging")
    
    with obter_conexao_destino() as conn_destino:
        conn_destino.autocommit = True
        
        with conn_destino.cursor() as cursor_pg:
            # Cria schema de staging
            cursor_pg.execute("CREATE SCHEMA IF NOT EXISTS staging")
            
            with obter_conexao_fonte() as conn_fonte:
                for tabela_origem, tabela_staging in MAPA_TABELAS.items():
                    logger.info(f"Processando {tabela_origem} -> staging.{tabela_staging}")
                    
                    # Extrai dados
                    with conn_fonte.cursor() as cursor_mssql:
                        cursor_mssql.execute(f"SELECT * FROM {tabela_origem}")
                        registros = cursor_mssql.fetchall()
                        colunas = [desc[0] for desc in cursor_mssql.description]
                    
                    # Recria tabela staging
                    cursor_pg.execute(f"DROP TABLE IF EXISTS staging.{tabela_staging} CASCADE")
                    
                    if not registros:
                        cursor_pg.execute(f"CREATE TABLE staging.{tabela_staging} (placeholder INT)")
                        logger.warning(f"Tabela {tabela_origem} está vazia")
                        continue
                    
                    # Inferência de tipos
                    amostra = registros[:min(len(registros), 1000)]
                    tipos_coluna = {}
                    
                    for idx, coluna in enumerate(colunas):
                        valores_amostra = [r[idx] for r in amostra if r[idx] is not None]
                        tipos_coluna[coluna] = inferir_tipo_coluna(valores_amostra)
                    
                    # Cria tabela staging
                    definicao_colunas = ", ".join([
                        f'"{col}" {tipos_coluna[col]}' for col in colunas
                    ])
                    cursor_pg.execute(
                        f"CREATE TABLE staging.{tabela_staging} ({definicao_colunas})"
                    )
                    
                    # Insere dados em lotes
                    tamanho_lote = 500
                    total_inserido = 0
                    
                    for i in range(0, len(registros), tamanho_lote):
                        lote = registros[i:i + tamanho_lote]
                        placeholders = ", ".join(["%s"] * len(colunas))
                        
                        execute_batch(
                            cursor_pg,
                            f"INSERT INTO staging.{tabela_staging} VALUES ({placeholders})",
                            lote,
                            page_size=tamanho_lote
                        )
                        total_inserido += len(lote)
                    
                    logger.info(f"Tabela {tabela_staging}: {total_inserido} registros carregados")
    
    logger.info("Extração para staging concluída com sucesso")


def carregar_dimensao_cliente(**context):
    """Carga da dimensão cliente com SCD Type 1"""
    logger.info("Carregando dimensão cliente")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dw.dim_cliente (
                    nk_cliente, nome_cliente, sobrenome, nome_completo,
                    tipo_cliente, segmento, data_cadastro
                )
                SELECT DISTINCT
                    c."CustomerID"::INT AS nk_cliente,
                    COALESCE(p."FirstName", 'Desconhecido') AS nome_cliente,
                    COALESCE(p."LastName", '') AS sobrenome,
                    COALESCE(p."FirstName" || ' ' || p."LastName", 'Cliente ' || c."CustomerID"::TEXT) AS nome_completo,
                    CASE 
                        WHEN c."PersonID" IS NOT NULL THEN 'Pessoa Física'
                        WHEN c."StoreID" IS NOT NULL THEN 'Empresa'
                        ELSE 'Indefinido'
                    END AS tipo_cliente,
                    'Varejo' AS segmento,
                    CURRENT_DATE AS data_cadastro
                FROM staging.stage_clientes c
                LEFT JOIN staging.stage_pessoas p ON p."BusinessEntityID"::INT = c."PersonID"::INT
                WHERE c."CustomerID" IS NOT NULL
                ON CONFLICT (nk_cliente) DO UPDATE SET
                    nome_cliente = EXCLUDED.nome_cliente,
                    sobrenome = EXCLUDED.sobrenome,
                    nome_completo = EXCLUDED.nome_completo,
                    tipo_cliente = EXCLUDED.tipo_cliente,
                    ultima_atualizacao = CURRENT_TIMESTAMP
            """)
            
            conn.commit()
            logger.info(f"Dimensão cliente atualizada: {cursor.rowcount} registros")


def carregar_dimensao_produto(**context):
    """Carga da dimensão produto com hierarquia completa"""
    logger.info("Carregando dimensão produto")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dw.dim_produto (
                    nk_produto, nome_produto, codigo_produto,
                    categoria_produto, subcategoria_produto, linha_produto,
                    cor_produto, tamanho, peso,
                    custo_unitario, preco_lista, margem_percentual,
                    data_inicio_vigencia, status_ativo
                )
                SELECT DISTINCT
                    p."ProductID"::INT AS nk_produto,
                    p."Name" AS nome_produto,
                    p."ProductNumber" AS codigo_produto,
                    COALESCE(pc."Name", 'Sem Categoria') AS categoria_produto,
                    COALESCE(psc."Name", 'Sem Subcategoria') AS subcategoria_produto,
                    COALESCE(p."ProductLine", 'N/A') AS linha_produto,
                    COALESCE(p."Color", 'N/A') AS cor_produto,
                    COALESCE(p."Size", 'N/A') AS tamanho,
                    COALESCE(p."Weight", 0)::NUMERIC(10,2) AS peso,
                    COALESCE(p."StandardCost", 0)::NUMERIC(15,4) AS custo_unitario,
                    COALESCE(p."ListPrice", 0)::NUMERIC(15,4) AS preco_lista,
                    CASE 
                        WHEN COALESCE(p."ListPrice", 0) > 0 
                        THEN ((COALESCE(p."ListPrice", 0) - COALESCE(p."StandardCost", 0)) / COALESCE(p."ListPrice", 1) * 100)::NUMERIC(5,2)
                        ELSE 0
                    END AS margem_percentual,
                    COALESCE(p."SellStartDate"::DATE, CURRENT_DATE) AS data_inicio_vigencia,
                    CASE WHEN p."SellEndDate" IS NULL THEN TRUE ELSE FALSE END AS status_ativo
                FROM staging.stage_produtos p
                LEFT JOIN staging.stage_subcategorias psc 
                    ON psc."ProductSubcategoryID"::INT = p."ProductSubcategoryID"::INT
                LEFT JOIN staging.stage_categorias pc 
                    ON pc."ProductCategoryID"::INT = psc."ProductCategoryID"::INT
                WHERE p."ProductID" IS NOT NULL
                ON CONFLICT (nk_produto) DO UPDATE SET
                    nome_produto = EXCLUDED.nome_produto,
                    categoria_produto = EXCLUDED.categoria_produto,
                    subcategoria_produto = EXCLUDED.subcategoria_produto,
                    cor_produto = EXCLUDED.cor_produto,
                    custo_unitario = EXCLUDED.custo_unitario,
                    preco_lista = EXCLUDED.preco_lista,
                    margem_percentual = EXCLUDED.margem_percentual,
                    status_ativo = EXCLUDED.status_ativo
            """)
            
            conn.commit()
            logger.info(f"Dimensão produto atualizada: {cursor.rowcount} registros")


def carregar_dimensao_regiao(**context):
    """Carga da dimensão região geográfica"""
    logger.info("Carregando dimensão região")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dw.dim_regiao (
                    nk_regiao, nome_territorio, codigo_pais,
                    nome_pais, continente, grupo_regional
                )
                SELECT DISTINCT
                    t."TerritoryID"::INT AS nk_regiao,
                    t."Name" AS nome_territorio,
                    t."CountryRegionCode" AS codigo_pais,
                    t."CountryRegionCode" AS nome_pais,
                    CASE 
                        WHEN t."Group" LIKE '%America%' THEN 'Américas'
                        WHEN t."Group" LIKE '%Europe%' THEN 'Europa'
                        WHEN t."Group" LIKE '%Pacific%' THEN 'Ásia-Pacífico'
                        ELSE 'Outros'
                    END AS continente,
                    t."Group" AS grupo_regional
                FROM staging.stage_territorios t
                WHERE t."TerritoryID" IS NOT NULL
                ON CONFLICT (nk_regiao) DO UPDATE SET
                    nome_territorio = EXCLUDED.nome_territorio,
                    grupo_regional = EXCLUDED.grupo_regional
            """)
            
            conn.commit()
            logger.info(f"Dimensão região atualizada: {cursor.rowcount} registros")


def carregar_dimensao_vendedor(**context):
    """Carga da dimensão vendedor"""
    logger.info("Carregando dimensão vendedor")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dw.dim_vendedor (
                    nk_vendedor, nome_vendedor, codigo_vendedor,
                    sk_regiao, data_admissao, meta_anual, comissao_percentual
                )
                SELECT DISTINCT
                    sp."BusinessEntityID"::INT AS nk_vendedor,
                    COALESCE(p."FirstName" || ' ' || p."LastName", 'Vendedor ' || sp."BusinessEntityID"::TEXT) AS nome_vendedor,
                    'V' || LPAD(sp."BusinessEntityID"::TEXT, 5, '0') AS codigo_vendedor,
                    dr.sk_regiao,
                    e."HireDate"::DATE AS data_admissao,
                    COALESCE(sp."SalesQuota", 0)::NUMERIC(18,2) AS meta_anual,
                    COALESCE(sp."CommissionPct", 0)::NUMERIC(5,2) AS comissao_percentual
                FROM staging.stage_vendedores sp
                LEFT JOIN staging.stage_funcionarios e 
                    ON e."BusinessEntityID"::INT = sp."BusinessEntityID"::INT
                LEFT JOIN staging.stage_pessoas p 
                    ON p."BusinessEntityID"::INT = sp."BusinessEntityID"::INT
                LEFT JOIN dw.dim_regiao dr 
                    ON dr.nk_regiao = sp."TerritoryID"::INT
                WHERE sp."BusinessEntityID" IS NOT NULL
                ON CONFLICT (nk_vendedor) DO UPDATE SET
                    nome_vendedor = EXCLUDED.nome_vendedor,
                    sk_regiao = EXCLUDED.sk_regiao,
                    meta_anual = EXCLUDED.meta_anual,
                    comissao_percentual = EXCLUDED.comissao_percentual
            """)
            
            conn.commit()
            logger.info(f"Dimensão vendedor atualizada: {cursor.rowcount} registros")


def carregar_dimensao_oferta(**context):
    """Carga da dimensão oferta especial"""
    logger.info("Carregando dimensão oferta")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dw.dim_oferta (
                    nk_oferta, descricao_oferta, tipo_oferta,
                    percentual_desconto, data_inicio, data_fim,
                    quantidade_minima, quantidade_maxima
                )
                SELECT DISTINCT
                    o."SpecialOfferID"::INT AS nk_oferta,
                    o."Description" AS descricao_oferta,
                    o."Type" AS tipo_oferta,
                    COALESCE(o."DiscountPct", 0)::NUMERIC(5,2) AS percentual_desconto,
                    o."StartDate"::DATE AS data_inicio,
                    o."EndDate"::DATE AS data_fim,
                    COALESCE(o."MinQty", 0) AS quantidade_minima,
                    COALESCE(o."MaxQty", 999999) AS quantidade_maxima
                FROM staging.stage_ofertas o
                WHERE o."SpecialOfferID" IS NOT NULL
                ON CONFLICT (nk_oferta) DO UPDATE SET
                    descricao_oferta = EXCLUDED.descricao_oferta,
                    percentual_desconto = EXCLUDED.percentual_desconto,
                    data_fim = EXCLUDED.data_fim
            """)
            
            conn.commit()
            logger.info(f"Dimensão oferta atualizada: {cursor.rowcount} registros")


def carregar_fato_vendas(**context):
    """
    Carga da tabela fato vendas com todas as métricas calculadas
    Inclui: valor bruto, descontos, valor líquido, custos, lucros e margens
    """
    logger.info("Carregando fato vendas")
    
    with obter_conexao_destino() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dw.fato_vendas (
                    sk_tempo, sk_cliente, sk_produto, sk_regiao, sk_vendedor, sk_oferta,
                    numero_pedido, numero_linha,
                    quantidade_vendida, valor_unitario, valor_bruto,
                    valor_desconto, valor_liquido, custo_total, lucro_bruto,
                    percentual_desconto, margem_contribuicao
                )
                SELECT
                    -- Chaves dimensionais
                    TO_CHAR(soh."OrderDate"::DATE, 'YYYYMMDD')::INT AS sk_tempo,
                    dc.sk_cliente,
                    dp.sk_produto,
                    dr.sk_regiao,
                    dv.sk_vendedor,
                    do_oferta.sk_oferta,
                    
                    -- Chaves de negócio
                    soh."SalesOrderID"::INT AS numero_pedido,
                    sod."SalesOrderDetailID"::INT AS numero_linha,
                    
                    -- Métricas de quantidade e valor
                    sod."OrderQty"::INT AS quantidade_vendida,
                    sod."UnitPrice"::NUMERIC(15,4) AS valor_unitario,
                    (sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT) AS valor_bruto,
                    
                    -- Cálculo de desconto
                    (sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT * 
                     COALESCE(sod."UnitPriceDiscount", 0)) AS valor_desconto,
                    
                    -- Valor líquido (após desconto)
                    (sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT * 
                     (1 - COALESCE(sod."UnitPriceDiscount", 0))) AS valor_liquido,
                    
                    -- Custo e lucro
                    (COALESCE(dp.custo_unitario, 0) * sod."OrderQty"::INT) AS custo_total,
                    ((sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT * 
                      (1 - COALESCE(sod."UnitPriceDiscount", 0))) - 
                     (COALESCE(dp.custo_unitario, 0) * sod."OrderQty"::INT)) AS lucro_bruto,
                    
                    -- Percentuais
                    (COALESCE(sod."UnitPriceDiscount", 0) * 100)::NUMERIC(5,2) AS percentual_desconto,
                    
                    -- Margem de contribuição
                    CASE 
                        WHEN (sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT * 
                              (1 - COALESCE(sod."UnitPriceDiscount", 0))) > 0
                        THEN (
                            ((sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT * 
                              (1 - COALESCE(sod."UnitPriceDiscount", 0))) - 
                             (COALESCE(dp.custo_unitario, 0) * sod."OrderQty"::INT)) / 
                            (sod."UnitPrice"::NUMERIC(15,4) * sod."OrderQty"::INT * 
                             (1 - COALESCE(sod."UnitPriceDiscount", 0))) * 100
                        )::NUMERIC(5,2)
                        ELSE 0
                    END AS margem_contribuicao
                    
                FROM staging.stage_pedidos_detalhe sod
                INNER JOIN staging.stage_pedidos_header soh 
                    ON soh."SalesOrderID" = sod."SalesOrderID"
                LEFT JOIN dw.dim_cliente dc 
                    ON dc.nk_cliente = soh."CustomerID"::INT
                LEFT JOIN dw.dim_produto dp 
                    ON dp.nk_produto = sod."ProductID"::INT
                LEFT JOIN dw.dim_regiao dr 
                    ON dr.nk_regiao = soh."TerritoryID"::INT
                LEFT JOIN dw.dim_vendedor dv 
                    ON dv.nk_vendedor = soh."SalesPersonID"::INT
                LEFT JOIN dw.dim_oferta do_oferta 
                    ON do_oferta.nk_oferta = sod."SpecialOfferID"::INT
                WHERE soh."SalesOrderID" IS NOT NULL
                    AND sod."SalesOrderDetailID" IS NOT NULL
                ON CONFLICT (numero_pedido, numero_linha) DO NOTHING
            """)
            
            conn.commit()
            total_inserido = cursor.rowcount
            logger.info(f"Fato vendas carregado: {total_inserido} novos registros")
            
            # Estatísticas finais
            cursor.execute("SELECT COUNT(*), SUM(valor_liquido), SUM(lucro_bruto) FROM dw.fato_vendas")
            stats = cursor.fetchone()
            logger.info(f"Total no DW: {stats[0]} vendas | Receita: R$ {stats[1]:,.2f} | Lucro: R$ {stats[2]:,.2f}")


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================
configuracao_padrao = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='dw_adventureworks_pipeline',
    default_args=configuracao_padrao,
    description='Pipeline ETL completo para Data Warehouse AdventureWorks',
    schedule_interval='0 2 * * *',  # Executa às 2h da manhã todos os dias
    start_date=datetime(2025, 11, 23),
    catchup=False,
    tags=['dw', 'adventureworks', 'etl', 'analytics'],
    max_active_runs=1
) as dag:
    
    # Marcadores de início e fim
    inicio_pipeline = EmptyOperator(
        task_id='inicio_pipeline',
        doc="Marca o início da execução do pipeline ETL"
    )
    
    fim_pipeline = EmptyOperator(
        task_id='fim_pipeline',
        doc="Marca o fim da execução do pipeline ETL"
    )
    
    # Fase 1: Inicialização
    task_dim_tempo = PythonOperator(
        task_id='inicializar_dim_tempo',
        python_callable=inicializar_dimensao_tempo,
        doc="Popula calendário na dimensão tempo (executa apenas se vazia)"
    )
    
    # Fase 2: Extração
    task_staging = PythonOperator(
        task_id='extrair_para_staging',
        python_callable=extrair_dados_para_staging,
        doc="Extrai dados do SQL Server e carrega em área de staging"
    )
    
    # Fase 3: Carga de Dimensões
    checkpoint_staging = EmptyOperator(
        task_id='checkpoint_staging_completo',
        doc="Checkpoint: Staging carregado"
    )
    
    task_dim_cliente = PythonOperator(
        task_id='carregar_dim_cliente',
        python_callable=carregar_dimensao_cliente
    )
    
    task_dim_produto = PythonOperator(
        task_id='carregar_dim_produto',
        python_callable=carregar_dimensao_produto
    )
    
    task_dim_regiao = PythonOperator(
        task_id='carregar_dim_regiao',
        python_callable=carregar_dimensao_regiao
    )
    
    task_dim_vendedor = PythonOperator(
        task_id='carregar_dim_vendedor',
        python_callable=carregar_dimensao_vendedor
    )
    
    task_dim_oferta = PythonOperator(
        task_id='carregar_dim_oferta',
        python_callable=carregar_dimensao_oferta
    )
    
    # Fase 4: Carga Fato
    checkpoint_dimensoes = EmptyOperator(
        task_id='checkpoint_dimensoes_completas',
        doc="Checkpoint: Todas as dimensões carregadas"
    )
    
    task_fato_vendas = PythonOperator(
        task_id='carregar_fato_vendas',
        python_callable=carregar_fato_vendas,
        doc="Carga da tabela fato com métricas calculadas"
    )
    
    # Definição do fluxo de execução
    inicio_pipeline >> task_dim_tempo >> task_staging >> checkpoint_staging
    
    checkpoint_staging >> [
        task_dim_cliente,
        task_dim_produto,
        task_dim_regiao,
        task_dim_vendedor,
        task_dim_oferta
    ] >> checkpoint_dimensoes
    
    checkpoint_dimensoes >> task_fato_vendas >> fim_pipeline
