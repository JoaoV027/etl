"""
Script para testar KPIs do Data Warehouse AdventureWorks
Executa as queries de KPI e exibe os resultados
"""

import psycopg2
import pandas as pd
from datetime import datetime
import sys

# Configurações de conexão com o PostgreSQL DW
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'dw_adventureworks',
    'user': 'dw_user',
    'password': 'dw_password'
}

def conectar_db():
    """Estabelece conexão com o banco de dados DW"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            client_encoding='utf8'
        )
        print("[OK] Conexao estabelecida com o Data Warehouse")
        return conn
    except Exception as e:
        print(f"[ERRO] Erro ao conectar ao banco de dados: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def executar_kpi(conn, titulo, query, num_kpi):
    """Executa uma query de KPI e exibe os resultados"""
    print(f"\n{'='*80}")
    print(f"KPI {num_kpi}: {titulo}")
    print(f"{'='*80}")
    
    try:
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("[AVISO] Nenhum resultado encontrado")
            return
        
        print(f"\n[INFO] Total de registros: {len(df)}")
        print(f"\n{df.to_string(index=False)}")
        
    except Exception as e:
        print(f"[ERRO] Erro ao executar KPI {num_kpi}: {e}")

def main():
    """Função principal"""
    print("\n" + "="*80)
    print("TESTE DE KPIs - DATA WAREHOUSE ADVENTUREWORKS")
    print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    conn = conectar_db()
    
    # KPI 1: Margem de Contribuição Média por Produto
    kpi1_query = """
    SELECT 
        p.categoria_produto,
        p.subcategoria_produto,
        p.nome_produto,
        COUNT(DISTINCT fv.numero_pedido) AS total_pedidos,
        SUM(fv.quantidade_vendida) AS quantidade_total_vendida,
        SUM(fv.valor_liquido) AS receita_liquida,
        SUM(fv.lucro_bruto) AS lucro_bruto,
        ROUND(AVG(fv.margem_contribuicao), 2) AS margem_contribuicao_media_pct,
        ROUND(SUM(fv.lucro_bruto) / NULLIF(SUM(fv.valor_liquido), 0) * 100, 2) AS rentabilidade_pct
    FROM dw.fato_vendas fv
    INNER JOIN dw.dim_produto p ON fv.sk_produto = p.sk_produto
    WHERE fv.valor_liquido > 0
    GROUP BY p.categoria_produto, p.subcategoria_produto, p.nome_produto
    HAVING SUM(fv.valor_liquido) > 1000
    ORDER BY margem_contribuicao_media_pct DESC
    LIMIT 20;
    """
    executar_kpi(conn, "Margem de Contribuição Média por Produto", kpi1_query, 1)
    
    # KPI 2: Análise de Efetividade de Descontos
    kpi2_query = """
    SELECT 
        CASE 
            WHEN fv.percentual_desconto = 0 THEN 'Sem Desconto'
            WHEN fv.percentual_desconto <= 10 THEN '1-10%'
            WHEN fv.percentual_desconto <= 20 THEN '11-20%'
            WHEN fv.percentual_desconto <= 30 THEN '21-30%'
            ELSE 'Acima de 30%'
        END AS faixa_desconto,
        COUNT(*) AS numero_transacoes,
        SUM(fv.quantidade_vendida) AS unidades_vendidas,
        ROUND(SUM(fv.valor_bruto), 2) AS valor_bruto_total,
        ROUND(SUM(fv.valor_desconto), 2) AS desconto_concedido,
        ROUND(SUM(fv.valor_liquido), 2) AS receita_liquida,
        ROUND(SUM(fv.lucro_bruto), 2) AS lucro_total,
        ROUND(AVG(fv.margem_contribuicao), 2) AS margem_media_pct,
        ROUND(SUM(fv.valor_desconto) / NULLIF(SUM(fv.valor_bruto), 0) * 100, 2) AS taxa_desconto_media_pct
    FROM dw.fato_vendas fv
    GROUP BY 1
    ORDER BY 1;
    """
    executar_kpi(conn, "Análise de Efetividade de Descontos", kpi2_query, 2)
    
    # KPI 3: Performance de Vendedores por Região
    kpi3_query = """
    SELECT 
        r.grupo_regional,
        r.nome_territorio,
        v.nome_vendedor,
        v.meta_anual,
        COUNT(DISTINCT fv.numero_pedido) AS total_vendas,
        ROUND(SUM(fv.valor_liquido), 2) AS receita_gerada,
        ROUND(SUM(fv.lucro_bruto), 2) AS lucro_gerado,
        ROUND(SUM(fv.valor_liquido) / NULLIF(v.meta_anual, 0) * 100, 2) AS percentual_meta_atingido,
        ROUND(SUM(fv.valor_liquido) / NULLIF(COUNT(DISTINCT fv.numero_pedido), 0), 2) AS ticket_medio_vendedor,
        ROUND(AVG(fv.margem_contribuicao), 2) AS margem_media_pct
    FROM dw.fato_vendas fv
    INNER JOIN dw.dim_vendedor v ON fv.sk_vendedor = v.sk_vendedor
    INNER JOIN dw.dim_regiao r ON fv.sk_regiao = r.sk_regiao
    WHERE v.meta_anual > 0
    GROUP BY r.grupo_regional, r.nome_territorio, v.nome_vendedor, v.meta_anual
    ORDER BY receita_gerada DESC
    LIMIT 15;
    """
    executar_kpi(conn, "Performance de Vendedores por Região", kpi3_query, 3)
    
    # KPI 4: Evolução Temporal de Receita
    kpi4_query = """
    WITH metricas_mensais AS (
        SELECT 
            t.ano,
            t.mes,
            t.nome_mes,
            SUM(fv.valor_liquido) AS receita_mes,
            SUM(fv.lucro_bruto) AS lucro_mes,
            COUNT(DISTINCT fv.numero_pedido) AS pedidos_mes,
            COUNT(DISTINCT fv.sk_cliente) AS clientes_unicos
        FROM dw.fato_vendas fv
        INNER JOIN dw.dim_tempo t ON fv.sk_tempo = t.sk_tempo
        GROUP BY t.ano, t.mes, t.nome_mes
    )
    SELECT 
        ano,
        mes,
        nome_mes,
        ROUND(receita_mes, 2) AS receita_mensal,
        ROUND(lucro_mes, 2) AS lucro_mensal,
        ROUND(lucro_mes / NULLIF(receita_mes, 0) * 100, 2) AS margem_lucro_pct,
        pedidos_mes,
        clientes_unicos,
        ROUND(receita_mes / NULLIF(pedidos_mes, 0), 2) AS ticket_medio
    FROM metricas_mensais
    ORDER BY ano DESC, mes DESC
    LIMIT 12;
    """
    executar_kpi(conn, "Evolução Temporal de Receita e Lucratividade", kpi4_query, 4)
    
    # KPI 5: Análise ABC de Produtos
    kpi5_query = """
    WITH ranking_produtos AS (
        SELECT 
            p.sk_produto,
            p.nome_produto,
            p.categoria_produto,
            SUM(fv.valor_liquido) AS receita_produto,
            SUM(SUM(fv.valor_liquido)) OVER () AS receita_total,
            SUM(fv.quantidade_vendida) AS quantidade_vendida
        FROM dw.fato_vendas fv
        INNER JOIN dw.dim_produto p ON fv.sk_produto = p.sk_produto
        GROUP BY p.sk_produto, p.nome_produto, p.categoria_produto
    ),
    acumulado AS (
        SELECT 
            *,
            SUM(receita_produto) OVER (ORDER BY receita_produto DESC) AS receita_acumulada,
            ROUND(
                SUM(receita_produto) OVER (ORDER BY receita_produto DESC) / 
                NULLIF(receita_total, 0) * 100, 
                2
            ) AS percentual_acumulado
        FROM ranking_produtos
    )
    SELECT 
        nome_produto,
        categoria_produto,
        ROUND(receita_produto, 2) AS receita,
        quantidade_vendida,
        percentual_acumulado,
        CASE 
            WHEN percentual_acumulado <= 80 THEN 'A - Alto Valor'
            WHEN percentual_acumulado <= 95 THEN 'B - Médio Valor'
            ELSE 'C - Baixo Valor'
        END AS classificacao_abc
    FROM acumulado
    ORDER BY receita_produto DESC
    LIMIT 30;
    """
    executar_kpi(conn, "Análise ABC de Produtos", kpi5_query, 5)
    
    # KPI 6: Concentração Geográfica de Vendas
    kpi6_query = """
    SELECT 
        r.continente,
        r.grupo_regional,
        r.nome_territorio,
        COUNT(DISTINCT fv.sk_cliente) AS total_clientes,
        COUNT(DISTINCT fv.numero_pedido) AS total_pedidos,
        SUM(fv.quantidade_vendida) AS unidades_vendidas,
        ROUND(SUM(fv.valor_liquido), 2) AS receita_total,
        ROUND(SUM(fv.lucro_bruto), 2) AS lucro_total,
        ROUND(AVG(fv.margem_contribuicao), 2) AS margem_media_pct,
        ROUND(
            SUM(fv.valor_liquido) / NULLIF(SUM(SUM(fv.valor_liquido)) OVER (), 0) * 100, 
            2
        ) AS participacao_receita_pct,
        ROUND(SUM(fv.valor_liquido) / NULLIF(COUNT(DISTINCT fv.numero_pedido), 0), 2) AS ticket_medio_regiao
    FROM dw.fato_vendas fv
    INNER JOIN dw.dim_regiao r ON fv.sk_regiao = r.sk_regiao
    GROUP BY r.continente, r.grupo_regional, r.nome_territorio
    ORDER BY receita_total DESC
    LIMIT 15;
    """
    executar_kpi(conn, "Concentração Geográfica de Vendas", kpi6_query, 6)
    
    # KPI 7: Eficiência Operacional por Categoria
    kpi7_query = """
    SELECT 
        p.categoria_produto,
        COUNT(DISTINCT p.sk_produto) AS total_produtos_categoria,
        COUNT(DISTINCT fv.numero_pedido) AS total_transacoes,
        SUM(fv.quantidade_vendida) AS unidades_vendidas,
        ROUND(SUM(fv.valor_liquido), 2) AS receita_categoria,
        ROUND(SUM(fv.custo_total), 2) AS custo_categoria,
        ROUND(SUM(fv.lucro_bruto), 2) AS lucro_categoria,
        ROUND(SUM(fv.lucro_bruto) / NULLIF(SUM(fv.valor_liquido), 0) * 100, 2) AS margem_lucro_pct,
        ROUND(SUM(fv.valor_liquido) / NULLIF(COUNT(DISTINCT p.sk_produto), 0), 2) AS receita_media_produto,
        ROUND(SUM(fv.quantidade_vendida) / NULLIF(COUNT(DISTINCT p.sk_produto), 0), 2) AS giro_medio_produto,
        ROUND(
            SUM(fv.valor_liquido) / NULLIF(SUM(SUM(fv.valor_liquido)) OVER (), 0) * 100, 
            2
        ) AS participacao_receita_total_pct
    FROM dw.fato_vendas fv
    INNER JOIN dw.dim_produto p ON fv.sk_produto = p.sk_produto
    GROUP BY p.categoria_produto
    ORDER BY receita_categoria DESC;
    """
    executar_kpi(conn, "Eficiência Operacional por Categoria", kpi7_query, 7)
    
    # KPI 8: Customer Lifetime Value (CLV) por Segmento
    kpi8_query = """
    WITH metricas_cliente AS (
        SELECT 
            c.tipo_cliente,
            c.segmento,
            COUNT(DISTINCT fv.sk_cliente) AS total_clientes,
            SUM(fv.valor_liquido) AS receita_total_segmento,
            SUM(fv.lucro_bruto) AS lucro_total_segmento,
            COUNT(DISTINCT fv.numero_pedido) AS total_pedidos,
            AVG(fv.valor_liquido) AS ticket_medio
        FROM dw.fato_vendas fv
        INNER JOIN dw.dim_cliente c ON fv.sk_cliente = c.sk_cliente
        GROUP BY c.tipo_cliente, c.segmento
    )
    SELECT 
        tipo_cliente,
        segmento,
        total_clientes,
        ROUND(receita_total_segmento, 2) AS receita_total,
        ROUND(lucro_total_segmento, 2) AS lucro_total,
        ROUND(receita_total_segmento / NULLIF(total_clientes, 0), 2) AS receita_media_por_cliente,
        ROUND(lucro_total_segmento / NULLIF(total_clientes, 0), 2) AS lucro_medio_por_cliente,
        ROUND(total_pedidos::NUMERIC / NULLIF(total_clientes, 0), 2) AS pedidos_por_cliente,
        ROUND(ticket_medio, 2) AS ticket_medio,
        CASE 
            WHEN receita_total_segmento / NULLIF(total_clientes, 0) >= 50000 THEN 'VIP'
            WHEN receita_total_segmento / NULLIF(total_clientes, 0) >= 20000 THEN 'Premium'
            WHEN receita_total_segmento / NULLIF(total_clientes, 0) >= 5000 THEN 'Regular'
            ELSE 'Basico'
        END AS categoria_segmento
    FROM metricas_cliente
    ORDER BY receita_total DESC;
    """
    executar_kpi(conn, "Customer Lifetime Value por Segmento", kpi8_query, 8)
    
    # KPI 9: Análise de Sazonalidade
    kpi9_query = """
    SELECT 
        t.trimestre,
        t.nome_dia_semana,
        t.eh_fim_semana,
        COUNT(DISTINCT fv.numero_pedido) AS total_vendas,
        SUM(fv.quantidade_vendida) AS unidades_vendidas,
        ROUND(SUM(fv.valor_liquido), 2) AS receita_total,
        ROUND(AVG(fv.valor_liquido), 2) AS valor_medio_transacao,
        ROUND(SUM(fv.lucro_bruto), 2) AS lucro_total,
        ROUND(AVG(fv.margem_contribuicao), 2) AS margem_media_pct
    FROM dw.fato_vendas fv
    INNER JOIN dw.dim_tempo t ON fv.sk_tempo = t.sk_tempo
    GROUP BY t.trimestre, t.nome_dia_semana, t.eh_fim_semana
    ORDER BY t.trimestre, 
        CASE t.nome_dia_semana
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
        END;
    """
    executar_kpi(conn, "Análise de Sazonalidade", kpi9_query, 9)
    
    # KPI 10: Top 10 Produtos por Receita (Resumo Executivo)
    kpi10_query = """
    SELECT 
        p.categoria_produto,
        p.nome_produto,
        COUNT(DISTINCT fv.numero_pedido) AS total_pedidos,
        SUM(fv.quantidade_vendida) AS quantidade_vendida,
        ROUND(SUM(fv.valor_liquido), 2) AS receita_total,
        ROUND(SUM(fv.lucro_bruto), 2) AS lucro_total,
        ROUND(AVG(fv.margem_contribuicao), 2) AS margem_media_pct,
        ROUND(SUM(fv.valor_liquido) / NULLIF(SUM(fv.quantidade_vendida), 0), 2) AS preco_medio_unitario
    FROM dw.fato_vendas fv
    INNER JOIN dw.dim_produto p ON fv.sk_produto = p.sk_produto
    GROUP BY p.categoria_produto, p.nome_produto
    ORDER BY receita_total DESC
    LIMIT 10;
    """
    executar_kpi(conn, "Top 10 Produtos por Receita", kpi10_query, 10)
    
    conn.close()
    
    print("\n" + "="*80)
    print("[OK] TESTE DE KPIs CONCLUIDO COM SUCESSO!")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
