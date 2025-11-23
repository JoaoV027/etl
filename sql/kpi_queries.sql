-- ============================================================================
-- KPI QUERIES - DATA WAREHOUSE ADVENTUREWORKS
-- Arquivo: kpi_queries.sql
-- Descrição: Consultas SQL para análise de KPIs de negócio
-- Data: 23/11/2025
-- ============================================================================

-- ============================================================================
-- KPI 1: Margem de Contribuição Média por Produto
-- ============================================================================
-- Objetivo: Identificar produtos com melhor rentabilidade
-- Métrica: Margem de contribuição média em percentual
-- Critérios: Apenas produtos com receita > R$ 1.000
-- ============================================================================

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


-- ============================================================================
-- KPI 2: Análise de Efetividade de Descontos
-- ============================================================================
-- Objetivo: Avaliar impacto dos descontos na lucratividade
-- Métrica: Margem média e taxa de desconto por faixa
-- Faixas: Sem Desconto, 1-10%, 11-20%, 21-30%, Acima de 30%
-- ============================================================================

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


-- ============================================================================
-- KPI 3: Performance de Vendedores por Região
-- ============================================================================
-- Objetivo: Avaliar desempenho individual dos vendedores
-- Métrica: Receita gerada vs meta anual (percentual de atingimento)
-- Filtro: Apenas vendedores com meta definida (meta_anual > 0)
-- ============================================================================

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


-- ============================================================================
-- KPI 4: Evolução Temporal de Receita e Lucratividade
-- ============================================================================
-- Objetivo: Analisar tendências mensais de receita e margem
-- Métrica: Receita, lucro, margem % e ticket médio por mês
-- Período: Últimos 12 meses disponíveis
-- ============================================================================

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


-- ============================================================================
-- KPI 5: Análise ABC de Produtos
-- ============================================================================
-- Objetivo: Classificar produtos por importância na receita (Curva 80-20)
-- Métrica: Percentual acumulado de receita
-- Classificação: A (até 80%), B (81-95%), C (acima de 95%)
-- ============================================================================

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


-- ============================================================================
-- KPI 6: Concentração Geográfica de Vendas
-- ============================================================================
-- Objetivo: Identificar territórios com maior performance
-- Métrica: Receita total, participação % e ticket médio por região
-- Ordenação: Por receita total decrescente
-- ============================================================================

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


-- ============================================================================
-- KPI 7: Eficiência Operacional por Categoria
-- ============================================================================
-- Objetivo: Avaliar rentabilidade e giro por categoria de produto
-- Métrica: Margem de lucro %, receita média e giro por produto
-- Análise: Compara eficiência entre categorias
-- ============================================================================

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


-- ============================================================================
-- KPI 8: Customer Lifetime Value (CLV) por Segmento
-- ============================================================================
-- Objetivo: Calcular valor médio de cliente por segmento
-- Métrica: Receita e lucro médio por cliente, pedidos por cliente
-- Classificação: VIP, Premium, Regular, Básico (baseado em receita/cliente)
-- ============================================================================

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


-- ============================================================================
-- KPI 9: Análise de Sazonalidade
-- ============================================================================
-- Objetivo: Identificar padrões de venda por trimestre e dia da semana
-- Métrica: Receita, margem e ticket médio por período
-- Análise: Compara vendas em dias úteis vs fim de semana
-- ============================================================================

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


-- ============================================================================
-- KPI 10: Top 10 Produtos por Receita (Resumo Executivo)
-- ============================================================================
-- Objetivo: Identificar produtos mais importantes para o negócio
-- Métrica: Receita total, lucro total e margem média
-- Ordenação: Por receita total decrescente
-- ============================================================================

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


-- ============================================================================
-- FIM DAS CONSULTAS KPI
-- ============================================================================
-- Total de KPIs: 10
-- Conexão: PostgreSQL DW (localhost:5433)
-- Database: dw_adventureworks
-- Schema: dw
-- ============================================================================
