-- ============================================================
-- SCHEMA DIMENSIONAL PARA DATA WAREHOUSE - ADVENTUREWORKS
-- Modelo Estrela para Análise de Vendas e Rentabilidade
-- Author: Sistema de Análise de Dados
-- Date: 2025-11-23
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ============================================================
-- DIMENSÃO: TEMPO
-- Calendário completo para análises temporais
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    sk_tempo            INT PRIMARY KEY,
    data_completa       DATE NOT NULL UNIQUE,
    ano                 SMALLINT NOT NULL,
    trimestre           SMALLINT NOT NULL,
    mes                 SMALLINT NOT NULL,
    dia                 SMALLINT NOT NULL,
    semana_ano          SMALLINT,
    dia_semana          SMALLINT,
    nome_mes            VARCHAR(20),
    nome_dia_semana     VARCHAR(20),
    eh_fim_semana       BOOLEAN DEFAULT FALSE,
    eh_feriado          BOOLEAN DEFAULT FALSE
);

-- ============================================================
-- DIMENSÃO: CLIENTE
-- Informações demográficas e de localização dos clientes
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_cliente (
    sk_cliente          SERIAL PRIMARY KEY,
    nk_cliente          INT NOT NULL UNIQUE,
    nome_cliente        VARCHAR(150),
    sobrenome           VARCHAR(100),
    nome_completo       VARCHAR(250),
    tipo_cliente        VARCHAR(30),
    segmento            VARCHAR(50),
    data_cadastro       DATE,
    ultima_atualizacao  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- DIMENSÃO: PRODUTO
-- Hierarquia completa de produtos (Categoria > Subcategoria > Produto)
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_produto (
    sk_produto          SERIAL PRIMARY KEY,
    nk_produto          INT NOT NULL UNIQUE,
    nome_produto        VARCHAR(150),
    codigo_produto      VARCHAR(50),
    categoria_produto   VARCHAR(100),
    subcategoria_produto VARCHAR(100),
    linha_produto       VARCHAR(50),
    cor_produto         VARCHAR(30),
    tamanho             VARCHAR(20),
    peso                NUMERIC(10,2),
    custo_unitario      NUMERIC(15,4),
    preco_lista         NUMERIC(15,4),
    margem_percentual   NUMERIC(5,2),
    data_inicio_vigencia DATE,
    data_fim_vigencia   DATE,
    status_ativo        BOOLEAN DEFAULT TRUE
);

-- ============================================================
-- DIMENSÃO: REGIÃO GEOGRÁFICA
-- Hierarquia geográfica para análises territoriais
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_regiao (
    sk_regiao           SERIAL PRIMARY KEY,
    nk_regiao           INT NOT NULL UNIQUE,
    nome_territorio     VARCHAR(100),
    codigo_pais         VARCHAR(10),
    nome_pais           VARCHAR(100),
    continente          VARCHAR(50),
    grupo_regional      VARCHAR(100)
);

-- ============================================================
-- DIMENSÃO: VENDEDOR
-- Informações sobre força de vendas
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_vendedor (
    sk_vendedor         SERIAL PRIMARY KEY,
    nk_vendedor         INT NOT NULL UNIQUE,
    nome_vendedor       VARCHAR(150),
    codigo_vendedor     VARCHAR(50),
    sk_regiao           INT REFERENCES dw.dim_regiao(sk_regiao),
    data_admissao       DATE,
    meta_anual          NUMERIC(18,2),
    comissao_percentual NUMERIC(5,2),
    status_ativo        BOOLEAN DEFAULT TRUE
);

-- ============================================================
-- DIMENSÃO: OFERTA ESPECIAL
-- Promoções e descontos aplicados
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_oferta (
    sk_oferta           SERIAL PRIMARY KEY,
    nk_oferta           INT NOT NULL UNIQUE,
    descricao_oferta    VARCHAR(200),
    tipo_oferta         VARCHAR(50),
    percentual_desconto NUMERIC(5,2),
    data_inicio         DATE,
    data_fim            DATE,
    quantidade_minima   INT,
    quantidade_maxima   INT
);

-- ============================================================
-- FATO: VENDAS
-- Fato principal contendo todas as transações de vendas
-- Granularidade: Linha de pedido (SalesOrderDetailID)
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.fato_vendas (
    sk_venda            BIGSERIAL PRIMARY KEY,
    sk_tempo            INT NOT NULL REFERENCES dw.dim_tempo(sk_tempo),
    sk_cliente          INT NOT NULL REFERENCES dw.dim_cliente(sk_cliente),
    sk_produto          INT NOT NULL REFERENCES dw.dim_produto(sk_produto),
    sk_regiao           INT REFERENCES dw.dim_regiao(sk_regiao),
    sk_vendedor         INT REFERENCES dw.dim_vendedor(sk_vendedor),
    sk_oferta           INT REFERENCES dw.dim_oferta(sk_oferta),
    
    -- Chaves de negócio
    numero_pedido       INT NOT NULL,
    numero_linha        INT NOT NULL,
    
    -- Métricas aditivas
    quantidade_vendida  INT NOT NULL DEFAULT 0,
    valor_unitario      NUMERIC(15,4) NOT NULL DEFAULT 0,
    valor_bruto         NUMERIC(18,4) NOT NULL DEFAULT 0,
    valor_desconto      NUMERIC(18,4) NOT NULL DEFAULT 0,
    valor_liquido       NUMERIC(18,4) NOT NULL DEFAULT 0,
    custo_total         NUMERIC(18,4) NOT NULL DEFAULT 0,
    lucro_bruto         NUMERIC(18,4) NOT NULL DEFAULT 0,
    
    -- Métricas derivadas
    percentual_desconto NUMERIC(5,2),
    margem_contribuicao NUMERIC(5,2),
    
    -- Controle
    data_carga          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uk_pedido_linha UNIQUE (numero_pedido, numero_linha)
);

-- ============================================================
-- ÍNDICES PARA OTIMIZAÇÃO DE CONSULTAS
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_fato_vendas_tempo 
    ON dw.fato_vendas(sk_tempo);
    
CREATE INDEX IF NOT EXISTS idx_fato_vendas_cliente 
    ON dw.fato_vendas(sk_cliente);
    
CREATE INDEX IF NOT EXISTS idx_fato_vendas_produto 
    ON dw.fato_vendas(sk_produto);
    
CREATE INDEX IF NOT EXISTS idx_fato_vendas_regiao 
    ON dw.fato_vendas(sk_regiao);
    
CREATE INDEX IF NOT EXISTS idx_fato_vendas_vendedor 
    ON dw.fato_vendas(sk_vendedor);
    
CREATE INDEX IF NOT EXISTS idx_fato_vendas_pedido 
    ON dw.fato_vendas(numero_pedido);

-- Índice composto para análises temporais por região
CREATE INDEX IF NOT EXISTS idx_fato_vendas_tempo_regiao 
    ON dw.fato_vendas(sk_tempo, sk_regiao);

-- Índice composto para análises de produto por categoria
CREATE INDEX IF NOT EXISTS idx_produto_categoria 
    ON dw.dim_produto(categoria_produto, subcategoria_produto);
