{{
    config(
        materialized='table',
        schema='mart_core',
        tags=['mart_core', 'fact']
    )
}}

with int_fact_producao as (
    select *
     from {{ ref("int_fact_producao") }}
)
select 
       -- Chaves
        sk_prod
      , nk_cod_prod
      , nk_prod_raw

      -- Chaves de Relacionamento
      , fk_poco
      , nk_poco_cod

      -- Dimensão Temporal
      , dt_producao
      , ano_producao
      , mes_producao
      , dia_producao
      , ano_mes_producao
      , trimestre_producao

      -- Métricas de Produção
      , prod_vlr_barris
      , prod_vlr_agua_m3
      , prod_pct_agua
      , prod_pct_disponibilidade

      -- Operação (Métricas)
      , prod_qtd_horas_operacao
      , prod_vlr_pressao_bar
      , prod_vlr_temperatura_celsius

      -- Classificações / Tipos
      , prod_operacao_class_dsc
      , prod_pressao_class_dsc

      -- Flags Operacionais
      , flg_alto_teor_agua
      , flg_baixa_disponibilidade
      , flg_pressao_baixa
      , flg_alta_temperatura

      -- Metadados
      , dt_carga_staging
      , dt_carga_intermediate
      , current_timestamp    as dt_carga_mart

 from int_fact_producao