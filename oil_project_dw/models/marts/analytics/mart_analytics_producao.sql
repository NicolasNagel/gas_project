{{
    config(
        materialized='table',
        schema='mart_analytics',
        tags=['analytics', 'features', 'ml'],
        unique_key='sk_poco_mes'
    )
}}

with prod as (
    select *
     from {{ ref('mart_core_fact_producao') }}
),
     agg as (
    select 
         -- Chaves
           {{ dbt_utils.generate_surrogate_key(['fk_poco', 'ano_mes_producao']) }} as sk_poco_mes
         , fk_poco
         
         -- Dimensões Temporais
         , ano_producao
         , mes_producao
         , ano_mes_producao

         -- Features Quantitativas
         , avg( prod_vlr_barris )                            as media_barris
         , avg( prod_vlr_agua_m3 )                           as media_agua
         , avg( prod_pct_agua )                              as media_pct_agua
         , avg( prod_pct_disponibilidade )                   as media_disponibilidade
         , avg( prod_qtd_horas_operacao )                    as media_horas_operacao
         , avg( prod_vlr_pressao_bar )                       as media_pressao
         , avg( prod_vlr_temperatura_celsius )               as media_temperatura

         -- Taxas / Tendências
         , stddev( prod_vlr_barris )                         as variancia_barris
         , stddev( prod_pct_agua )                           as variancia_pct_agua
         , stddev( prod_vlr_pressao_bar )                    as variancia_pressao

         -- Indice Sintético (Para ML e DS)
         , avg( prod_pct_disponibilidade * prod_vlr_barris ) as indice_producao

     from prod
    group by 1
           , 2
           , 3
           , 4
           , 5
)

select *
 from agg