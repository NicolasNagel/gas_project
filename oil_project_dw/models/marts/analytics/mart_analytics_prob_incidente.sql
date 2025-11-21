{{
    config(
        materialized='table',
        schema='mart_analytics',
        tags=['mart_analytics', 'feature', 'ml'],
        unique_key='sk_poco'
    )
}}

with inc as (
    select *
     from {{ ref('mart_core_fact_incidentes') }}
),
     agg as (
    select 
         -- Chaves
           fk_poco as sk_poco
         
         -- Frequência
         , count( * )                                                                             as qtd_incidentes
         , avg( incid_vlr_custo )                                                                 as custo_medio
         , avg( incid_qtd_horas_parada )                                                          as media_horas_parada

         -- Probabilidade de Severidade Crítica
         , avg( cast( flg_severidade_alta as int ) )                                              as prob_severidade_alta

         -- Índice de Risco Combinado
         , avg( cast( flg_severidade_alta as int ) ) * avg( cast(flg_prioridade_critica as int) ) as indice_risco

     from inc
    group by fk_poco
)

select *
 from agg