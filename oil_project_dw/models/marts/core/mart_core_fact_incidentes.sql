{{ 
    config(
        materialized='table',
        schema='mart_core',
        tags=['mart_core', 'fact']
    )
}}

with int_fact_incidentes as (
    select *
     from {{ ref('int_fact_incidentes') }}
)
select 
       -- Chaves Primárias
        sk_incidente
      , nk_incid_cod
      , nk_poco_cod
      , nk_equip_cod
       
       -- Chaves de Relacionamento
     , fk_poco
     , fk_equipamento

     -- Dimensão Temporal
     , dt_incidente
     , ano_incidente
     , mes_incidente
     , ano_mes_incidente
     , trimestre_incidente

     -- Descrições / Tipos
     , incid_tp_dsc
     , incid_serveridade_dsc
     , incid_prioridade_dsc
     , incid_impacto_class_dsc
     , incid_custo_class_desc
     , incid_status_desc

     -- Impacto (Métricas)
     , incid_qtd_horas_parada
     , incid_qtd_dias_parada
     , incid_vlr_custo

     -- Flags
     , flg_severidade_alta
     , flg_prioridade_critica
     , flg_parada_longa
     , flg_custo_alto
     , flg_resolvido
     , flg_pendente

     -- Metadados
     , dt_carga_staging
     , dt_carga_intermediate
     , current_timestamp     as dt_carga_mart
     
 from int_fact_incidentes