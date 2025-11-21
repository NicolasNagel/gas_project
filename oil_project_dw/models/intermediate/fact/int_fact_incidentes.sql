{{ 
    config(
        materialized='table',
        schema='intermediate',
        unique_key='sk_incid',
        tags=['intermediate', 'fact']
    )
}}

with stg_incidentes as (
    select *
     from {{ ref("stg_incidentes") }}
),
     int_dim_pocos as (
    select *
     from {{ ref("int_dim_pocos") }}
),
     int_dim_equipamentos as (
    select *
     from {{ ref("int_dim_equipamentos") }}
),
     int_fact_incidents as (
    select
         --Surrogate Key
           {{ dbt_utils.generate_surrogate_key(['i.incidente_cod']) }}                   as sk_incidente
         
         -- Foreign Keys
         , {{ dbt_utils.generate_surrogate_key(['i.poco_cod'] )}}                        as fk_poco
         , {{ dbt_utils.generate_surrogate_key(['i.equipamento_cod']) }}                 as fk_equipamento

         -- Natural Keys
         , i.incidente_cod                                                               as nk_incid_cod
         , i.incidente_id                                                                as nk_incid_raw
         , i.poco_cod                                                                    as nk_poco_cod
         , i.equipamento_cod                                                             as nk_equip_cod

         -- Dimensão Temporal
         , i.incidente_data                                                              as dt_incidente
         , extract( year from i.incidente_data )                                         as ano_incidente
         , extract( month from i.incidente_data )                                        as mes_incidente
         , to_char( i.incidente_data, 'YYYT-MM' )                                        as ano_mes_incidente
         , extract( quarter from i.incidente_data )                                      as trimestre_incidente

         -- Tipo Severidade
         , i.incidente_tipo                                                              as incid_tp_dsc
         , i.incidente_severidade                                                        as incid_serveridade_dsc
         , i.incidente_prioridade                                                        as incid_prioridade_dsc

         -- Flags de Severidade
         , case when i.incidente_severidade = 'Alta'            then true else false end as flg_severidade_alta
         , case when i.incidente_prioridade = 'Crítica'         then true else false end as flg_prioridade_critica

         -- Impacto (Tempo)
         , i.incidente_tempo_parada_horas                                                as incid_qtd_horas_parada
         , i.incidente_tempo_parada_dias                                                 as incid_qtd_dias_parada
         , i.incidente_classificacao_impacto                                             as incid_impacto_class_dsc

         -- Impacto (Custo)
         , i.incidente_custo_estimado_reais                                              as incid_vlr_custo
         , i.incidente_classificacao_custo                                               as incid_custo_class_desc

         -- Flags de Impacto
         , case when i.incidente_tempo_parada_horas > 24        then true else false end as flg_parada_longa
         , case when i.incidente_custo_estimado_reais > 1000000 then true else false end as flg_custo_alto

         -- Status
         , i.incidente_status_resolucao                                                  as incid_status_desc
         , case when i.incidente_status_resolucao = 'Resolvido' then true else false end as flg_resolvido
         , case when i.incidente_status_resolucao = 'Pendente'  then true else false end as flg_pendente

         -- Atributos do Poço (Denormalizados para BI)
         , p.poco_nm
         , p.poco_tipo_desc
         , p.flg_maritimo
         , p.poco_regiao_desc
         , p.operadora_nm

         -- Atributos de Equipamento (Denormalizados para BI)
         , e.equip_tp_desc
         , e.equip_marca_nm
         , e.equip_eficiencia_opr
         , e.flg_eficiencia_critica

         -- Metadados
         , i.data_carga                                                           as dt_carga_staging
         , current_timestamp                                                      as dt_carga_intermediate

     from stg_incidentes           i
    left join int_dim_pocos        p 
           on {{ dbt_utils.generate_surrogate_key(['i.poco_cod']) }}        = p.sk_poco
    left join int_dim_equipamentos e 
           on {{ dbt_utils.generate_surrogate_key(['i.equipamento_cod']) }} = e.sk_equip
)

select *
 from int_fact_incidents