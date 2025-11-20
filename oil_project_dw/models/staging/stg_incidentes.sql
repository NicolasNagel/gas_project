{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select *
     from {{ source('raw', 'raw_incidentes') }}
),
     stg_incidentes as (
    select
         -- IDs e Códigos
           id                                                                                  as incidente_id
         , cod_incidente                                                                       as incidente_cod
         , cod_poco                                                                            as poco_cod -- FK para stg_pocos
         , cod_equipamento                                                                     as equipamento_cod -- FK para stg_equipamentos
         
         -- Data e Tipo
         , data_incidente                                                                      as incidente_data
         , tipo_incidente                                                                      as incidente_tipo
         , severidade                                                                          as incidente_severidade

         -- Impacto
         , tempo_parada_horas                                                                  as incidente_tempo_parada_horas
         , custo_estimado_reais                                                                as incidente_custo_estimado_reais
         
         -- Status
         , status_resolucao                                                                    as incidente_status_resolucao

         -- Campos Calculados
         , round( tempo_parada_horas::numeric / 24, 2 )                                        as incidente_tempo_parada_dias
         , case
                when tempo_parada_horas <= 4  then 'Impacto Baixo'
                when tempo_parada_horas  > 4
                 and tempo_parada_horas <= 24 then 'Impacto Médio'
                when tempo_parada_horas  > 24 then 'Impacto Alto'
           end                                                                                 as incidente_classificacao_impacto
         , case 
                when custo_estimado_reais < 10000                  then 'Custo Baixo'
                when custo_estimado_reais between 10000 and 100000 then 'Custo Médio'
                when custo_estimado_reais > 100000                 then 'Custo Alto'
           end                                                                                 as incidente_classificacao_custo
         , case
                when severidade = 'Alta'  and status_resolucao = 'Pendente'     then 'Crítica' 
                when severidade = 'Alta'  and status_resolucao = 'Em Andamento' then 'Alta'
                when severidade = 'Média' and status_resolucao = 'Pendente'     then 'Alta'
                when severidade = 'Média' and status_resolucao = 'Em Andamento' then 'Média'
                                                                                else 'Baixa' 
           end                                                                                 as incidente_prioridade

         -- Metadados de Auditoria
         , data_insercao                                                                       as data_insercao
         , current_timestamp                                                                   as data_carga

     from source
)

select *
 from stg_incidentes