{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select *
     from {{ source('raw', 'raw_pocos') }}
),
    stg_pocos as (
    select 
                -- IDs e Códigos
                id                  as poco_id
              , codigo_poco         as poco_cod
              
              -- Informações Básicas
              , nome_poco           as poco_nome
              , tipo_poco           as poco_tipo
              , localizacao         as poco_localizacao
              , camada              as poco_camada

              -- Métricas Físicas
              , profundidade_metros as poco_profundidade_metros
              
              -- Status
              , status_operacional  as poco_status
              
              -- Operação
              , operadora           as poco_operadora

              -- Datas
              , data_perfuracao     as poco_data_perfuracao

              -- Metadados de Auditoria
              , data_insercao       as data_insercao
              , current_timestamp   as data_carga

     from source
)

select *
 from stg_pocos 