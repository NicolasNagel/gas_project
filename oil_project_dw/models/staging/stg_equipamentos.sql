{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging']
    )
}}

with source as (
    select *
     from {{ source('raw', 'raw_equipamentos') }}
),
    stg_equipamentos as (
    select
           -- IDs e Códigos
             id                                                        as equipamento_id
           , cod_equipamento                                           as equipamento_cod
           , cod_poco                                                  as poco_codigo -- FK para stg_pocos

           -- Informações do Equipamento
           , tipo_equipamento                                          as equipamento_tipo
           , marca                                                     as equipamento_marca
           , modelo                                                    as equipamento_modelo
           
           -- Ciclo de Vida
           , data_instalacao                                           as equipamento_data_instalacao
           , vida_util_anos                                            as equipamento_vida_util_anos

           -- Manutenção e Performance
           , ultimo_teste                                              as equipamento_ultimo_teste
           , eficiencia_operacional                                    as equipamento_eficiencia_percentual

           -- Campos Calculados
           , data_instalacao + ( vida_util_anos || 'years' )::interval as equipamento_data_fim_vida_util
           , current_date - ultimo_teste                               as equipamento_dias_desde_ultimo_teste

           -- Metadados de Auditoria
           , data_insercao                                             as data_insercao
           , current_timestamp                                         as data_carga

     from source
)

select *
 from stg_equipamentos