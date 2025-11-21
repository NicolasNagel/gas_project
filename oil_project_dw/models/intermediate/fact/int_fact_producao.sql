{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key=['sk_prod'],
        on_schema_change='append_new_columns',
        tags=['intermediate', 'fact']
    )
}}

with stg_producao as (
    select *
     from {{ ref("stg_producao") }}

    {% if is_incremental() %}
    -- Processar apenas dados novos em modo incremental
    where producao_data > (select max( dt_producao )) from {{ this }}
    {% endif %}
),
     int_dim_pocos as (
    select *
     from {{ ref("int_dim_pocos") }}
),
     int_fact_producao as (
    select
         -- Surrogate Keys
           {{ dbt_utils.generate_surrogate_key(['prod.producao_cod']) }}                    as sk_prod
         
         -- Foreign Keys
         , {{ dbt_utils.generate_surrogate_key(['poco.nk_poco_cod']) }}                     as fk_poco

         -- Natural Keys
         , prod.producao_cod                                                                as nk_cod_prod
         , prod.producao_id                                                                 as nk_prod_raw
         , prod.poco_cod                                                                    as nk_poco_cod

         -- Dimensão Temporal
         , prod.producao_data                                                               as dt_producao
         , extract( year from prod.producao_data )                                          as ano_producao
         , extract( month from prod.producao_data )                                         as mes_producao
         , extract( day from prod.producao_data )                                           as dia_producao
         , to_char( prod.producao_data, 'YYYY-MM' )                                         as ano_mes_producao
         , extract( quarter from prod.producao_data )                                       as trimestre_producao

         -- Métricas de Produção
         , prod.producao_petroleo_barris_dia                                                as prod_vlr_barris
         , prod.producao_agua_m3                                                            as prod_vlr_agua_m3

         -- Métricas Calculadas
         , prod.producao_percentual_agua                                                    as prod_pct_agua
         , prod.producao_disponibilidade_percentual                                         as prod_pct_disponibilidade

         -- Operação
         , prod.producao_tempo_horas_operacao                                               as prod_qtd_horas_operacao
         , prod.producao_pressao_bar                                                        as prod_vlr_pressao_bar
         , prod.producao_temperatura_celsius                                                as prod_vlr_temperatura_celsius

         -- Classificações
         , prod.producao_classificacao_operacao                                             as prod_operacao_class_dsc
         , prod.producao_classificacao_pressao                                              as prod_pressao_class_dsc

         -- Flags de Qualidade
         , case when prod.producao_percentual_agua > 40            then true else false end as flg_alto_teor_agua
         , case when prod.producao_disponibilidade_percentual < 50 then true else false end as flg_baixa_disponibilidade
         , case when prod.producao_pressao_bar < 200               then true else false end as flg_pressao_baixa
         , case when prod.producao_temperatura_celsius > 100       then true else false end as flg_alta_temperatura

         -- Atributos do Poço
         , poco.poco_nm
         , poco.poco_tipo_desc
         , poco.flg_maritimo
         , poco.poco_regiao_desc
         , poco.flg_pre_sal
         , poco.operadora_nm

         -- Metadados
         , prod.data_atualizacao                                                            as dt_carga_staging
         , current_timestamp                                                                as dt_carga_intermediate

     from stg_producao      prod
    left join int_dim_pocos poco on {{ dbt_utils.generate_surrogate_key(['prod.poco_cod']) }} = poco.sk_poco
)

select *
 from int_fact_producao