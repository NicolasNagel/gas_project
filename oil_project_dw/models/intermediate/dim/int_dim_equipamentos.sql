{{ 
    config(
        materialized='table',
        schema='intermediate',
        unique_key='sk_equip',
        tags=['intermediate', 'dimension']
    )
}}

with stg_equipamentos as (
    select *
     from {{ ref("stg_equipamentos") }}
),
     stg_pocos as (
    select *
     from {{ ref("stg_pocos") }}
),
     int_dim_pocos as (
    select *
     from {{ ref("int_dim_pocos") }}
),
     int_dim_equipamentos as (
    select
         -- Surrogate Key
           {{ dbt_utils.generate_surrogate_key(['e.equipamento_cod']) }}                                                                 as sk_equip
         
         -- Foreign Key para Poços
         , {{ dbt_utils.generate_surrogate_key(['e.poco_codigo']) }}                                                                     as fk_poco

         -- Natural Keys
         , e.equipamento_cod                                                                                                             as nk_equip_cod
         , e.equipamento_id                                                                                                              as nk_equip_raw
         , e.poco_codigo                                                                                                                 as nk_poco_cod

         -- Atributos do Equipamento
         , e.equipamento_tipo                                                                                                            as equip_tp_desc
         , e.equipamento_marca                                                                                                           as equip_marca_nm
         , e.equipamento_modelo                                                                                                          as equip_modelo_nm

         -- Ciclo de Vida
         , e.equipamento_data_instalacao                                                                                                 as dt_instalacao
         , e.equipamento_vida_util_anos                                                                                                  as equip_vida_util_anos
         , e.equipamento_data_fim_vida_util                                                                                              as dt_fim_vida_util

         -- Flags de Ciclo de Vida
         , case when e.equipamento_data_fim_vida_util < current_date then true else false end                                            as flg_vida_util_expirada
         , case when e.equipamento_data_fim_vida_util between current_date and current_date + interval '1 year' then true else false end as flg_proximo_fim_vida

         -- Manutenção
         , e.equipamento_ultimo_teste                                                                                                    as dt_ultimo_teste
         , e.equipamento_dias_desde_ultimo_teste                                                                                         as qtd_dias_ultimo_teste

         -- Flags de Manutenção
         , case when e.equipamento_dias_desde_ultimo_teste > interval '180 days' then true else false end                                as flg_teste_atrasado
         , case when e.equipamento_dias_desde_ultimo_teste <= interval '30 days' then true else false end                                as flg_teste_recente

         -- Performance
         , e.equipamento_eficiencia_percentual                                                                                           as equip_eficiencia_opr

         -- Classificação de Eficiência
         , case
                when e.equipamento_eficiencia_percentual >= 0.9 then 'Excelente'
                when e.equipamento_eficiencia_percentual >= 0.8 then 'Boa'
                when e.equipamento_eficiencia_percentual >= 0.7 then 'Regular'
                                                                 else 'Crítica'
           end                                                                                                                           as equip_eficiencia_class_dsc
         , case when e.equipamento_eficiencia_percentual < 0.7 then true else false end                                                  as flg_eficiencia_critica

         -- Atributos de Poço (Denormalizado para BI)
         , p.poco_nm
         , p.poco_tipo_desc
         , p.flg_maritimo
         , p.poco_regiao_desc
         , p.flg_pre_sal
         , p.operadora_nm

         -- Metadados
         , e.data_carga                                                                                                                  as dt_carga_staging
         , current_timestamp                                                                                                             as dt_carga_intermediate

     from stg_equipamentos  e
    left join int_dim_pocos p on {{ dbt_utils.generate_surrogate_key(['e.poco_codigo']) }} = p.sk_poco
)

select *
 from int_dim_equipamentos