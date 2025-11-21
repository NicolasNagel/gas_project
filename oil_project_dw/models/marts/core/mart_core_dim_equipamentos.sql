{{ 
    config(
        materialized='table',
        schema='mart_core',
        tags=['mart_core', 'dimension']
    )
}}

with int_dim_equipamentos as (
    select *
     from {{ ref('int_dim_equipamentos') }}
)
    select
           -- Chaves
             sk_equip
           , nk_equip_cod
           , nk_equip_raw

           -- Relacionamentos
           , fk_poco
           , nk_poco_cod

           -- Descrições / Tipos / Atributos
           , equip_tp_desc
           , equip_marca_nm
           , equip_modelo_nm
           , equip_vida_util_anos
           , equip_eficiencia_opr
           , equip_eficiencia_class_dsc

           -- Flags Operacionais
           , flg_vida_util_expirada
           , flg_proximo_fim_vida
           , flg_teste_atrasado
           , flg_teste_recente
           , flg_eficiencia_critica

           -- Datas Operacionais
           , dt_instalacao
           , dt_fim_vida_util
           , dt_ultimo_teste

           -- Atributos de Poços (Denormalizados para BI)
           , poco_nm
           , poco_tipo_desc
           , flg_maritimo
           , flg_pre_sal
           , poco_regiao_desc
           , operadora_nm

           -- Metadados
           , dt_carga_staging
           , dt_carga_intermediate
           , current_timestamp     as dt_carga_mart
           
     from int_dim_equipamentos