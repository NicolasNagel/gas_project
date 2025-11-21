{{
    config(
        materialized='table',
        schema='mart_analytics',
        tags=['analytics', 'ml', 'features'],
        unique_key='sk_equip'
    )
}}

with equip as (
    select *
     from {{ ref('mart_core_dim_equipamentos') }}
),
     risco as (
    select 
         -- Chaves
           sk_equip
         , nk_equip_cod
         , equip_marca_nm
         , equip_tp_desc

         --  Feature de Vida Útil
         , equip_vida_util_anos
         , case when flg_vida_util_expirada then 1 else 0 end                     as vida_util_expirada

         -- Eficiência como Feature Numérica
         , equip_eficiencia_opr
         , case when flg_eficiencia_critica then 1 else 0 end                     as eficiencia_critica

         -- Feature Combinada (Proxy de Falha)
         , ( equip_eficiencia_opr * ( 1 - cast(flg_vida_util_expirada as int) ) ) as indice_saude
     
     from equip
)

select *
 from risco