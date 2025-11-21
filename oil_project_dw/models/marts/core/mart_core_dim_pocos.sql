{{ 
    config(
        materialized='table',
        schema='mart_core',
        tags=['mart_core', 'dimension']
    )
}}

with int_dim_pocos as (
    select *
     from {{ ref('int_dim_pocos') }}
)
select
     -- Chaves de Relacionamento
       sk_poco
     , nk_poco_cod
     , pk_poco_raw

     -- Identificação
     , poco_nm

     -- Localização e Classificações
     , poco_tipo_desc
     , poco_localizacao_nm
     , poco_regiao_desc
     , poco_camada_desc
     , poco_profundidae_mt
     , poco_profundidade_class_desc
     , poco_st_desc
     , operadora_nm

     -- Flags
     , flg_maritimo
     , flg_terrestre
     , flg_pre_sal
     , flg_pos_sal
     , flg_ativo
     , flg_manutencao
     , flg_inativo

     -- Datas
     , dt_perfuracao
     , ano_perfuracao
     , mes_perfuracao
     , qtd_anos_operacao

     -- Metadados
     , dt_carga_intermediate
     , current_timestamp     as dt_carga_mart
    
     from int_dim_pocos