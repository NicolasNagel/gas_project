{{ 
    config(
        materialiezed='table',
        schema='intermediate',
        unique_key='sk_poco',
        tags=['intermediate', 'dimension']
    )
}}

with stg_pocos as (
    select *
     from {{ ref('stg_pocos') }}
),
     int_dim_pocos as (
        select 
              -- Surrogate Key
               {{ dbt_utils.generate_surrogate_key(['poco_cod']) }}                          as sk_poco
             
             -- Chaves Naturais/Business Keys
             , poco_cod                                                                      as nk_poco_cod
             , poco_id                                                                       as pk_poco_raw

             -- Atributos Básicos
             , poco_nome                                                                     as poco_nm
             , poco_tipo                                                                     as poco_tipo_id

             -- Tipos Descritivos
             , case
                    when poco_tipo = '1' then 'Marítimo'
                    when poco_tipo = '2' then 'Terrestre'
                                         else 'Desconhecido'
               end                                                                           as poco_tipo_desc
             
             -- Classificação por Tipo (Para facilitar filtros)
             , case when poco_tipo = '1' then true else false end                            as flg_maritimo
             , case when poco_tipo = '2' then true else false end                            as flg_terrestre

             -- Localização
             , poco_localizacao                                                              as poco_localizacao_nm

             -- Classificação da Região
             , case
                    when poco_localizacao like '%Santos%'         then 'Sudeste'
                    when poco_localizacao like '%Campos%'         then 'Sudeste'
                    when poco_localizacao like '%Espírito Santo%' then 'Nordeste'
                    when poco_localizacao like '%Recôncavo%'      then 'Nordeste'
                    when poco_localizacao like '%Portiguar%'      then 'Nordeste'
                                                                  else 'Outro'      
               end                                                                           as poco_regiao_desc
             
             -- Camadas
             , poco_camada                                                                   as poco_camada_desc
             , case when poco_camada = 'Pre-Sal' then true else false end                    as flg_pre_sal
             , case when poco_camada = 'Pos-Sal' then true else false end                    as flg_pos_sal

             -- Métricas Físicas
             , poco_profundidade_metros                                                      as poco_profundidae_mt

             -- Classificação de Profundidade
             , case
                    when poco_profundidade_metros < 1000                then 'Rasa'
                    when poco_profundidade_metros between 1000 and 3000 then 'Média'
                    when poco_profundidade_metros between 3000 and 5000 then 'Profunda'
                    when poco_profundidade_metros > 5000                then 'Ultra-Profunda'
               end                                                                           as poco_profundidade_class_desc
             
             -- Status
             , poco_status                                                                   as poco_st_desc
             , case when poco_status = 'Ativo'      then true else false end                 as flg_ativo
             , case when poco_status = 'Manutenção' then true else false end                 as flg_manutencao
             , case when poco_status = 'Inativo'    then true else false end                 as flg_inativo

             -- Operadora
             , poco_operadora                                                                as operadora_nm

             -- Datas
             , poco_data_perfuracao                                                          as dt_perfuracao
             , extract( year from poco_data_perfuracao )                                     as ano_perfuracao
             , extract( month from poco_data_perfuracao )                                    as mes_perfuracao
             , date_part( 'year', current_date ) - extract( year from poco_data_perfuracao ) as qtd_anos_operacao

             -- Metadados
             , data_insercao                                                                 as dt_criacao_registro
             , data_carga                                                                    as dt_carga_staging
             , current_timestamp                                                             as dt_carga_intermediate

         from stg_pocos
)

select *
 from int_dim_pocos