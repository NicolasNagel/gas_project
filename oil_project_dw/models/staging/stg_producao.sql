{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select *
     from {{ source('raw', 'raw_producao') }}
),
     stg_producao as (
    select
         -- IDs e Códigos
           id                                                                                          as producao_id
         , cod_producao                                                                                as producao_cod
         , cod_poco                                                                                    as poco_cod -- FK para stg_pocos

         -- Data de Produção
         , data_producao                                                                               as producao_data
         
         -- Métricas de Produção
         , petroleo_barris_dia                                                                         as producao_petroleo_barris_dia
         , agua_produzida_m3                                                                           as producao_agua_m3

         -- Operação
         , tempo_horas_operacao                                                                        as producao_tempo_horas_operacao
         , pressao_bar                                                                                 as producao_pressao_bar
         , temperatura_celsius                                                                         as producao_temperatura_celsius

         -- Campos Calculados
         , round( petroleo_barris_dia::numeric / nullif( tempo_horas_operacao, 0)::numeric, 2 )        as producao_barris_por_dia
         , round( ( agua_produzida_m3::numeric / nullif( petroleo_barris_dia, 0)::numeric ) * 100, 2 ) as producao_percentual_agua
         , round( ( tempo_horas_operacao::numeric / 24 ) * 100, 2 )                                    as producao_disponibilidade_percentual

         -- Flags de Qualidade
         , case
                when tempo_horas_operacao  < 12 then 'Baixa Operação'
                when tempo_horas_operacao >= 12 
                 and tempo_horas_operacao  < 20 then 'Operação Média'
                when tempo_horas_operacao >= 20 then 'Alta Operação'
           end                                                                                         as producao_classificacao_operacao
         , case
                when pressao_bar < 200               then 'Pressão Baixa'
                when pressao_bar between 200 and 350 then 'Pressão Normal'
                when pressao_bar > 350               then 'Pressão Alta'
           end                                                                                         as producao_classificacao_pressao

         -- Metados de Auditoria
         , data_insercao                                                                               as data_insercao
         , current_timestamp                                                                           as data_atualizacao

     from source
)

select *
 from stg_producao