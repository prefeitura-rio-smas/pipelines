-- Tabela responsável por retornar parte dos dados relacionados ao usuário
-- Contém usuários testes
-- Não contém usuários repetidos

{{ config(materialized='table')}}

with familia_membros as (
    select
        seqpac,
        seqfamil,
        seqmembro,
        seqfamilnova,
        seqmotivsaida,
        datentrada as data_entrada_membro_familia,
        datsaida as data_saida_membro_familia,
        parentesco_responsavel_familia
    from  {{ source('source_dashboard_acolherio', 'gh_familias_membros') }}
)

select * from familia_membros
