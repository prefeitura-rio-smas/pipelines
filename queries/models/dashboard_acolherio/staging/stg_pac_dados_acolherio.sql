-- Tabela responsável por retornar parte dos dados relacionados ao usuário
-- Contém usuários testes
-- Não contém usuários repetidos

{{ config(materialized='table')}}

with dados_pac as (
    select
        seqpac,
        indcadunico as flag_cadunico,
        dsctomdecproces as numero_processo_decisao_apoiada,
        dsctomdecnome as nome_apoiador,
        indsmentcompr as saude_mental_comprometida,
        indmotivacol as motivo_acolhimento,
        indvioldir as violacao_direito,
        valpontos as pontuacao,
        indorientsex as orientacao_sexual,
        indtipovinc  as vinculo_trabalhista
    from  {{ source('source_dashboard_acolherio', 'gh_pac_dados') }}
)

select * from dados_pac