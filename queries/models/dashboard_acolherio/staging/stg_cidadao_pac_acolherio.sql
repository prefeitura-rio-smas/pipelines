-- Tabela responsável por retornar parte dos dados relacionados ao usuário
-- Contém usuários testes
-- Não contém usuários repetidos

{{ config(materialized='table')}}

with dados_cidadao_pac as (
    select
        seqpac,
        dscnomepac as nome_usuario,
        dscnomsoci as nome_social,
        dscnmmae as filiacao_mae,
        datnascim as data_nascimento,
        nacional as nacionalidade,
        condestr as condicao_estrangeira,
        paisorigem as pais_origem,
        dscbairroender as bairro,
        racacor as raca, 
        numcpfpac as cpf,
        indsexo as sexo,
        indgenero as genero,
        nuprontpapel as prontuario,
        datcadast as data_cadastro
    from  {{ source('source_dashboard_acolherio', 'gh_cidadao_pac') }}
)

select * from dados_cidadao_pac