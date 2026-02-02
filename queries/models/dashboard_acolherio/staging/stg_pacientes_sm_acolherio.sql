-- Tabela responsável por retornar parte dos dados relacionados ao usuário
-- Contém usuários testes
-- Não contém usuários repetidos

with dados_pacientes_sm as (
    select
        seqpacsm as seqpac,
        codorigem,
        indtrab as flag_trabalho,
        nmprofi as profissao,
        indfreqescol as flag_frequencia_escola,
        indserie as serie_escola,
        indescolari as escolaridade,
        indrecbenef as flag_recebe_beneficio,
        indqualbenef as tipo_beneficio,
        indcuratela as flag_curatela, 
        indtipcuratela as tipo_curatela,
        indmoradi as flag_situacao_rua,
        indpresdefi as flag_deficiencia,
        indtpdefi as tipo_deficiencia
    from  {{ source('source_dashboard_acolherio', 'gh_pacientes_sm') }}
)

select * from dados_pacientes_sm