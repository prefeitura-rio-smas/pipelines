-- Camada Raw: Dados complementares de pacientes (módulo Saúde Mental)
with source as (
    select
        seqpacsm as id_paciente,
        codorigem as codigo_origem,
        indtrab as flag_trabalha,
        nmprofi as profissao,
        indfreqescol as flag_frequenta_escola,
        indserie as serie_escolar,
        indescolari as escolaridade,
        indrecbenef as flag_recebe_beneficio,
        indqualbenef as tipo_beneficio,
        indcuratela as flag_curatela, 
        indtipcuratela as tipo_curatela,
        {{ map_flag_situacao_rua('indmoradi') }} as flag_situacao_rua,
        indpresdefi as flag_deficiencia,
        indtpdefi as tipo_deficiencia
    from  {{ source('brutos_acolherio_staging', 'gh_pacientes_sm') }}
)
select * from source
