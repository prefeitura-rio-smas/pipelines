-- Camada Raw: Tipos de atendimento
with source as (
    select
        seqtpatend as id_tipo_atendimento,
        descatend as descricao,
        codabapront as codigo_aba_prontuario
    from {{ source('brutos_acolherio_staging', 'gh_tpatendimentos') }}
)
select * from source
