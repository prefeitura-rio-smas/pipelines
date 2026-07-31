-- Camada Raw: Tipos de atendimento
with source as (
    select
        seqtpatend as id_tipo_atendimento,
        descatend as descricao,
        codabapront as codigo_aba_prontuario,
        descatend as tipo_atendimento_descricao
    from {{ source('brutos_acolherio_staging', 'gh_tpatendimentos') }}
)
select * from source
