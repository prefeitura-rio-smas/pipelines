-- Camada Raw: Evoluções administrativas dos pacientes
with source as (
    select
        seqpac as id_paciente,
        sequs as id_unidade,
        seqlogin as id_login,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao
    from {{ source('brutos_acolherio_staging', 'gh_evoluadm') }}
)
select * from source
