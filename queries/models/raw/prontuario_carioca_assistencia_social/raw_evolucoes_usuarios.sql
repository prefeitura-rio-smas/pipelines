-- Camada Raw: Evoluções do módulo Paciente
with source as (
    select
        seqpac as id_paciente,
        sequs as id_unidade,
        seqlogin as id_login,
        dtevol as data_evolucao,
        dscevol as descricao_evolucao,
        indtpevol as tipo_evolucao
    from {{ source('brutos_acolherio_staging', 'gh_evolupac') }}
)
select * from source
