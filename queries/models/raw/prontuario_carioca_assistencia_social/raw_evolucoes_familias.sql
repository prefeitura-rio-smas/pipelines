-- Camada Raw: Evoluções do módulo Família
with source as (
    select
        seqfamil as id_familia,
        sequs as id_unidade,
        seqprof as id_profissional,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        codabapac as modulo_prontuario,
        seqpac as id_paciente
    from {{ source('brutos_acolherio_staging', 'gh_evolufamil') }}
)
select * from source
