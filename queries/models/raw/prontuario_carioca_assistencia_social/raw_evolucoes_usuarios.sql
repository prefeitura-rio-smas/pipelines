-- Camada Raw: Evoluções do módulo Usuário
with source as (
    select
        seqpac as id_usuario,
        sequs as id_unidade,
        seqprof as id_profissional,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        codabapac as codigo_abrangencia
    from {{ source('prontuario_carioca_assistencia_social', 'gh_evolupac') }}
)
select * from source
