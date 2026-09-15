-- Camada Raw: Evoluções administrativas dos pacientes
with source as (
    select
        seqpac as id_paciente,
        seqevopac as id_evolucao,
        sequs as id_unidade,
        seqlogin as id_login,
        dtevopac as data_evolucao,
        dscevopac as descricao_evolucao,
        indtpevopac as tipo_evolucao,
        dtcancpac as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_evoluadm') }}
)

select * from source
-- Dedupe: a fonte pode conter linhas 100% idênticas para o mesmo id_evolucao
-- (ex.: seqevopac 98137 duplicado), o que quebrava o teste unique em fct_evolucoes.
qualify row_number() over (partition by id_evolucao) = 1
