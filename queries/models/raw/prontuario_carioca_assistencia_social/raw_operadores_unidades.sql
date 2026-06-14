-- Camada Raw: Vínculo entre contas de operadores e unidades do sistema
-- Tabela: gh_contas_us (seqlogin → sequs)
with source as (
    select
        seqlogin as id_login,
        sequs as id_unidade
    from {{ source('brutos_acolherio_staging', 'gh_contas_us') }}
)
select * from source
