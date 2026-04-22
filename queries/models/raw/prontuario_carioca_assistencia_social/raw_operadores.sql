-- Camada Raw: Operadores (contas de login) do sistema
with source as (
    select 
        seqlogin as id_login,
        nompess as nome_operador,
        dsclogin as login
    from {{ source('brutos_acolherio_staging', 'gh_contas') }}
)
select * from source
