-- Camada Raw: Operadores (contas de login) do sistema
with source as (
    select 
        seqlogin as id_login,
        nompess as nome_operador,
        dscemail as email,
        dsclogin as login,
        datcadastro as data_cadastro,
        datultacess as data_ultimo_acesso
    from {{ source('brutos_acolherio_staging', 'gh_contas') }}
)
select * from source
