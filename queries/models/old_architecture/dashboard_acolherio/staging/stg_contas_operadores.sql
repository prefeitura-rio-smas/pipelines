-- Tabela que retorna total de pessoas que possuem contas no acolherio. 

with operadores_unicos as (
    select 
        seqlogin,
        nompess as operador,
        dsclogin as login_operador
    from {{ source('source_dashboard_acolherio', 'gh_contas') }}
    where not regexp_contains(nompess, '(?i)teste|suporte|admin')
)

select * from operadores_unicos