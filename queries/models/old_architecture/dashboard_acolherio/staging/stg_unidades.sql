-- Tabela que contém os dados de unidades do Acolherio.
-- Tabela 1:1 com os dados brutos.
-- Unidades testes foram retiradas.

with unidades as (
    select
        apus as cas,
        dscus as unidade,
        sequs,
        siguf as uf,
        esfera
    from {{ source('source_dashboard_acolherio', 'gh_us') }}
)

select * from unidades
--where not regexp_contains(unidade, '(?i)teste')