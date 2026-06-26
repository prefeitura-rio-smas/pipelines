-- Camada Raw: Filtro de e-mails por unidade (planilha de apoio do dashboard)
with source as (
    select
        UNIDADE_ATENDIMENTO as unidade_atendimento,
        EMAIL as email
    from {{ source('dashboard_acolherio', 'filtro_email_dev') }}
)
select * from source
