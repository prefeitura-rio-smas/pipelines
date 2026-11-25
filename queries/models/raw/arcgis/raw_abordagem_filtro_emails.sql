-- Camada Raw: de/para CAS -> e-mail institucional usado no filtro do dashboard.
-- Fonte mantida fora do dbt no dataset `dashboard_arcgis` (projeto dev).
{{ config(materialized = 'view') }}

select
    cas,
    e_mail
from {{ source('dashboard_arcgis_manuais', 'abordagem_filtro_emails') }}
