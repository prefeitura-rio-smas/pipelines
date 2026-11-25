-- Mart de Abordagem Social (situação de rua): 1 linha por abordagem (globalid).
-- Camada de apresentação: consome a int_abordagem e mantém o contrato de 154
-- colunas. Destino: `rj-smas.abordagem.abordagem` em prod; dev/CI via +schema.
-- A tabela antiga em `dashboard_arcgis` deixa de ser atualizada; o repoint do
-- Looker é uma etapa operacional posterior.

{{ config(
    materialized = 'table',
    alias = 'abordagem',
    tags = ['abordagem']
) }}

select *  -- noqa: AM04
from {{ ref('int_abordagem') }}
