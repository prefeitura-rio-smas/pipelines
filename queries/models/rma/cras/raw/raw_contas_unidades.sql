-- Tabela responsável por retornar os operadores e suas respectivas unidades
-- Existem contas com mais de uma unidade.
-- Contas testes, admin e suporte não foram retiradas

with contas_mais_de_uma_unidade as (
select 
  sequs,
  seqlogin,
  row_number() over (
   partition by seqlogin 
  ) as n_number
from {{ source('cras_rma_prod', 'gh_contas_us')}}
order by datacesso asc
)

select * from contas_mais_de_uma_unidade 
where n_number = 1
