-- Retorna todos os profissionais e suas respectivas unidades. No caso de profissionais com mais de uma unidade, será retornado a unidade maisa recente logada, caso a coluna datacesso não seja vazia.
with contas_mais_de_uma_unidade as (
select 
  sequs,
  seqlogin,
  datacesso,
  row_number() over (
   partition by seqlogin 
  ) as n_number
from {{ source('cras_rma_prod', 'gh_contas_us')}}
order by datacesso asc
),


nome_conta as (
select
    a.seqlogin,
    a.sequs,
    a.datacesso,
    b.nompess
from contas_mais_de_uma_unidade a
inner join {{ source('cras_rma_prod', 'gh_contas') }} b on a.seqlogin = b.seqlogin
where n_number = 1
)

select
    *
from nome_conta
where not regexp_contains(nompess, r'(?i)teste|admin|suporte')