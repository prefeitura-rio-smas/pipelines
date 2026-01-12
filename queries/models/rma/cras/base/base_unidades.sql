-- Tabela responsável por retornar todas as unidades cadastradas no simtema;
-- Não retorna unidade testes;
with sem_unidades_teste as (
select 
    sequs,
    dscus as unidade
from {{ source('cras_rma_prod', 'gh_us')}}
where not regexp_contains(dscus, r'(?i)teste')
)

select * from sem_unidades_teste 