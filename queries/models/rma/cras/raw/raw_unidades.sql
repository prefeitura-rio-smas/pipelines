-- Tabela responsável por retornar todas as unidades CRAS cadastradas no simtema;
-- Não retorna unidade testes;
with sem_unidades_teste as (
select 
    sequs,
    dscus as unidade,
    current_datetime() as data_extracao
from {{ source('cras_rma_prod', 'gh_us')}}
where not regexp_contains(dscus, r'(?i)teste')
and regexp_contains(dscus, '(?i)cras')
)

select * from sem_unidades_teste 