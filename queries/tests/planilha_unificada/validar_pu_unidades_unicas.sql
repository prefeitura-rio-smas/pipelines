{{ config(tags = ['planilha_unificada']) }}

select id_unidade
from {{ pu_ref('dim_unidades') }}
group by id_unidade
having id_unidade is null or count(*) > 1
