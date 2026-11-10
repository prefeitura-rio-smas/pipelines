{{ config(tags=['prontuario_bi_unidade']) }}
select id_unidade
from {{ pu_ref('raw_unidades') }}
group by id_unidade
having count(distinct to_json_string(struct(cas, email_unidade))) > 1
