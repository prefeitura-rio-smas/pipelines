
WITH stg_viol_direito as (
select
    nome_usuario,
    seqpac,
    viol_direito
from {{ source('dashboard_acolherio', 'violacao_direito')}}
where not regexp_contains(nome_usuario, r'(?i)teste')
)

select * from stg_viol_direito