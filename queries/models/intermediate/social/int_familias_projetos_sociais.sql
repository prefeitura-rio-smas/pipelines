{{ config(materialized="ephemeral") }}

select
    id_familia,
    array_agg((select as struct p.* except (id_familia))) as projetos_sociais
from ({{ registros_projetos_sociais() }}) as p
group by id_familia
