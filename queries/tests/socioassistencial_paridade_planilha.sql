-- Compara multiconjuntos para detectar tanto divergência de contexto como fanout.
-- Nº depende da apresentação; todos os demais campos do contrato devem sobreviver.
with anterior as (
    select
        to_json_string((select as struct p.* except (numero))) as conteudo,
        count(*) as quantidade
    from {{ ref('mart_planilha_unificada_centro_pop') }} as p
    group by conteudo
),

nova as (
    select
        to_json_string(p) as conteudo,
        count(*) as quantidade
    from {{ ref('mart_socioassistencial_unificada') }} as m,
        unnest(m.planilha_centro_pop) as p
    where m.tipo_registro = 'ATENDIMENTO'
    group by conteudo
)

select farm_fingerprint(coalesce(a.conteudo, n.conteudo)) as hash_contexto
from anterior as a
full outer join nova as n on a.conteudo = n.conteudo
where a.quantidade is distinct from n.quantidade
