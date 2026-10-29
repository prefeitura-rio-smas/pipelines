-- Limites inclusivos: períodos que compartilham um dia se sobrepõem.
with catalogo as (
    select
        *,
        row_number() over (order by dominio, codigo, vigente_desde, vigente_ate, rotulo) as linha
    from {{ ref('acolherio_dominios') }}
)

select
    a.dominio,
    a.codigo,
    a.linha
from catalogo as a
inner join catalogo as b
    on
        a.dominio = b.dominio
        and a.codigo = b.codigo
        and a.linha < b.linha
        and (a.vigente_desde is null or b.vigente_ate is null or a.vigente_desde <= b.vigente_ate)
        and (b.vigente_desde is null or a.vigente_ate is null or a.vigente_ate >= b.vigente_desde)
union all
select
    dominio,
    codigo,
    linha
from catalogo
where vigente_desde > vigente_ate
