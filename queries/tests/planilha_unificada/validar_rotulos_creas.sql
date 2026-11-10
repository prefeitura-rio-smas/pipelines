{{ config(tags=['planilha_unificada']) }}
-- Um rótulo novo precisa ser associado a uma coluna antes de ser publicado.
select distinct
    h.titulo_formulario,
    c.label
from {{ ref('dim_creas') }} as p, unnest(p.historico) as h, unnest(h.campos) as c
left join {{ ref('creas_campos') }} as mapa
    on
        h.titulo_formulario = mapa.titulo
        and lower(trim(c.label)) = lower(trim(mapa.rotulo))
where mapa.coluna is null
