{{ config(tags=['planilha_unificada']) }}
select p.usuario.id_usuario
from {{ ref('planilha_unificada') }} as p
where
    {% for bloco in ['atendimentos', 'evolucoes'] %}
        {% if not loop.first %} or {% endif %}
        exists (
            select 1 from unnest(p.{{ bloco }}.registros) as registro
            where registro.tipo_servico is null or registro.tipo_servico not in ('cras', 'creas', 'centro_pop', 'outros')
        )
        or
        p.{{ bloco }}.quantidade != array_length(p.{{ bloco }}.registros)
        or p.{{ bloco }}.quantidade != (
            {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
                {% if not loop.first %} + {% endif %}p.{{ bloco }}.quantidade_{{ servico }}
            {% endfor %}
        )
        {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
            or p.{{ bloco }}.quantidade_{{ servico }} is distinct from (
                select count(*) from unnest(p.{{ bloco }}.registros) as registro
                where registro.tipo_servico = '{{ servico }}'
            )

        {% endfor %}
    {% endfor %}
