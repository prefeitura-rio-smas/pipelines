{{ config(tags=['planilha_unificada']) }}
select p.usuario.id_usuario
from {{ ref('planilha_unificada') }} as p
where
    {% for bloco in ['atendimentos', 'evolucoes'] %}
        {% if not loop.first %} or {% endif %}
        p.{{ bloco }}.quantidade != array_length(p.{{ bloco }}.registros)
        or p.{{ bloco }}.quantidade != (
            {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
                {% if not loop.first %} + {% endif %}p.{{ bloco }}.quantidade_por_servico.{{ servico }}
            {% endfor %}
        )
        {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
            {% set nome = pu_nome_dimensao(servico) %}
            {% set dependencia = ref(nome) %}
            or p.{{ bloco }}.quantidade_por_servico.{{ servico }} is distinct from (
                select coalesce(sum(array_length(d.{{ bloco }})), 0)
                from {{ (var('pu_dimensoes', {}).get(servico, dependencia) if execute == true else dependencia) }} as d
                where cast(d.id_usuario as string) = p.usuario.id_usuario
            )
        {% endfor %}
    {% endfor %}
