{% macro pu_ref(nome) %}
    {# Register the real dependency even when its input is redirected. #}
    {% set dependencia = ref(nome) %}
    {% if execute == true and nome in var('pu_fontes', {}) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier=nome) }}
    {% else %}
        {{ dependencia }}
    {% endif %}
{% endmacro %}

{% macro pu_contagens_servico(registros) %}
    struct({% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}(select count(*) from unnest({{ registros }}) as r where r.tipo_servico = '{{ servico }}') as {{ servico }}{% if not loop.last %}, {% endif %}{% endfor %})
{% endmacro %}

{% macro pu_contagens_diretas(registros) %}
    {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
        {% if not loop.first %},{% endif %}
        (select count(*) from unnest({{ registros }}) as evento where evento.tipo_servico = '{{ servico }}') as quantidade_{{ servico }}
    {% endfor %}
{% endmacro %}
