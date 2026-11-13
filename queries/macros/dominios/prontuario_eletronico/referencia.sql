{% macro pu_ref(nome) %}
    {# Register the real dependency even when its input is redirected. #}
    {% set dependencia = ref(nome) %}
    {% if execute == true and nome in var('pu_fontes', {}) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier=nome) }}
    {% else %}
        {{ dependencia }}
    {% endif %}
{% endmacro %}

{% macro pu_programada() %}
    {% set origem = source('acolherio_referencia', 'atendimentos_programada') %}
    {% if execute == true and var('pu_referencia', false) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier='atendimentos_programada') }}
    {% else %}
        {{ origem }}
    {% endif %}
{% endmacro %}

{% macro pu_filtro_email() %}
    {% set origem = source('dashboard_acolherio', 'filtro_email_dev') %}
    {% if execute and var('pu_base_schema', false) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier='filtro_email') }}
    {% else %}{{ origem }}{% endif %}
{% endmacro %}
