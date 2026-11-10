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
