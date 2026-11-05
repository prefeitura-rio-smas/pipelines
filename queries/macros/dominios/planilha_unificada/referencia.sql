{% macro pu_programada() %}
    {% set origem = source('acolherio_referencia', 'atendimentos_programada') %}
    {% if execute == true and var('pu_referencia', false) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier='atendimentos_programada') }}
    {% else %}
        {{ origem }}
    {% endif %}
{% endmacro %}
