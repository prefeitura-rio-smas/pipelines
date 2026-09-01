{% macro map_label_violacao(titulo_formulario, regras=[]) %}
    CASE
        {% for r in regras %}
        WHEN (
            {% for t in r.titulos %}{{ titulo_formulario }} LIKE '%{{ t }}%'{% if not loop.last %} OR {% endif %}{% endfor %}
        ) THEN '{{ r.codigo }}'
        {% endfor %}
        ELSE NULL
    END
{% endmacro %}