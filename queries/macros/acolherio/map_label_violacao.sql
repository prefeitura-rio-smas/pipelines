{% macro map_label_violacao(titulo_formulario, label, valor) %}
    CASE
        WHEN {{ titulo_formulario }} LIKE '%Adolescente em trabalho%'
            OR {{ titulo_formulario }} LIKE '%PETI%'
            OR (
                {{ label }} LIKE '%Marcação%CadÚnico%'
                AND {{ valor }} LIKE '%Trabalho Infantil%'
            )
            THEN '20'
        WHEN {{ titulo_formulario }} LIKE '%MSE%'
            OR {{ titulo_formulario }} LIKE '%Penas e Medidas Alternativas%'
            THEN '11'
        ELSE NULL
    END
{% endmacro %}