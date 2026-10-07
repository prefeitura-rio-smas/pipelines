{% macro evidencia_saude(coluna) %}
    -- Texto descritivo indica evidência; marcadores de ausência não indicam Sim.
    -- NULL permite distinguir desconhecido de uma resposta negativa explícita.
    case
        when {{ coluna }} is null or lower(trim({{ coluna }})) in (
            '', 'undefined', 'null', 'não informado', 'nao informado',
            'não informada', 'nao informada', 'não sabe', 'nao sabe',
            'não soube informar', 'nao soube informar',
            'sem informação', 'sem informacao', '-', 'não se aplica', 'nao se aplica'
        ) then null
        when lower(trim({{ coluna }})) in ('n', 'nao', 'não') then false
        else true
    end
{% endmacro %}
