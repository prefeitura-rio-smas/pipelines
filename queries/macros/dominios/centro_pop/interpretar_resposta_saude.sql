{% macro interpretar_resposta_saude(coluna) %}
    -- O formulário aceita descrições livres: texto conhecido é evidência positiva.
    -- Esta regra reconhece marcadores exatos; não interpreta frases ou diagnósticos.
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
