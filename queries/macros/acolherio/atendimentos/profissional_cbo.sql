{% macro profissional_cbo(dsccbo_col) %}
case
    when lower({{ dsccbo_col }}) like 'articulador comunitário%' then 'ARTICULADOR COMUNITÁRIO'
    when lower({{ dsccbo_col }}) like 'assistente administrativo%' then 'ASSISTENTE ADMINISTRATIVO'
    when lower({{ dsccbo_col }}) like 'assistente social%' then 'ASSISTENTE SOCIAL'
    when lower({{ dsccbo_col }}) like 'educador social%' then 'EDUCADOR SOCIAL'
    when lower({{ dsccbo_col }}) like 'entrevistador social%' then 'ENTREVISTADOR SOCIAL'
    when lower({{ dsccbo_col }}) like 'pedagogo%' then 'PEDAGOGO'
    when lower({{ dsccbo_col }}) like 'psicólogo%' then 'PSICÓLOGO'
    when lower({{ dsccbo_col }}) like 'recepcionista%' then 'RECEPCIONISTA'
    else upper({{ dsccbo_col }})
end
{% endmacro %}
