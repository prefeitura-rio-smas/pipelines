{% macro email_cas (coluna) %}
  case
    when lower({{ coluna }}) = '10' then 'cas10@prefeitura.rio'
    when lower({{ coluna }}) = '09' then 'cas9@prefeitura.rio'
    when lower({{ coluna }}) = '08' then 'cas8@prefeitura.rio'
    when lower({{ coluna }}) = '07' then 'cas7@prefeitura.rio'
    when lower({{ coluna }}) = '06' then 'cas6@prefeitura.rio'
    when lower({{ coluna }}) = '05' then 'cas5@prefeitura.rio'
    when lower({{ coluna }}) = '04' then 'cas4@prefeitura.rio'
    when lower({{ coluna }}) = '03' then 'cas3@prefeitura.rio'
    when lower({{ coluna }}) = '02' then 'cas2@prefeitura.rio'
    when lower({{ coluna }}) = '01' then 'cas1@prefeitura.rio'
  end
{% endmacro %}
