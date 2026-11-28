{% macro idade_faixa(idade) -%}
case
    when {{ idade }} < 18 then 'Até 17 anos'
    when {{ idade }} < 30 then 'De 18 a 29 anos'
    when {{ idade }} < 45 then 'De 30 a 44 anos'
    when {{ idade }} < 60 then 'De 45 a 59 anos'
    when {{ idade }} < 75 then 'De 60 a 74 anos'
    when {{ idade }} >= 75 then 'Mais de 75 anos'
    else 'Não Informado'
end
{%- endmacro %}
