{% macro map_flag_situacao_rua (coluna) %}
  case
    when safe_cast(trim(cast({{ coluna }} as string)) as int64) = 5 then 'Sim'
    -- Domínio indmoradi (moradia.ini): demais moradias conhecidas.
    when safe_cast(trim(cast({{ coluna }} as string)) as int64) in (1, 2, 3, 4, 6, 7, 8, 9) then 'Não'
    when upper(trim(cast({{ coluna }} as string))) in ('A', 'B', 'C', 'D') then 'Não'
    -- E = Desconhecido; ausência e códigos fora do domínio permanecem desconhecidos.
    else null
  end
{% endmacro %}
