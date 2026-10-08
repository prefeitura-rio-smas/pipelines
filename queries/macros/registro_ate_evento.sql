{#
Gera uma condição para verificar se a data do registro é anterior ou igual
à data de referência. Ambos os parâmetros são expressões SQL.
A referência é definida pelo modelo consumidor, que também relaciona os
registros e escolhe qual deles retornar.
#}
{% macro registro_ate_evento(data_registro, data_referencia) %}
    (
        safe_cast({{ data_registro }} as date) <= {{ data_referencia }}
    )
{% endmacro %}
