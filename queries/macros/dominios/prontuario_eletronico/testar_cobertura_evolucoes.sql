{% test pu_evolucoes_cobertura(model) %}
with esperado as (
    select 'usuario' origem_modulo, id_evolucao, id_usuario from {{ ref('raw_evolucoes_usuarios') }} where data_cancelamento is null
    union distinct
    select 'familia', id_evolucao, id_paciente from {{ ref('raw_evolucoes_familias') }} where data_cancelamento is null
    union distinct
    select 'administrativa', id_evolucao, id_paciente from {{ ref('raw_evolucoes_administrativas') }} where data_cancelamento is null
)
select * from esperado
except distinct
select origem_modulo, id_evolucao, coalesce(id_usuario, id_paciente_familia) from {{ model }}
{% endtest %}

{% test pu_fonte_evolucoes_atualizada(model) %}
    {{ test_pu_evolucoes_cobertura(ref('int_evolucoes')) }}
{% endtest %}
