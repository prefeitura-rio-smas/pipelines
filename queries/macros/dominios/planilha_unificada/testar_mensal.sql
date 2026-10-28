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

{% test pu_mensal_50_colunas(model) %}
select count(*) quantidade from {{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ model.identifier }}'
having count(*) != 50 or countif(regexp_contains(data_type, r'ARRAY|STRUCT')) > 0
{% endtest %}

{% test pu_mensal_contagem(model) %}
select sum(quantidade_atendimentos_mes) observado from {{ model }}
having sum(quantidade_atendimentos_mes) != (
    select count(*) from {{ ref('planilha_unificada') }} p, unnest(p.atendimentos.registros) a where a.tipo_servico='centro_pop'
)
{% endtest %}

{% test pu_mensal_grupos(model) %}
with esperado as (
    select p.usuario.id_usuario, a.id_unidade,
        date_trunc(safe_cast(substr(a.data_atendimento, 1, 10) as date), month) as mes_referencia,
        count(distinct a.id_atendimento_modulo) as quantidade
    from {{ ref('planilha_unificada') }} p, unnest(p.atendimentos.registros) a
    where a.tipo_servico='centro_pop'
    group by 1,2,3
)
select coalesce(e.id_usuario,m.id_usuario) id_usuario
from esperado e full join {{ model }} m using(id_usuario,id_unidade,mes_referencia)
where e.id_usuario is null or m.id_usuario is null or e.quantidade != m.quantidade
    or nullif(m.id_unidade,'') is null or m.mes_referencia is null
    or m.ultimo_tecnico > last_day(m.mes_referencia)
{% endtest %}
