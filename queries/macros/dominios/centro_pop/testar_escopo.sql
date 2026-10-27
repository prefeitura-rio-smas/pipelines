{% test pu_cpop_integridade(model) %}
select id_registro
from {{ model }}
where conflitos != 0 or id_usuario is null or id_registro is null
    or tipo_registro not in ('pai', 'atendimento_social', 'saude', 'situacao_rua', 'desligamento', 'participacao')
    or abrangencia not in ('pessoa', 'unidade')
    or (tipo_registro = 'participacao' and (id_atividade is null or id_unidade is null or data_presenca is null))
    {% for campo in ['possui_referencias_familiares', 'possibilidade_reinsercao_familiar', 'uso_substancias_psicoativas', 'possui_problema_saude', 'realiza_acompanhamento_saude'] %}
        or ({{ campo }} is not null and {{ campo }} not in ('Sim', 'Não'))
    {% endfor %}
{% endtest %}

{% test pu_cpop_perfil(model) %}
with esperado as ({{ pu_cpop_perfil_sql() }}),
comparacao as (
    select id_usuario, to_json_string(e) valor, 'esperado' origem from esperado e
    union all
    select id_usuario, to_json_string(o) valor, 'obtido' origem from {{ model }} o
)
select id_usuario from comparacao group by id_usuario, valor
having countif(origem = 'esperado') != countif(origem = 'obtido')
{% endtest %}

{% test pu_cpop_sem_structs(model) %}
select column_name from {{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ model.identifier }}' and regexp_contains(data_type, r'STRUCT')
{% endtest %}
