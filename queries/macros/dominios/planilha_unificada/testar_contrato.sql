{% test pu_contrato_publico(model) %}
{% set tipos = {} %}
{% set contrato = {} %}
{% if execute %}
    {% for node in graph.nodes.values() if node.resource_type == 'model' and node.name == 'planilha_unificada' %}
        {% set _ = tipos.update(node.config.meta.get('excecoes_tipos', {})) %}
        {% set _ = contrato.update(node.config.meta) %}
    {% endfor %}
{% endif %}
with campos as (
    select field_path, data_type
    from `{{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    where table_name = '{{ model.identifier }}'
    union distinct
    select column_name, data_type
    from `{{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{{ model.identifier }}'
), esperado as (
    select 'email' as campo, 'STRING' as tipo
    union all select 'nome_unidade', 'STRING'
    union all select 'usuario.id_usuario', 'STRING'
    union all select 'usuario.idade_faixa', 'STRING'
    union all select 'atendimentos.registros.tipo_unidade', 'STRING'
    union all select 'atendimentos.registros.elegivel_indicador', 'STRING'
    union all select 'escopos.creas', 'STRUCT<'
    union all select 'escopos.alta_complexidade', 'STRUCT<'
    {% for campo, tipo in tipos.items() %}
    union all select '{{ campo }}', '{{ tipo }}'
    {% endfor %}
)
select e.campo as falha
from esperado as e
left join campos as c on e.campo = c.field_path
where c.field_path is null or not starts_with(c.data_type, e.tipo)
union all
select field_path as falha from campos
where field_path in ('atendimentos.registros.email', 'atendimentos.familias_atendidas')
union all
select 'quantidade_campos' as falha from campos
having count(*) != {{ contrato.get('quantidade_campos', 298) }}
union all
select 'campos_raiz' as falha
from `{{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS`
where table_name = '{{ model.identifier }}'
having count(*) != {{ contrato.get('campos_raiz', ['usuario', 'familias', 'projetos_sociais', 'atendimentos', 'evolucoes', 'escopos', 'nome_unidade', 'email']) | length }}
{% endtest %}

{% test pu_campos_topo(model) %}
-- Compara conjuntos de destinatários, contando também repetições indevidas na saída.
with esperado as (
    select distinct cast(b.id_usuario as string) as id_usuario, lower(trim(e)) as email
    from {{ ref('int_atendimentos_bi') }} as b,
        unnest(split(regexp_replace(coalesce(b.email, ''), r'[;\r\n]', ','), ',')) as e
    where nullif(trim(e), '') is not null
), obtido as (
    select p.usuario.id_usuario, trim(e) as email
    from {{ model }} as p, unnest(split(p.email, ',')) as e
    where nullif(trim(e), '') is not null
), comparacao as (
    select id_usuario, email, 1 as delta from esperado
    union all
    select id_usuario, email, -1 as delta from obtido
)
select id_usuario from comparacao
group by id_usuario, email having sum(delta) != 0
union all
select usuario.id_usuario from {{ model }} where email is null or nome_unidade is null
{% endtest %}

{% test pu_escopos_preservados(model) %}
select p.usuario.id_usuario
from {{ model }} as p
left join {{ ref('dim_creas') }} as c on p.usuario.id_usuario = cast(c.id_usuario as string)
left join {{ ref('dim_alta_complexidade') }} as a on p.usuario.id_usuario = cast(a.id_usuario as string)
where to_json_string(p.escopos.creas) is distinct from to_json_string(struct(
    coalesce(c.historico, []) as historico, coalesce(c.participacoes, []) as participacoes
))
or to_json_string(p.escopos.alta_complexidade) is distinct from to_json_string(struct(
    coalesce(a.historico, []) as historico
))
{% endtest %}
