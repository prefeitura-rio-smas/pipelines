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
    union all select 'centro_pop', 'STRUCT<'
    union all select 'creas', 'STRUCT<'
    union all select 'alta_complexidade', 'STRUCT<'
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
where field_path in ('escopos', 'atendimentos.registros.email', 'atendimentos.familias_atendidas')
union all
select field_path as falha from campos
where (starts_with(field_path, 'centro_pop.') or starts_with(field_path, 'creas.') or starts_with(field_path, 'alta_complexidade.'))
  and (starts_with(data_type, 'STRUCT<') or starts_with(data_type, 'ARRAY<'))
union all
select 'campos_raiz' as falha
from (
    select count(*) as total
    from `{{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{{ model.identifier }}'
)
where total != {{ contrato.get('campos_raiz', ['usuario', 'familias', 'projetos_sociais', 'atendimentos', 'evolucoes', 'centro_pop', 'creas', 'alta_complexidade', 'nome_unidade', 'email']) | length }}
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

{% test pu_perfis_preservados(model) %}
with ciclo as (
    select id_usuario,
        array(select as struct c.* from unnest(historico) c
              order by if(c.data_saida is null, 0, 1), c.data_entrada desc nulls last,
                       c.id_ciclo desc, to_json_string(c) desc limit 1)[safe_offset(0)] as ultimo
    from {{ ref('dim_alta_complexidade') }}
)
select p.usuario.id_usuario
from {{ model }} p
left join {{ ref('int_perfil_creas_simples') }} c on p.usuario.id_usuario = cast(c.id_usuario as string)
left join ciclo a on p.usuario.id_usuario = cast(a.id_usuario as string)
where (c.id_usuario is not null and to_json_string(parse_json(to_json_string(p.creas))) is distinct from to_json_string(parse_json(to_json_string((select as struct c.* except(id_usuario))))))
   or to_json_string(p.alta_complexidade) is distinct from to_json_string(struct(
        a.ultimo.id_ciclo as id_ciclo, a.ultimo.id_unidade as id_unidade,
        a.ultimo.nome_unidade as nome_unidade, a.ultimo.data_entrada as data_entrada,
        a.ultimo.data_saida as data_saida, a.ultimo.motivo_saida as motivo_saida,
        a.ultimo.tempo_permanencia_dias as tempo_permanencia_dias))
{% endtest %}

{% test pu_mapeamento_creas(model) %}
select distinct mapa.coluna
from {{ ref('creas_campos') }} mapa
left join `{{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` campos
    on campos.table_name = '{{ model.identifier }}' and campos.field_path = concat('creas.', mapa.coluna)
where campos.field_path is null
{% endtest %}
