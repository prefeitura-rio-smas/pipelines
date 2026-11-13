{% test pu_contrato_publico(model) %}
    {% set tipos = {} %}
    {% set contrato = {} %}
    {% if execute %}
    {% for node in graph.nodes.values() if node.resource_type == 'model' and node.name == 'prontuario_eletronico' %}
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
    select 'email_filtro' as campo, 'STRING' as tipo
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
where field_path in ('escopos', 'atendimentos.familias_atendidas')
union all
select field_path as falha from campos
where (starts_with(field_path, 'centro_pop.') or starts_with(field_path, 'creas.') or starts_with(field_path, 'alta_complexidade.'))
  and (starts_with(data_type, 'STRUCT<') or starts_with(data_type, 'ARRAY<'))
  and field_path not in ('centro_pop.historico_mensal', 'centro_pop.registros', 'creas.registros', 'alta_complexidade.registros')
union all
select 'campos_raiz' as falha
from (
    select count(*) as total
    from `{{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{{ model.identifier }}'
)
where total != {{ contrato.get('campos_raiz', ['usuario', 'familias', 'projetos_sociais', 'atendimentos', 'evolucoes', 'centro_pop', 'creas', 'alta_complexidade', 'nome_unidade', 'email_filtro']) | length }}
{% endtest %}

{% test pu_campos_topo(model) %}
-- Compara conjuntos de destinatários, contando também repetições indevidas na saída.
with esperado as (
    select distinct cast(b.id_usuario as string) as id_usuario, lower(trim(e)) as email
    from (
with eventos as (
    select
        'u' as modulo,
        id_atendimento,
        id_unidade,
        id_tipo_atendimento,
        flag_cancelado
    from {{ pu_ref('raw_atendimentos_usuarios') }}
    union all
    select
        'f' as modulo,
        id_atendimento,
        id_unidade,
        id_tipo_atendimento,
        flag_cancelado
    from {{ pu_ref('raw_atendimentos_familias') }}
),

contextos as (
    select
        modulo,
        id_atendimento,
        if(
            count(distinct to_json_string(struct(id_unidade, id_tipo_atendimento, flag_cancelado))) > 1,
            error('Contextos internos conflitantes para atendimento da programada'),
            array_agg(struct(id_unidade, id_tipo_atendimento, flag_cancelado) limit 1)[offset(0)]
        ) as contexto
    from eventos
    group by modulo, id_atendimento
),

cbos as (
    select
        p.id_profissional,
        c.descricao,
        if(count(distinct p.codigo_cbo) = 1, max(p.codigo_cbo), null) as codigo_cbo
    from {{ pu_ref('raw_profissionais_ocupacoes') }} as p
    inner join {{ pu_ref('raw_cbo') }} as c on p.codigo_cbo = c.codigo_cbo
    group by p.id_profissional, c.descricao
),

selecionados as (
    select
        s.*,
        e.contexto.id_unidade as _id_unidade,
        e.contexto.id_tipo_atendimento as _id_tipo_atendimento,
        e.contexto.flag_cancelado as _flag_cancelado,
        c.codigo_cbo as _codigo_cbo
    from {{ ref('raw_atendimentos_programada') }} as s
    left join contextos as e on s.modulo = e.modulo and s.seq_atendimento = e.id_atendimento
    left join cbos as c on s.profissional_id = c.id_profissional and s.profissional_cbo_original = c.descricao
),

enriquecidos as (
    {{ aplicar_filtro_email('selecionados', pu_ref('raw_unidades'), pu_filtro_email()) }}
),

datas as (
    select
        modulo,
        id_atendimento,
        id_usuario,
        id_unidade,
        data_atendimento,
        origem_data_atendimento
    from {{ ref('int_datas_atendimento') }}
)

select
    b.modulo,
    b.cas,
    b.tipo_unidade,
    b.unidade_atendimento,
    b.email_unidade,
    b.email_cas,
    b.seq_atendimento,
    datas.data_atendimento as data_de_atendimento,
    b.hora_de_atendimento_original,
    b.hora_de_atendimento,
    b.id_usuario,
    b.id_familia,
    b.nome_usuario,
    b.documentacao,
    b.numero_documento,
    b.endereco,
    b.endereco_numero,
    b.endereco_complemento,
    b.bairro,
    b.referencia_ou_comunidade,
    b.nome_atendimento_original,
    b.data_nascimento,
    b.idade,
    b.raca_cor,
    b.sexo,
    b.profissional_id,
    b.profissional,
    b.profissional_cbo_original,
    b.profissional_cbo,
    b.cadastrante,
    b.data_cadastro_atendimento,
    b.tipo_atendimento,
    b.nome_atendimento,
    b.idade_faixa,
    b.id_atendimento,
    b.email,
    b.cbo_unico_rank,
    b.atendimento_unico_rank,
    b.projetos_sociais,
    b._id_unidade,
    b._id_tipo_atendimento,
    b._flag_cancelado,
    b._codigo_cbo,
    b.data_de_atendimento as _data_de_atendimento_base,
    coalesce(datas.origem_data_atendimento, 'sem_correspondencia') as _origem_data_atendimento
from enriquecidos as b
left join datas on
    b.modulo = datas.modulo and b.seq_atendimento = datas.id_atendimento
    and b.id_usuario = datas.id_usuario and b._id_unidade = datas.id_unidade

) as b,
        unnest(split(regexp_replace(coalesce(b.email, ''), r'[;\r\n]', ','), ',')) as e
    where nullif(trim(e), '') is not null
), obtido as (
    select p.usuario.id_usuario, trim(e) as email
    from {{ model }} as p, unnest(split(p.email_filtro, ',')) as e
    where nullif(trim(e), '') is not null
), comparacao as (
    select id_usuario, email, 1 as delta from esperado
    union all
    select id_usuario, email, -1 as delta from obtido
)
select id_usuario from comparacao
group by id_usuario, email having sum(delta) != 0
union all
select usuario.id_usuario from {{ model }} where email_filtro is null or nome_unidade is null
{% endtest %}

{% test pu_perfis_preservados(model) %}
with ciclo as (
    select id_usuario,
        array(select as struct c.* from unnest(historico) c
              order by if(c.data_saida is null, 0, 1), c.data_entrada desc nulls last,
                       c.id_ciclo desc, to_json_string(c) desc limit 1)[safe_offset(0)] as ultimo
    from {{ ref('alta_complexidade') }}
)
select p.usuario.id_usuario
from {{ model }} p
left join {{ ref('int_perfil_creas_simples') }} c on p.usuario.id_usuario = cast(c.id_usuario as string)
left join ciclo a on p.usuario.id_usuario = cast(a.id_usuario as string)
where (c.id_usuario is not null and to_json_string(parse_json(to_json_string((select as struct p.creas.* except (email_filtro, registros))))) is distinct from to_json_string(parse_json(to_json_string((select as struct c.* except(id_usuario))))))
   or to_json_string((select as struct p.alta_complexidade.* except (email_filtro, registros))) is distinct from to_json_string(struct(
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
