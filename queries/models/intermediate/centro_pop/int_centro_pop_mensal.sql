{{ config(alias='centro_pop_mensal', schema='relatorio', materialized='table', enabled=target.name != 'prod', tags=['centro_pop_mensal', 'planilha_unificada']) }}
{% set mes = var('centro_pop_mes', none) %}
with cargas as (
    {% for tabela in ['gh_atendimentos', 'gh_atend_familia', 'gh_evolupac', 'gh_evolufamil', 'gh_ativ_paciente'] %}
        select max(_airbyte_extracted_at) as data_carga from {{ source('brutos_acolherio_staging', tabela) }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

unidades as (
    select
        id_unidade,
        nome_unidade
    from {{ ref('dim_unidades') }}
    where tipo_unidade = 'Centro POP'
),

registros as (
    select
        r.*,
        case
            when r.tipo_registro = 'participacao' then r.data_presenca
            else coalesce(safe_cast(r.data_ocorrencia as date), safe_cast(r.data_registro as date))
        end as data_evento
    from {{ ref('int_registros_centro_pop') }} as r
),

atendimentos as (
    select
        p.id_usuario,
        a.id_unidade,
        a.data_atendimento as data_evento,
        (select as struct a.* except (email, flag_cancelado, projetos_sociais_familia, atendimento_compartilhado)) as atendimento
    from {{ ref('int_atendimentos_por_usuario') }} as p
    cross join unnest(p.atendimentos) as a
),

formularios as (
    select
        p.id_usuario,
        f.id_unidade,
        f.id_evolucao_modulo,
        array_agg(f order by posicao) as formularios
    from {{ ref('int_formularios_por_usuario') }} as p
    cross join unnest(p.formularios) as f with offset as posicao
    group by p.id_usuario, f.id_unidade, f.id_evolucao_modulo
),

evolucoes as (
    select
        e.* replace ({{ texto_html_legivel('e.descricao_evolucao') }} as descricao_evolucao),
        date(e.data_evolucao) as data_evento,
        coalesce(f.formularios, []) as formularios
    from {{ ref('int_evolucoes_por_usuario') }} as e
    left join formularios as f
        on e.id_usuario = f.id_usuario and e.id_unidade = f.id_unidade and e.id_evolucao_modulo = f.id_evolucao_modulo
),

movimento as (
    {% for origem in ['atendimentos', 'evolucoes', 'registros'] %}
        select
            id_usuario,
            id_unidade,
            data_evento
        from {{ origem }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

competencias as (
    select distinct
        m.id_usuario,
        m.id_unidade,
        date_trunc(m.data_evento, month) as mes
    from movimento as m
    inner join unidades as un on m.id_unidade = un.id_unidade
    where
        m.data_evento <= current_date('America/Sao_Paulo')
        {% if mes %}and date_trunc(m.data_evento, month) = date('{{ mes }}'){% endif %}
),

usuarios_escopo as (
    select
        id_usuario as id_cidadao,
        id_unidade,
        mes,
        concat(cast(id_usuario as string), ':', cast(id_unidade as string), ':', cast(mes as string)) as id_usuario,
        least(last_day(mes), current_date('America/Sao_Paulo')) as limite
    from competencias
),

registros_escopo as (
    select
        r.* replace (u.id_usuario as id_usuario),
        u.mes
    from usuarios_escopo as u
    inner join registros as r on u.id_cidadao = r.id_usuario and u.id_unidade = r.id_unidade
    where
        r.data_evento <= u.limite
        and (r.data_registro is null or date(r.data_registro) <= u.limite)
        and (r.data_resposta is null or date(r.data_resposta) <= u.limite)
),

atendimentos_escopo as (
    select
        u.id_usuario,
        array_agg(a.atendimento) as atendimentos
    from usuarios_escopo as u
    inner join atendimentos as a on u.id_cidadao = a.id_usuario and u.id_unidade = a.id_unidade
    where
        a.data_evento <= u.limite
        and coalesce(safe_cast(a.atendimento.data_cadastro_atendimento as date), a.data_evento) <= u.limite
    group by u.id_usuario
),

{% for origem, ordenacao in [
    ('atendimentos', 'a.data_evento, a.atendimento.hora_atendimento, a.atendimento.id_atendimento_modulo'),
    ('evolucoes', 'a.data_evolucao, a.id_evolucao_modulo'),
    ('registros', 'a.data_evento, a.id_registro')
] %}
    {{ origem }}_mensais as (
        select
            u.id_usuario,
            array_agg(
                {% if origem == 'atendimentos' %}a.atendimento
                {% else %}(select as struct a.* except (id_usuario)){% endif %}
                order by {{ ordenacao }}
            ) as itens
        from usuarios_escopo as u
        inner join {{ origem }} as a on u.id_cidadao = a.id_usuario and u.id_unidade = a.id_unidade
        where a.data_evento between u.mes and u.limite
        group by u.id_usuario
    ),
{% endfor %}

perfil_scoped as ({{ pu_perfil_centro_pop_sql('registros_escopo', 'atendimentos_escopo', 'usuarios_escopo') }}),

atendimentos_tecnicos as ({{ datas_atendimentos_tecnicos_centro_pop() }}),

ultimo_tecnico as (
    select
        u.id_usuario,
        max(t.data_ultimo_atend_tecnico_informada) as data_ultimo
    from usuarios_escopo as u
    inner join atendimentos_tecnicos as t
        on u.id_cidadao = t.id_usuario and u.id_unidade = t.id_unidade
    where t.data_evolucao <= u.limite and t.data_ultimo_atend_tecnico_informada <= u.limite
    group by u.id_usuario
),

participacoes as (
    select * from registros_escopo
    where tipo_registro = 'participacao'
    qualify row_number() over (
        partition by
            id_usuario order by
            data_presenca desc nulls last,
            hora_presenca desc nulls last, id_atividade desc nulls last, id_registro desc
    ) = 1
)

select
    cast(u.id_cidadao as string) as id_usuario,
    u.id_unidade,
    un.nome_unidade,
    u.mes as mes_referencia,
    'Prévia' as situacao,
    cast(null as int64) as versao,
    cast(null as timestamp) as data_snapshot,
    cast(null as string) as id_fechamento,
    current_timestamp() as data_atualizacao,
    (select min(cg.data_carga) from cargas as cg) as data_carga_fontes,
    array_length(coalesce(a.itens, [])) as quantidade_atendimentos,
    array_length(coalesce(e.itens, [])) as quantidade_evolucoes,
    (select countif(item.tipo_registro = 'participacao') from unnest(r.itens) as item) as quantidade_participacoes,
    {{ pu_cpop_perfil_publico('perfil', listas_em_texto=true) }},
    part.nome_atividade as ultima_participacao_atividade,
    part.nome_unidade as ultima_participacao_unidade,
    part.data_presenca as ultima_participacao_data,
    part.hora_presenca as ultima_participacao_hora,
    coalesce(a.itens, []) as atendimentos,
    coalesce(e.itens, []) as evolucoes,
    coalesce(r.itens, []) as ocorrencias,
    (
        select string_agg(distinct trim(demanda), ', ' order by trim(demanda))
        from unnest(r.itens) as item cross join unnest(item.demandas) as demanda
        where item.tipo_registro in ('pai', 'atendimento_social') and nullif(trim(demanda), '') is not null
    ) as procedimentos_mes,
    (
        select
            string_agg(
                distinct concat(trim(enc.label), ': ', trim(enc.valor)), ', '
                order by concat(trim(enc.label), ': ', trim(enc.valor))
            )
        from unnest(r.itens) as item cross join unnest(item.encaminhamentos) as enc
        where nullif(trim(enc.label), '') is not null and nullif(trim(enc.valor), '') is not null
    ) as encaminhamentos_mes,
    coalesce(cast(t.data_ultimo as string), '') as data_ultimo_atend_tecnico_informada
from usuarios_escopo as u
left join perfil_scoped as perfil on u.id_usuario = perfil.id_usuario
left join participacoes as part on u.id_usuario = part.id_usuario
left join atendimentos_mensais as a on u.id_usuario = a.id_usuario
left join evolucoes_mensais as e on u.id_usuario = e.id_usuario
left join registros_mensais as r on u.id_usuario = r.id_usuario
left join ultimo_tecnico as t on u.id_usuario = t.id_usuario
left join unidades as un on u.id_unidade = un.id_unidade
