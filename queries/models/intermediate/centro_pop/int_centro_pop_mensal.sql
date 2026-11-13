{{ config(alias='centro_pop_mensal', schema='relatorio', materialized='table', enabled=target.name != 'prod', tags=['centro_pop_mensal', 'planilha_unificada']) }}
{% set mes = var('centro_pop_mes', none) %}
with cargas as (
    {% for tabela in ['gh_atendimentos', 'gh_atend_familia', 'gh_evolupac', 'gh_evolufamil', 'gh_ativ_paciente'] %}
        select max(_airbyte_extracted_at) as data_carga from {{ source('brutos_acolherio_staging', tabela) }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

competencia as (
    select {% if mes %}date('{{ mes }}'){% else %}date_trunc(current_date('America/Sao_Paulo'), month){% endif %} as mes
),

unidades as (
    select
        id_unidade,
        nome_unidade
    from {{ ref('dim_unidades') }} where tipo_unidade = 'Centro POP'
),

registros_escopo as (
    select r.* replace (concat(cast(r.id_usuario as string), ':', cast(r.id_unidade as string)) as id_usuario)
    from {{ ref('int_registros_centro_pop') }} as r
    where
        r.id_unidade in (select un.id_unidade from unidades as un)
        and coalesce(safe_cast(r.data_ocorrencia as date), safe_cast(r.data_registro as date), r.data_presenca, date('1900-01-01')) <= least(last_day((select c.mes from competencia as c)), current_date('America/Sao_Paulo'))
        and (r.data_registro is null or date(r.data_registro) <= last_day((select c.mes from competencia as c)))
),

atendimentos_escopo as (
    select
        concat(cast(p.id_usuario as string), ':', cast(a.id_unidade as string)) as id_usuario,
        array_agg(a) as atendimentos
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    where
        a.id_unidade in (select un.id_unidade from unidades as un)
        and (a.data_atendimento is null or a.data_atendimento <= least(last_day((select c.mes from competencia as c)), current_date('America/Sao_Paulo')))
        and (safe_cast(a.data_cadastro_atendimento as date) is null or safe_cast(a.data_cadastro_atendimento as date) <= last_day((select c.mes from competencia as c)))
    group by 1
),

evolucoes_escopo as (
    select e.* from {{ ref('int_evolucoes_por_usuario') }} as e
    where
        e.id_unidade in (select un.id_unidade from unidades as un)
        and (e.data_evolucao is null or date(e.data_evolucao) <= least(last_day((select c.mes from competencia as c)), current_date('America/Sao_Paulo')))
),

usuarios_escopo as (
    select id_usuario from registros_escopo
    union distinct
    select id_usuario from atendimentos_escopo
    union distinct
    select concat(cast(id_usuario as string), ':', cast(id_unidade as string)) from evolucoes_escopo
),

perfil_scoped as ({{ pu_perfil_centro_pop_sql('registros_escopo', 'atendimentos_escopo', 'usuarios_escopo') }}),

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
    split(u.id_usuario, ':')[offset(0)] as id_usuario,
    safe_cast(split(u.id_usuario, ':')[offset(1)] as int64) as id_unidade,
    un.nome_unidade,
    (select c.mes from competencia as c) as mes_referencia,
    'Prévia' as situacao,
    cast(null as int64) as versao,
    cast(null as timestamp) as data_snapshot,
    cast(null as string) as id_fechamento,
    current_timestamp() as data_atualizacao,
    (select min(cg.data_carga) from cargas as cg) as data_carga_fontes,
    (
        select count(*) from unnest(a.atendimentos) as ev
        where date_trunc(ev.data_atendimento, month) = (select c.mes from competencia as c)
    ) as quantidade_atendimentos,
    (
        select count(*) from evolucoes_escopo as ev
        where concat(cast(ev.id_usuario as string), ':', cast(ev.id_unidade as string)) = u.id_usuario and date_trunc(date(ev.data_evolucao), month) = (select c.mes from competencia as c)
    ) as quantidade_evolucoes,
    (
        select count(*) from registros_escopo as ev
        where ev.id_usuario = u.id_usuario and ev.tipo_registro = 'participacao' and date_trunc(ev.data_presenca, month) = (select c.mes from competencia as c)
    ) as quantidade_participacoes,
    {{ pu_cpop_perfil_publico('perfil', listas_em_texto=true) }},
    part.nome_atividade as ultima_participacao_atividade,
    part.nome_unidade as ultima_participacao_unidade,
    part.data_presenca as ultima_participacao_data,
    part.hora_presenca as ultima_participacao_hora
from usuarios_escopo as u
left join perfil_scoped as perfil on u.id_usuario = perfil.id_usuario
left join participacoes as part on u.id_usuario = part.id_usuario
left join atendimentos_escopo as a on u.id_usuario = a.id_usuario
left join unidades as un on safe_cast(split(u.id_usuario, ':')[offset(1)] as int64) = un.id_unidade
where (select c.mes from competencia as c) >= date('2026-11-01')
