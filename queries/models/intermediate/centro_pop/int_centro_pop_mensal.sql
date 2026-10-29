{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with fonte as (
    select * from {{ ref('planilha_unificada') }}
),

eventos as (
    select
        a.*,
        p.usuario.id_usuario,
        safe_cast(substr(a.data_atendimento, 1, 10) as date) as data_evento
    from fonte as p, unnest(p.atendimentos.registros) as a
    where a.tipo_servico = 'centro_pop'
),

meses as (
    select
        id_usuario,
        id_unidade,
        date_trunc(data_evento, month) as mes_referencia,
        max(nome_unidade) as nome_centro_pop,
        count(distinct id_atendimento_modulo) as quantidade,
        countif(
            nullif(tipo_atendimento_descricao, '') is not null
            and lower(trim(tipo_atendimento_descricao)) not in (
                'centro pop - plano de acompanhamento indiv. (pai)',
                'centro pop - plano de atendimento individual (pai)'
            )
        ) as pontuais,
        countif(nullif(tipo_atendimento_descricao, '') is null) as tipos_desconhecidos
    from eventos
    group by id_usuario, id_unidade, mes_referencia
),

contexto as (
    select
        m.*,
        p.usuario,
        array(
            select distinct pr.nome_profissional
            from unnest(p.atendimentos.registros) as a, unnest(a.profissionais) as pr
            where
                a.id_unidade = m.id_unidade and a.tipo_servico = 'centro_pop'
                and date_trunc(safe_cast(substr(a.data_atendimento, 1, 10) as date), month) = m.mes_referencia
                and pr.nome_profissional != ''
            order by pr.nome_profissional
        ) as profissionais,
        (
            select max(safe_cast(substr(a.data_atendimento, 1, 10) as date))
            from unnest(p.atendimentos.registros) as a
            where
                a.id_unidade = m.id_unidade and a.tipo_servico = 'centro_pop'
                and a.classificacao_atendimento = 'Atendimento Técnico'
                and safe_cast(substr(a.data_atendimento, 1, 10) as date) <= last_day(m.mes_referencia)
        ) as ultimo_tecnico,
        array(
            select h from unnest(p.escopos.centro_pop.historico) as h
            where
                (h.id_unidade = m.id_unidade or h.abrangencia = 'pessoa')
                and safe_cast(h.data_registro as datetime) < datetime(date_add(last_day(m.mes_referencia), interval 1 day))
                and (
                    nullif(h.data_resposta, '') is null
                    or safe_cast(h.data_resposta as datetime) < datetime(date_add(last_day(m.mes_referencia), interval 1 day))
                )
            order by
                safe_cast(h.data_registro as datetime) desc,
                safe_cast(h.data_resposta as datetime) desc, to_json_string(h) desc
        ) as historico,
        array(
            select distinct as struct
                a.id_atividade,
                a.id_unidade,
                a.data_presenca,
                a.hora_presenca
            from unnest(p.escopos.centro_pop.participacoes) as a
            where
                a.id_unidade = m.id_unidade
                and date_trunc(safe_cast(substr(a.data_presenca, 1, 10) as date), month) = m.mes_referencia
        ) as participacoes
    from meses as m inner join fonte as p on m.id_usuario = p.usuario.id_usuario
)

select
    c.*,
    {% for assunto in ['pai', 'atendimento_social', 'saude', 'situacao_rua', 'desligamento'] %}
        (
            select h from unnest(c.historico) as h
            where h.tipo_registro = '{{ assunto }}'
            order by safe_cast(h.data_registro as datetime) desc, safe_cast(h.data_resposta as datetime) desc, to_json_string(h) desc limit 1
        ) as {{ assunto }}{% if not loop.last %},{% endif %}
    {% endfor %}
from contexto as c
