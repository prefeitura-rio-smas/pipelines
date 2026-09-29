-- Datas de solicitação e regulação no grão de ciclo (1 linha por id_ciclo).
-- A solicitação se relaciona pela data/unidade de atendimento; a regulação,
-- pela data/unidade de entrada prevista. Notificações se relacionam por id_regulacao.

with ciclos as (
    select
        id_ciclo,
        id_usuario,
        id_unidade,
        data_entrada
    from {{ ref('raw_usuarios_acolhimentos') }}
),

notificacoes_por_regulacao as (
    select
        id_regulacao,
        min(data_notificacao) as data_notificacao
    from {{ ref('raw_notificacoes_acolhimento') }}
    where id_regulacao is not null
    group by id_regulacao
),

solicitacoes_por_ciclo as (
    select
        c.id_ciclo,
        array_agg(
            s.data_solicitacao ignore nulls
            order by s.data_solicitacao desc, s.id_solicitacao desc
            limit 1
        )[safe_offset(0)] as data_solicitacao
    from ciclos as c
    left join {{ ref('raw_solicitacoes_vagas') }} as s
        on
            c.id_usuario = s.id_usuario
            and c.data_entrada = s.data_solicitacao_atendida
            and c.id_unidade = s.id_unidade_atendimento
    group by c.id_ciclo
),

regulacoes_por_ciclo as (
    select
        c.id_ciclo,
        array_agg(
            coalesce(n.data_notificacao, r.data_regulacao) ignore nulls
            order by coalesce(n.data_notificacao, r.data_regulacao) desc, r.id_regulacao desc
            limit 1
        )[safe_offset(0)] as data_regulacao
    from ciclos as c
    left join {{ ref('raw_regulacoes_acolhimento') }} as r
        on
            c.id_usuario = r.id_usuario
            and c.data_entrada = r.data_entrada_prevista
            and c.id_unidade = r.id_unidade
    left join notificacoes_por_regulacao as n
        on r.id_regulacao = n.id_regulacao
    group by c.id_ciclo
)

select
    c.id_ciclo,
    s.data_solicitacao,
    r.data_regulacao
from ciclos as c
left join solicitacoes_por_ciclo as s
    on c.id_ciclo = s.id_ciclo
left join regulacoes_por_ciclo as r
    on c.id_ciclo = r.id_ciclo
