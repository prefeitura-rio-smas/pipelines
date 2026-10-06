-- Data de solicitação no grão de ciclo (1 linha por id_ciclo).
-- A solicitação se relaciona pela data/unidade de atendimento.

with ciclos as (
    select
        id_ciclo,
        id_usuario,
        id_unidade,
        data_entrada
    from {{ ref('raw_usuarios_acolhimentos') }}
)

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
