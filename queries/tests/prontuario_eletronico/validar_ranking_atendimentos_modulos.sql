{{ config(tags=['planilha_unificada']) }}
-- depends_on: {{ ref('int_acolherio_atendimentos') }}

{% set campos = ['seq_atendimento', 'modulo', 'data_cadastro_atendimento', 'id_usuario',
    'id_familia', 'profissional_cbo', 'email'] %}

with casos as (
    select [
        struct(42 as seq_atendimento, 'u' as modulo, 'N' as flag_cancelado, date '2026-01-02' as data_cadastro_atendimento,
            42 as id_usuario, 10 as id_familia, 'Psicólogo' as profissional_cbo),
        struct(42, 'f', 'N', date '2026-01-01', 42, 10, 'Psicólogo'),
        struct(43, 'f', 'N', date '2026-01-01', 43, 10, 'Psicólogo'),
        struct(43, 'u', null, null, 43, 10, 'Psicólogo'),
        struct(44, 'u', 'S', date '2026-01-01', 44, 10, 'Psicólogo'),
        struct(45, 'u', 'N', date '2026-01-03', 45, 10, 'Psicólogo'),
        struct(46, 'f', 'N', date '2026-01-03', 45, 10, 'Psicólogo'),
        struct(47, 'u', 'N', date '2026-01-03', 47, 10, 'Psicólogo'),
        struct(47, 'f', 'N', date '2026-01-03', 47, 20, 'Psicólogo'),
        struct(48, 'f', 'N', date '2026-01-03', 48, 20, 'Psicólogo'),
        struct(48, 'f', 'N', date '2026-01-03', 48, 10, 'Psicólogo'),
        struct(48, 'f', 'N', date '2026-01-03', 48, 10, 'Educador social'),
        struct(48, 'f', 'N', date '2026-01-03', 48, 10, 'Educador social'),
        -- Mesmo primeiro grupo, dois segundos grupos. O QUALIFY conjunto elimina o segundo.
        struct(49, 'u', 'N', date '2026-01-01', 49, 10, 'Psicólogo'),
        struct(49, 'u', 'N', date '2026-01-02', 50, 10, 'Psicólogo'),
        struct(51, 'f', 'N', date '2026-01-03', 51, 10, 'Psicólogo'),
        struct(52, 'u', 'N', date '2026-01-02', 51, 10, 'Psicólogo')
    ] as eventos
),

linhas as (
    select
        posicao,
        evento.*,
        7 as profissional_id,
        date '2026-01-01' as data_de_atendimento,
        'Recepção' as nome_atendimento_original,
        'CREAS' as unidade_atendimento,
        '10:00' as hora_de_atendimento,
        if(posicao = 12, 'z@example.test', 'a@example.test') as email,
        concat(cast(evento.seq_atendimento as string), '-7') as id_atendimento
    from casos, unnest(eventos) as evento with offset as posicao
),

candidatos as (
    select * except (posicao)
    from linhas
    order by posicao
),

candidatos_invertidos as (
    select * except (posicao)
    from linhas
    order by posicao desc
),

obtido as (
    select 'direta' as ordem, a.*
    from ({{ selecionar_participacoes_atendimento('candidatos', campos, ['flag_cancelado']) }}) as a
    union all
    select 'inversa' as ordem, a.*
    from ({{ selecionar_participacoes_atendimento('candidatos_invertidos', campos, ['flag_cancelado']) }}) as a
),

esperado as (
    select ordem, evento.*, 'a@example.test' as email
    from unnest(['direta', 'inversa']) as ordem
    cross join unnest([
        struct(42 as seq_atendimento, 'f' as modulo, 'N' as flag_cancelado, date '2026-01-01' as data_cadastro_atendimento,
            42 as id_usuario, 10 as id_familia, 'Psicólogo' as profissional_cbo),
        struct(43, 'u', null, null, 43, 10, 'Psicólogo'),
        struct(44, 'u', 'S', date '2026-01-01', 44, 10, 'Psicólogo'),
        struct(45, 'u', 'N', date '2026-01-03', 45, 10, 'Psicólogo'),
        struct(47, 'f', 'N', date '2026-01-03', 47, 20, 'Psicólogo'),
        struct(48, 'f', 'N', date '2026-01-03', 48, 10, 'Educador social'),
        struct(49, 'u', 'N', date '2026-01-01', 49, 10, 'Psicólogo'),
        struct(52, 'u', 'N', date '2026-01-02', 51, 10, 'Psicólogo')
    ]) as evento
),

comparacao as (
    select ordem, to_json_string(e) as linha, 1 as diferenca
    from esperado as e
    union all
    select ordem, to_json_string((select as struct a.* except (
        profissional_id, data_de_atendimento, nome_atendimento_original, unidade_atendimento,
        hora_de_atendimento, id_atendimento, cbo_unico_rank, atendimento_unico_rank
    ))) as linha, -1 as diferenca
    from obtido as a
)

select ordem, linha, sum(diferenca) as saldo
from comparacao
group by ordem, linha
having sum(diferenca) != 0
