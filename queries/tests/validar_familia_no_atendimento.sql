{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with referencia as (
    select * from unnest([
        struct(
            1 as id_usuario, 1 as seq_atendimento, 'u' as modulo,
            10 as id_familia, 'permitido@example.test' as email
        ),
        (1, 1, 'u', 10, 'permitido@example.test'),
        (1, 1, 'u', cast(null as int64), 'permitido@example.test'),
        (1, 1, 'f', 20, 'permitido@example.test'),
        (2, 2, 'u', cast(null as int64), cast(null as string)),
        (3, 3, 'f', 30, 'outro@example.test')
    ])
),

contexto as (
    {{ pu_familia_no_atendimento_sql('referencia') }}
),

obtido as (
    select
        to_json_string(c) as conteudo,
        1 as delta
    from contexto as c
    union all
    select
        to_json_string(e) as conteudo,
        -1 as delta
    from unnest([
        struct(
            1 as id_usuario, '1u' as id_atendimento_modulo,
            '10' as id_familia, 'permitido@example.test' as email
        ),
        (1, '1f', '20', 'permitido@example.test'),
        (2, '2u', '', ''),
        (3, '3f', '30', 'outro@example.test')
    ]) as e
)

select conteudo from obtido
group by conteudo
having sum(delta) != 0
