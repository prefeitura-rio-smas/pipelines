-- Fixtures posicionais nas linhas UNION ALL.
-- noqa: disable=AL03
{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with referencia as (
    select * from unnest([
        struct(1 as id_usuario, 1 as seq_atendimento, 'u' as modulo, 10 as id_familia),
        (1, 1, 'u', 10),
        (1, 1, 'u', cast(null as int64)),
        (1, 1, 'f', 20),
        (2, 2, 'u', cast(null as int64)),
        (3, 3, 'f', 30),
        (4, 4, 'u', 54771),
        (4, 4, 'u', 54424)
    ])
),

contexto as (
    select
        id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        {{ pu_ids_familias('id_familia') }} as id_familia
    from referencia
    group by id_usuario, id_atendimento_modulo
),

comparacao as (
    select
        to_json_string(c) as conteudo,
        1 as delta
    from contexto as c
    union all
    select
        to_json_string(e) as conteudo,
        -1 as delta
    from unnest([
        struct(1 as id_usuario, '1u' as id_atendimento_modulo, '10' as id_familia),
        (1, '1f', '20'),
        (2, '2u', ''),
        (3, '3f', '30'),
        (4, '4u', '54424, 54771')
    ]) as e
)

select conteudo from comparacao
group by conteudo
having sum(delta) != 0
