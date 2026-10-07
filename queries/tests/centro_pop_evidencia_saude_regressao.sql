{{ config(tags = ['daily']) }}

with casos as (
    select
        cast(null as string) as resposta,
        cast(null as bool) as esperado
    union all
    select
        '' as resposta,
        null as esperado
    union all
    select
        'undefined' as resposta,
        null as esperado
    union all
    select
        ' Não Informado ' as resposta,
        null as esperado
    union all
    select
        'Não sabe' as resposta,
        null as esperado
    union all
    select
        'Não se aplica' as resposta,
        null as esperado
    union all
    select
        'N' as resposta,
        false as esperado
    union all
    select
        'Não' as resposta,
        false as esperado
    union all
    select
        'Diabetes' as resposta,
        true as esperado
    union all
    select
        'Clínica da Família' as resposta,
        true as esperado
),

resultado as (
    select
        resposta,
        esperado,
        {{ evidencia_saude('resposta') }} as obtido
    from casos
)

select * from resultado
where obtido is distinct from esperado
