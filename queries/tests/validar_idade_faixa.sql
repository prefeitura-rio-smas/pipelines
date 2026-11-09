-- Fixtures posicionais nas linhas UNION ALL.
-- noqa: disable=AL03
{{ config(tags=['planilha_unificada']) }}
with casos as (
    select
        17 as idade,
        'Até 17 anos' as esperado
    union all
    select
        18,
        'De 18 a 29 anos'
    union all
    select
        29,
        'De 18 a 29 anos'
    union all
    select
        30,
        'De 30 a 44 anos'
    union all
    select
        44,
        'De 30 a 44 anos'
    union all
    select
        45,
        'De 45 a 59 anos'
    union all
    select
        59,
        'De 45 a 59 anos'
    union all
    select
        60,
        'De 60 a 74 anos'
    union all
    select
        74,
        'De 60 a 74 anos'
    union all
    select
        75,
        'Mais de 75 anos'
    union all
    select
        cast(null as int64),
        'Não Informado'
)

select * from casos
where {{ idade_faixa('idade') }} != esperado
