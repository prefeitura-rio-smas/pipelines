with casos as (
    select
        '29/02/2024' as valor,
        date '2024-02-29' as esperado
    union all
    select
        '2024-02-29 12:30:00' as valor,
        date '2024-02-29' as esperado
    union all
    select
        '29/02/2024 12:30' as valor,
        date '2024-02-29' as esperado
    union all
    select
        '29/02/2023' as valor,
        cast(null as date) as esperado
    union all
    select
        '02/2024' as valor,
        cast(null as date) as esperado
    union all
    select
        'data: 29/02/2024' as valor,
        cast(null as date) as esperado
    union all
    select
        '' as valor,
        cast(null as date) as esperado
)

select * from casos where {{ data_ocorrencia_parse('valor') }} is distinct from esperado
