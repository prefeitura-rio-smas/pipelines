{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with casos as (
    select [struct('B' as nome_unidade, ' A@EXAMPLE.TEST,b@example.test' as email), ('A', 'b@example.test;\na@example.test'), ('B', '')] as registros
),

obtido as (
    select
        {{ consolidar_unidades('registros') }} as unidades,
        {{ consolidar_emails('registros') }} as email
    from casos
)

select 'consolidacao' as falha from obtido
where unidades != 'A, B' or email != 'a@example.test, b@example.test'
union all
select 'ausencia' as falha from casos
where {{ consolidar_emails('array<struct<nome_unidade string, email string>>[]') }} != ''
