{{
    config(
        materialized='incremental',
        unique_key=['cpf_digits', 'ano', 'mes', 'origem', 'data_particao'],
    )
}}

with smas as (
    select distinct
        current_date() as data_particao,
        cast(extract(year from data_referencia) as string) as ano,
        cast(extract(month from data_referencia) as string) as mes,
        regexp_replace(coalesce(cpf, ''), '[^0-9]', '') as cpf_digits,
        case when length(regexp_replace(coalesce(cpf, ''), '[^0-9]', '')) = 11
             then 'cpf_ok' else 'cpf_vazio' end as cpf_status,
        case
            when tipo_publico like '%A%' or tipo_publico like '%F%' then 'ADULTO/FAMÍLIA'
            when tipo_publico like '%I%' then 'IDOSO'
        end as eixo,
        'SMAS' as origem
    from {{ ref('mart_acolhimento_diaria') }}
    where extract(year from data_referencia) >= 2024
),

sms as (
    select distinct
        current_date() as data_particao,
        cast(extract(year from safe.parse_date('%d/%m/%Y', s.DATA_ACOLHIMENTO)) as string) as ano,
        cast(extract(month from safe.parse_date('%d/%m/%Y', s.DATA_ACOLHIMENTO)) as string) as mes,
        lpad(regexp_replace(coalesce(s.CPF, ''), '[^0-9]', ''), 11, '0') as cpf_digits,
        case
          when regexp_replace(coalesce(s.CPF, ''), '[^0-9]', '') = ''
            then 'cpf_invalido'
          when length(
            lpad(regexp_replace(coalesce(s.CPF, ''), '[^0-9]', ''), 11, '0')
          ) = 11 then 'cpf_ok'
          else 'cpf_invalido'
        end as cpf_status,
        cast(null as string) as eixo,
        'SMS' as origem
    from {{ source('subpse_acolhimento', 'consolidado_sms_acolhimento_2026') }} s
)

select * from smas
union all
select * from sms

{% if is_incremental() %}
    where not exists (
        select 1 from {{ this }}
        where data_particao = current_date()
    )
{% endif %}

order by ano, mes, cpf_digits
