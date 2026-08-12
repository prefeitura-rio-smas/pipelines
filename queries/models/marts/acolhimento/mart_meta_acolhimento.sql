{{
    config(
        materialized='incremental',
        unique_key=['cpf_digits', 'ano', 'mes', 'origem', 'cpf_status', 'eixo', 'data_particao'],
    )
}}

-- Snapshot mensal de acolhimentos para apuração de meta.
-- Grão: 1 linha por (ano × mes × cpf_digits × origem).
-- Incremental: cada execução adiciona um snapshot com data_particao = current_date().
-- Procedência SMAS (vindo de fct_acolhimento_diaria) ou SMS (Google Sheets externa
--   via raw_sheets_sms_acolhimento).
--
-- Refactor: SMAS NÃO lê mais da `mart_acolhimento_diaria` (acoplamento mart→mart
-- era anti-padrão). Lê direto da `fct_acolhimento_diaria`, que já tem cpf e tipo_publico
-- propagados das dims. SMS migrou de `source(subpse_acolhimento, ...)` direto para
-- `raw_sheets_sms_acolhimento`, mantendo mesma fonte bruta.

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
        'SMAS' as origem,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from {{ ref('fct_acolhimento_diaria') }}
    where extract(year from data_referencia) >= 2024
),

sms as (
    select distinct
        current_date() as data_particao,
        cast(extract(year from s.data_acolhimento) as string) as ano,
        cast(extract(month from s.data_acolhimento) as string) as mes,
        lpad(regexp_replace(coalesce(s.cpf, ''), '[^0-9]', ''), 11, '0') as cpf_digits,
        case
          when regexp_replace(coalesce(s.cpf, ''), '[^0-9]', '') = ''
            then 'cpf_invalido'
          when length(
            lpad(regexp_replace(coalesce(s.cpf, ''), '[^0-9]', ''), 11, '0')
          ) = 11 then 'cpf_ok'
          else 'cpf_invalido'
        end as cpf_status,
        cast(null as string) as eixo,
        'SMS' as origem,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from {{ ref('raw_sheets_sms_acolhimento') }} s
    where s.data_acolhimento is not null
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