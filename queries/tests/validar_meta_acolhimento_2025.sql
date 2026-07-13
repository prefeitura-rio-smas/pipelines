-- Teste: valida CPFs SMAS na mart_meta_acolhimento vs mart_acolhimento_diaria
-- Ambos os lados filtram apenas CPFs de 11 dígitos (cpf_ok)

with meta as (
    select distinct cpf_digits
    from {{ ref('mart_meta_acolhimento') }}
    where ano = '2025'
      and cpf_status = 'cpf_ok'
      and origem = 'SMAS'
),

diaria as (
    select distinct regexp_replace(coalesce(cpf, ''), '[^0-9]', '') as cpf_digits
    from {{ ref('mart_acolhimento_diaria') }}
    where ano = 2025
      and length(regexp_replace(coalesce(cpf, ''), '[^0-9]', '')) = 11
),

so_na_meta as (
    select cpf_digits from meta
    except distinct
    select cpf_digits from diaria
),

so_na_diaria as (
    select cpf_digits from diaria
    except distinct
    select cpf_digits from meta
)

select 'divergencia' as tipo, count(*) as total
from (
    select cpf_digits from so_na_meta
    union all
    select cpf_digits from so_na_diaria
)
having count(*) > 0
