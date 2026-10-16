-- Casos de fronteira independentes da implementação dos modelos.
with casos as (
    select
        date '2026-01-01' as inicio,
        date '2026-01-31' as fim,
        false as esperado
    union all
    select
        date '2026-01-31' as inicio,
        date '2026-02-01' as fim,
        true as esperado
    union all
    select
        date '2026-02-01' as inicio,
        cast(null as date) as fim,
        false as esperado
    union all
    select
        date '2025-12-01' as inicio,
        cast(null as date) as fim,
        true as esperado
    union all
    select
        cast(null as date) as inicio,
        cast(null as date) as fim,
        false as esperado
)

select
    inicio,
    fim
from casos
where {{ vigencia_no_fechamento('inicio', 'fim', "date '2026-01-01'") }} is distinct from esperado
