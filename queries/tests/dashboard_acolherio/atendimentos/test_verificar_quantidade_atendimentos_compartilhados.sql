-- Teste para verificar se a quantidade de atendimentos no filtro final está correta.

{{ config(store_failures = true) }}

with atendimentos_compartilhados as (
    select
        concat(seqprof, ',', seqprof_atendimento_compartilhado) as prof_atendimentos_compartilhados,
    from {{ ref('int_atendimentos') }}
    where flag_atendimento_compartilhado = "Sim"
),

atendimentos_compartilhados_todos_profissionais as (
    select
        prof_atendimentos_compartilhados,
        ARRAY_LENGTH(SPLIT(prof_atendimentos_compartilhados, ',')) AS total_profissionais_compartilhado
    from atendimentos_compartilhados
),

-- Cte retorna o total de atendimentos compartilhados
-- Exemplo: Atendimento 1 com 2 profissionais. Ela retornará os dois atendimentos.
total_atendimentos_compartilhados_int as (
    select
        sum(total_profissionais_compartilhado) as total_compartilhados_int
    from atendimentos_compartilhados_todos_profissionais
),

-- Cte verifica a quantidade de atendimentos compartilhados no filtro final.
total_atendimentos_compartilhados_mart as(
    select
        count(*) as total_compartilhados_mart
    from {{ ref('dim_atendimento_compartilhado') }}
)

select * 
from total_atendimentos_compartilhados_int int_compartilhados
cross join total_atendimentos_compartilhados_mart mart_compartilhados
where int_compartilhados.total_compartilhados_int != mart_compartilhados.total_compartilhados_mart