-- Teste para verificar se a quantidade de atendimentos no filtro final está correta.

with atendimentos_compartilhados as (
    select
    seqprof_atendimento_compartilhado,
    ARRAY_LENGTH(SPLIT(seqprof_atendimento_compartilhado, ',')) AS total_profissionais_compartilhado
    from rj-smas-dev.relatorio.int_atendimentos
    where seqprof_atendimento_compartilhado != ''
),

-- Cte retorna o total de atendimentos compartilhados
-- Exemplo: Atendimento 1 com 2 profissionais. Ela retornará os dois atendimentos.
total_atendimentos_compartilhados_int as (
    select
        sum(total_profissionais_compartilhado) as total_compartilhados_int
    from atendimentos_compartilhados
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