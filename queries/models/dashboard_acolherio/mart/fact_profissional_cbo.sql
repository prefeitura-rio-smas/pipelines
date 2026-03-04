with base_table as (
    select
        a.cbo_sk,
        b.seqprof_sk
    from {{ ref('int_cbo') }} a 
    inner join {{ ref('int_profissionais') }} b on a.seqprof = b.seqprof
)

select 
 *   
from base_table 