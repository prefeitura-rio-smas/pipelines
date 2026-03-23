with base_table as (
    select
        a.seqprof_sk as seqprof_sk_cbo,
        b.seqprof_sk
    from {{ ref('int_cbo') }} a 
    inner join {{ ref('int_profissionais') }} b on a.seqprof = b.seqprof
)

select 
 *   
from base_table 