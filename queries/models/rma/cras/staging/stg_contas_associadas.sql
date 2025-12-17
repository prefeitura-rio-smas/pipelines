select
    sequs,
    seqlogin,
    datacesso
from {{ source('cras_rma_prod','gh_contas_us')}}
where datacesso is null