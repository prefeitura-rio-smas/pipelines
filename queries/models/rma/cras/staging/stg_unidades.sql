select 
    sequs,
    dscus as unidade
from {{ source('cras_rma_prod', 'gh_us')}}