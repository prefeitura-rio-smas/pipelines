select
    seqpac,
    dscnomepac,
    indsexo,
    racacor
from {{source('cras_rma_prod', 'gh_cidadao_pac')}}
    
0