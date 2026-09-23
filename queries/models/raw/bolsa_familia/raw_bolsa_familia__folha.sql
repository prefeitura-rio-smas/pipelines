-- Bronze: exposição tipada da tabela de ingestão do Bolsa Família.
-- O parsing do layout fica na camada intermediate.

select
    linha_bruta,
    timestamp_captura,
    data_particao
from {{ source('bolsa_familia_staging', 'folha') }}
