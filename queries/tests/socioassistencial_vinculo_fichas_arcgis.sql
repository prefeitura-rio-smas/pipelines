-- O relacionamento foi verificado contra as chaves da fonte: objectid não é
-- a chave referenciada pelo parentrowid. Não descartar as ocorrências órfãs.
with fichas as (
    select
        globalid,
        lower(trim(uniquerowid)) as chave_ficha
    from {{ ref('abordagem_ficha') }}
    where nullif(trim(uniquerowid), '') is not null
    qualify count(*) over (partition by lower(trim(uniquerowid))) = 1
),

registros as (
    select
        id_registro,
        abordagem.parentrowid,
        abordagem.id_ficha
    from {{ ref('mart_socioassistencial_unificada') }}
    where tipo_registro = 'ABORDAGEM'
)

select r.id_registro
from registros as r
left join fichas as f on lower(trim(r.parentrowid)) = f.chave_ficha
where r.id_ficha is distinct from f.globalid
