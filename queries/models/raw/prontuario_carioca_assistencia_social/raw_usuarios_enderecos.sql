-- Fonte adicional congelada para usuários sem endereço na foto do BI.
select distinct
    seqpac as id_paciente,
    trim(regexp_replace(dslogradouro, r'[,\s;]+', ' ')) as logradouro,
    trim(regexp_replace(numend, r'[,\s;]+', ' ')) as numero,
    trim(regexp_replace(complend, r'[,\s;]+', ' ')) as complemento,
    trim(regexp_replace(pontorefe, r'[,\s;]+', ' ')) as referencia_ou_comunidade
from {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }}
