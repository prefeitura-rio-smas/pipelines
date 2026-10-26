{{ config(tags = ['planilha_unificada']) }}

-- A chave do evento inclui o módulo. Um atendimento não deve pertencer
-- a vários usuários ou aparecer repetido dentro do array de uma pessoa.
select a.id_atendimento_modulo
from {{ ref('planilha_unificada') }} as p,
    unnest(p.atendimentos.registros) as a
group by a.id_atendimento_modulo
having nullif(trim(a.id_atendimento_modulo), '') is null or count(*) > 1
