-- Contexto de compatibilidade da planilha, não novas medidas de atendimento.
-- O número sequencial de apresentação não faz parte do dado socioassistencial.
select
    p.id_atendimento,
    p.id_usuario,
    p.id_unidade,
    p.data_atendimento,
    array_agg((select as struct p.* except (numero)) order by p.numero) as planilha_centro_pop
from {{ ref('int_planilha_unificada_centro_pop') }} as p
group by p.id_atendimento, p.id_usuario, p.id_unidade, p.data_atendimento
