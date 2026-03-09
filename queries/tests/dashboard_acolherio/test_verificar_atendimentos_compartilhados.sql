-- Teste para verificar se a tabela fact_Atendimentos possui algum atendimento compartilhado. (Ela não pode possuir)

select 
    a.seqatend_modulo
from {{ ref('fact_atendimentos') }} a
inner join {{ ref('dim_atendimento_compartilhado') }} b 
on a.seqatend_modulo = b.seqatend_modulo
