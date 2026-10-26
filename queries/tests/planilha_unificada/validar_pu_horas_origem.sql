{{ config(tags = ['planilha_unificada'], severity = 'warn') }}

-- Horas inválidas ficam vazias na saída, com a origem preservada para auditoria.
select
    p.id_usuario,
    a.id_atendimento_modulo,
    a.hora_atendimento
from {{ pu_ref('int_pu_atendimentos') }} as p,
    unnest(p.atendimentos) as a
where
    nullif(trim(a.hora_atendimento), '') is not null
    and {{ normalizar_hora_atendimento('a.hora_atendimento') }} = ''
