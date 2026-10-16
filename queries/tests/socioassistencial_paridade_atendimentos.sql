-- Paridade com o contrato por usuário do PR #147, inclusive compartilhados,
-- cancelamentos, inelegíveis e ranks. COUNT também identifica fanout por joins.
with anterior as (
    select
        a.id_atendimento_modulo,
        u.id_usuario,
        a.id_unidade,
        a.id_familia,
        a.flag_cancelado,
        a.flag_elegivel_dataviz,
        to_json_string(a.profissionais) as equipe
    from {{ ref('mart_atendimentos') }} as u,
        unnest(u.atendimentos) as a
),

nova as (
    select
        atendimento.id_atendimento_modulo,
        id_usuario,
        id_unidade,
        id_familia,
        atendimento.flag_cancelado,
        atendimento.flag_elegivel_dataviz,
        to_json_string(atendimento.profissionais) as equipe
    from {{ ref('mart_socioassistencial_unificada') }}
    where tipo_registro = 'ATENDIMENTO'
)

select coalesce(a.id_atendimento_modulo, n.id_atendimento_modulo) as id_atendimento_modulo
from anterior as a
full outer join nova as n on a.id_atendimento_modulo = n.id_atendimento_modulo
where
    a.id_atendimento_modulo is null or n.id_atendimento_modulo is null
    or a.id_usuario is distinct from n.id_usuario
    or a.id_unidade is distinct from n.id_unidade
    or a.id_familia is distinct from n.id_familia
    or a.flag_cancelado is distinct from n.flag_cancelado
    or a.flag_elegivel_dataviz is distinct from n.flag_elegivel_dataviz
    or a.equipe is distinct from n.equipe
