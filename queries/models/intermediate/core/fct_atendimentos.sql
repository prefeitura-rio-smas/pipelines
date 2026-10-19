select
    {{ dbt_utils.generate_surrogate_key(['a.id_atendimento_modulo', 'a.id_profissional']) }} as id_atendimento_sk,
    a.id_atendimento_modulo,
    if(u.id_paciente is not null, {{ dbt_utils.generate_surrogate_key(['a.id_usuario']) }}, null) as id_usuario_sk,
    if(p.id_profissional is not null, {{ dbt_utils.generate_surrogate_key(['a.id_profissional']) }}, null) as id_profissional_sk,
    if(un.id_unidade is not null, {{ dbt_utils.generate_surrogate_key(['a.id_unidade']) }}, null) as id_unidade_sk,
    a.id_usuario,
    a.id_profissional,
    a.id_unidade,
    a.id_atendimento,
    a.id_tipo_atendimento,
    t.tipo_atendimento_descricao,
    a.data_atendimento,
    a.hora_atendimento,
    a.origem_modulo,
    a.flag_cancelado
from {{ ref('int_atendimentos_base') }} as a
left join {{ ref('raw_usuarios') }} as u on a.id_usuario = u.id_paciente and u.nome not like '%TESTE%'
left join {{ ref('raw_profissionais') }} as p on a.id_profissional = p.id_profissional and upper(p.nome) not like '%TESTE%'
left join {{ ref('raw_unidades') }} as un on a.id_unidade = un.id_unidade and un.nome_unidade not like '%TESTE%'
left join {{ ref('raw_tipos_atendimento') }} as t on a.id_tipo_atendimento = t.id_tipo_atendimento
left join {{ ref('raw_operadores') }} as o on a.id_login_cadastro = o.id_login
where o.nome_operador is null or o.nome_operador not like '%TESTE%'
