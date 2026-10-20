select
    {{ dbt_utils.generate_surrogate_key(['a.id_atendimento_modulo', 'a.id_profissional']) }} as id_atendimento_sk,
    a.id_atendimento_modulo,
    if(
        e.flag_usuario_cadastrado and e.nome_usuario not like '%TESTE%',
        {{ dbt_utils.generate_surrogate_key(['a.id_usuario']) }}, null
    ) as id_usuario_sk,
    if(
        p.flag_cadastrado and upper(p.nome_cadastral) not like '%TESTE%',
        {{ dbt_utils.generate_surrogate_key(['a.id_profissional']) }}, null
    ) as id_profissional_sk,
    if(
        e.flag_unidade_cadastrada and e.unidade_atendimento not like '%TESTE%',
        {{ dbt_utils.generate_surrogate_key(['a.id_unidade']) }}, null
    ) as id_unidade_sk,
    a.id_usuario,
    a.id_profissional,
    a.id_unidade,
    a.id_atendimento,
    a.id_tipo_atendimento,
    e.nome_atendimento_original as tipo_atendimento_descricao,
    a.data_atendimento,
    a.hora_atendimento,
    a.origem_modulo,
    a.flag_cancelado
from {{ ref('int_atendimentos_base') }} as a
inner join {{ ref('int_atendimentos_enriquecidos') }} as e
    on a.id_atendimento_modulo = e.id_atendimento_modulo
inner join unnest(e.profissionais) as p
    on a.id_profissional is not distinct from p.id_profissional
where e.flag_elegivel_fato
