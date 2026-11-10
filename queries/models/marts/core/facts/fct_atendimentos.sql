-- Camada Mart: Fato de atendimentos (thin)
-- Consome int_atendimentos e adiciona a surrogate key e os joins de dimensão.
with atendimentos as (
    select * from {{ ref('int_atendimentos') }}
)

select
    -- A chave inclui o módulo e o profissional para preservar o grão do fato.
    {{ dbt_utils.generate_surrogate_key(['a.id_atendimento_modulo', 'a.id_profissional']) }} as id_atendimento_sk,
    a.id_atendimento_modulo,
    usr.id_usuario_sk,
    prof.id_profissional_sk,
    un.id_unidade_sk,

    -- Identificadores da origem.
    a.id_usuario,
    a.id_profissional,
    a.id_unidade,
    a.id_atendimento,
    a.id_tipo_atendimento,

    a.tipo_atendimento_descricao,

    a.data_atendimento,
    a.hora_atendimento,
    a.origem_modulo,
    a.flag_cancelado
from atendimentos as a
left join {{ ref('dim_usuarios') }} as usr on a.id_usuario = usr.id_usuario
left join {{ ref('dim_profissionais') }} as prof on a.id_profissional = prof.id_profissional
left join {{ ref('dim_unidades') }} as un on a.id_unidade = un.id_unidade
