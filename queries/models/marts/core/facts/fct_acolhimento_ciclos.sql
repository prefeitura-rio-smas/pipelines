-- Camada Mart: Fato de acolhimentos institucionais por ciclo (thin)
-- Consome int_acolhimento_ciclos e adiciona a surrogate key e os joins de dimensão.
with ciclos as (
    select * from {{ ref('int_acolhimento_ciclos') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['c.id_ciclo']) }} as id_acolhimento_sk,
    usr.id_usuario_sk,
    un.id_unidade_sk,
    c.id_ciclo,
    c.id_usuario,
    c.id_unidade,
    c.data_entrada,
    c.hora_entrada,
    c.data_saida,
    c.hora_saida,
    c.id_usuario_origem,
    c.id_login_entrada,
    c.id_login_saida,
    c.indicador_ciclo,
    c.motivo_saida,
    c.data_solicitacao,
    c.data_regulacao,
    c.dias_acolhimento,
    c.flag_em_acolhimento
from ciclos as c
left join {{ ref('dim_usuarios') }} as usr on c.id_usuario = usr.id_usuario
left join {{ ref('dim_unidades') }} as un on c.id_unidade = un.id_unidade
