-- Camada Mart: Fato de acolhimentos no grão (id_ciclo × data_referencia) (thin)
-- Consome int_acolhimento_diaria e adiciona a surrogate key, os joins de
-- dimensão e os campos cpf/tipo_publico esperados pelos consumidores.
with diaria as (
    select * from {{ ref('int_acolhimento_diaria') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['d.id_ciclo']) }} as id_acolhimento_sk,
    usr.id_usuario_sk,
    un.id_unidade_sk,
    d.id_ciclo,
    d.id_usuario,
    d.id_unidade,
    d.data_entrada,
    d.data_saida,
    d.data_solicitacao,
    d.data_regulacao,
    d.dias_acolhimento,
    d.flag_em_acolhimento,
    d.indicador_ciclo,
    d.motivo_saida,
    usr.cpf,
    un.tipo_publico,
    d.data_referencia
from diaria as d
left join {{ ref('dim_usuarios') }} as usr on d.id_usuario = usr.id_usuario
left join {{ ref('dim_unidades') }} as un on d.id_unidade = un.id_unidade
