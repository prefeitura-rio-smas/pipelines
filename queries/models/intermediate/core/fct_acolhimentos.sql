with acolhimentos as (
    select * from {{ ref('raw_usuarios_acolhimentos') }}
),

usuarios as (
    select * from {{ ref('dim_usuarios') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['a.id_ciclo']) }} as id_acolhimento_sk,
        usr.id_usuario_sk,
        un.id_unidade_sk,
        a.id_ciclo,
        a.id_usuario,
        a.id_unidade,
        a.data_entrada,
        a.hora_entrada,
        a.data_saida,
        a.hora_saida,
        a.id_usuario_origem,
        a.id_login_entrada,
        a.id_login_saida,
        a.indicador_ciclo,
        a.motivo_saida,
        date_diff(a.data_saida, a.data_entrada, day) as dias_acolhimento,
        case when a.data_saida is null then 1 else 0 end as flag_em_acolhimento
    from acolhimentos a
    left join usuarios usr on a.id_usuario = usr.id_usuario
    left join unidades un on a.id_unidade = un.id_unidade
)

select * from final
