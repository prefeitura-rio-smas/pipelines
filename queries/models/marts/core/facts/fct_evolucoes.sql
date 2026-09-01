with u as (
    select * from {{ ref('int_evolucoes') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'u.origem_modulo',
            'u.id_evolucao',
            'u.id_usuario'
        ]) }} as id_evolucao_sk,
        dim_u.id_usuario_sk,
        dim_p.id_profissional_sk,
        dim_un.id_unidade_sk,
        u.id_unidade,
        u.data_evolucao,
        u.descricao_evolucao,
        u.tipo_evolucao,
        u.origem_modulo,
        u.id_familia,
        u.modulo_prontuario,
        u.codigo_abrangencia,
        u.id_paciente_familia,
        u.id_atividade,
        u.id_evolucao_grupo,
        u.id_evolucao,
        u.data_cancelamento
    from u
    left join {{ ref('dim_usuarios') }} as dim_u on u.id_usuario = dim_u.id_usuario
    left join {{ ref('dim_profissionais') }} as dim_p on u.id_profissional = dim_p.id_profissional
    left join {{ ref('dim_unidades') }} as dim_un on u.id_unidade = dim_un.id_unidade
)

select * from final
