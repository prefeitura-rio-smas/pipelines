with adm as (
    select *, 'administrativa' as origem_modulo from {{ ref('raw_evolucoes_administrativas') }}
),
fam as (
    select *, 'familia' as origem_modulo from {{ ref('raw_evolucoes_familias') }}
),
usu as (
    select *, 'usuario' as origem_modulo from {{ ref('raw_evolucoes_usuarios') }}
),

uniao as (
    select id_paciente as id_usuario, id_unidade, id_login as id_profissional, data_evolucao, descricao_evolucao, tipo_evolucao, origem_modulo from adm
    union all
    select null as id_usuario, id_unidade, id_profissional, data_evolucao, descricao_evolucao, tipo_evolucao, origem_modulo from fam
    union all
    select id_usuario, id_unidade, id_profissional, data_evolucao, descricao_evolucao, tipo_evolucao, origem_modulo from usu
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['u.id_usuario', 'u.data_evolucao', 'u.descricao_evolucao']) }} as id_evolucao_sk,
        dim_u.id_usuario_sk,
        dim_p.id_profissional_sk,
        dim_un.id_unidade_sk,
        u.data_evolucao,
        u.descricao_evolucao,
        u.tipo_evolucao,
        u.origem_modulo
    from uniao u
    left join {{ ref('dim_usuarios') }} dim_u on u.id_usuario = dim_u.id_usuario
    left join {{ ref('dim_profissionais') }} dim_p on u.id_profissional = dim_p.id_profissional
    left join {{ ref('dim_unidades') }} dim_un on u.id_unidade = dim_un.id_unidade
)

select * from final
