with base as (
    select * from {{ ref('raw_familias') }}
),
usuarios as (
    select id_usuario_sk, id_usuario, nome as nome_responsavel from {{ ref('dim_usuarios') }}
),
vulnerabilidades as (
    select * from {{ ref('int_vulnerabilidades_agregadas') }}
),
servicos as (
    select * from {{ ref('int_servicos_agregados') }}
),
projetos as (
    select * from {{ ref('int_familias_projetos_sociais') }}
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['base.id_familia']) }} as id_familia_sk,
        base.id_familia,
        base.id_usuario_responsavel,
        u.nome_responsavel,
        base.data_ultima_modificacao,
        base.flag_ativo,
        v.vulnerabilidades,
        s.servicos,
        p.projetos_sociais
    from base
    inner join usuarios u on base.id_usuario_responsavel = u.id_usuario
    left join vulnerabilidades v on base.id_familia = v.id_familia
    left join servicos s on base.id_familia = s.id_familia
    left join projetos p on base.id_familia = p.id_familia
)
select * from final
