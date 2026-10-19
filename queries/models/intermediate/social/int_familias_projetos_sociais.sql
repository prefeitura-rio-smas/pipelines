{{ config(materialized="ephemeral") }}

-- Projetos sociais agregados por família
with base as (
    select
        *,
        {{ map_nome_projeto_social('id_projeto_social') }} as nome_projeto_social
    from {{ ref('raw_familias_projetos_sociais') }}
),

projetos as (
    select
        id_familia,
        array_agg(struct(
            id_projeto_social,
            id_unidade,
            indicador_ativo,
            data_cadastro,
            data_cancelamento
        )) as projetos_sociais,
        string_agg(
            distinct if(indicador_ativo = 'S' and data_cancelamento is null, nome_projeto_social, null),
            ', ' order by if(indicador_ativo = 'S' and data_cancelamento is null, nome_projeto_social, null)
        ) as projetos_sociais_descricao,
        array_agg(struct(
            id_projeto_social, nome_projeto_social, id_unidade, id_login_cadastro,
            data_cadastro, data_cancelamento, indicador_ativo
        ) order by id_projeto_social, data_cadastro) as projetos_sociais_historico
    from base
    group by id_familia
)
select * from projetos
