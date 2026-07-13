{{ config(materialized="ephemeral") }}

-- Projetos sociais agregados por família
with projetos as (
    select
        id_familia,
        array_agg(struct(
            id_projeto_social,
            indicador_ativo,
            data_cadastro,
            data_cancelamento
        )) as projetos_sociais
    from {{ ref('raw_familias_projetos_sociais') }}
    group by id_familia
)
select * from projetos
