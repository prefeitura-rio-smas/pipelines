{{ config(materialized='table') }}

with familias as (
    select * from {{ ref('dim_familias') }}
),

vulnerabilidades_agregadas as (
    select * from {{ ref('int_vulnerabilidades_agregadas') }}
),

servicos_agregados as (
    select * from {{ ref('int_servicos_agregados') }}
),

indicadores as (
    select
        f.id_familia_sk as sk_familia,
        f.id_familia,
        f.nome_responsavel,
        f.data_ultima_modificacao,
        f.flag_ativo,
        coalesce(array_length(v.vulnerabilidades), 0) as qtd_vulnerabilidades,
        coalesce(array_length(s.servicos), 0) as qtd_servicos,
        case
            when coalesce(array_length(v.vulnerabilidades), 0) > 0
                then 'Sim'
            else 'Não'
        end as possui_vulnerabilidade,
        case
            when coalesce(array_length(s.servicos), 0) > 0
                then 'Sim'
            else 'Não'
        end as possui_servico
    from familias f
    left join vulnerabilidades_agregadas v
        on f.id_familia = v.id_familia
    left join servicos_agregados s
        on f.id_familia = s.id_familia
)

select * from indicadores
