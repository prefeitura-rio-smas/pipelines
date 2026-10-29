with base as (
    select * from {{ ref('raw_familias_servicos_assistenciais') }}
),

agregado as (
    select
        id_familia,
        array_agg(
            struct(
                id_servico_assistencial,
                data_cadastro,
                id_login_cadastro as id_profissional
            )
        ) as servicos
    from base
    group by id_familia
),

servicos_distintos as (
    select distinct
        s.id_familia,
        s.id_servico_assistencial,
        s.id_unidade,
        s.id_login_cadastro,
        s.data_cadastro,
        s.data_cancelamento,
        un.nome_unidade
    from base as s
    left join {{ ref('dim_unidades') }} as un on s.id_unidade = un.id_unidade
),

servicos as (
    select
        id_familia,
        array_agg(struct(
            id_servico_assistencial,
            id_unidade,
            nome_unidade,
            id_login_cadastro,
            data_cadastro,
            data_cancelamento,
            {{ map_nome_servico_assistencial('id_servico_assistencial') }} as nome_servico
        ) order by data_cadastro, id_servico_assistencial, id_unidade, data_cancelamento, id_login_cadastro) as servicos
    from servicos_distintos
    group by id_familia
)

select
    a.*,
    coalesce(h.servicos, []) as servicos_historico
from agregado as a left join servicos as h on a.id_familia = h.id_familia
