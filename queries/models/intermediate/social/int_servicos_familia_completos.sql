with servicos as (
    select
        *,
        {{ nome_servico_socioassistencial('id_servico_assistencial') }} as nome_servico_socioassistencial
    from {{ ref('raw_familias_servicos_assistenciais') }}
)

select
    id_familia,
    string_agg(
        distinct if(data_cancelamento is null, nome_servico_socioassistencial, null),
        ', ' order by if(data_cancelamento is null, nome_servico_socioassistencial, null)
    ) as servicos_socioassistenciais,
    array_agg(struct(
        id_servico_assistencial,
        nome_servico_socioassistencial,
        id_unidade,
        id_login_cadastro,
        data_cadastro,
        data_cancelamento
    ) order by id_servico_assistencial, data_cadastro, data_cancelamento, id_unidade, id_login_cadastro)
        as servicos_socioassistenciais_historico
from servicos
group by id_familia
