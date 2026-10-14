-- Compara todos os vínculos originais, inclusive cancelados e duplicações.
with origem as (
    select
        id_familia,
        array_agg(to_json_string(struct(
            id_servico_assistencial, id_unidade, id_login_cadastro, data_cadastro, data_cancelamento
        ))) as itens
    from {{ ref('raw_familias_servicos_assistenciais') }}
    group by id_familia
),

comparacao as (
    select
        a.id_atendimento_modulo,
        to_json_string(array(
            select item from unnest(o.itens) as item
            order by item
        )) as origem,
        to_json_string(array(
            select
                to_json_string(struct(
                    item.id_servico_assistencial, item.id_unidade, item.id_login_cadastro,
                    item.data_cadastro, item.data_cancelamento
                )) as assinatura
            from unnest(a.servicos_socioassistenciais_historico) as item
            order by assinatura
        )) as mart
    from {{ ref('mart_atendimentos') }} as a
    left join origem as o on a.id_familia = o.id_familia
)

select * from comparacao
where origem is distinct from mart
