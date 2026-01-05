-- Tabela contendo todas as informações de evoluções filtrada para os itens do blcoo II (RMA - CRAS)

-- Tabela para tirar os valores vazios (não há encaminhamentos)
with retirar_vazios_evolucao as (
    select
        sequs,
        aba,
        atividades_smas,
        beneficios,
        orgaos
    from {{ ref('stg_filtro_evolucao') }}
    where atividades_smas is not null
    or beneficios is not null
    or orgaos is not null
),

-- Filtra pelos valores solicitados no relatório do RMA CRAS.
filtro_evolucao as (
    select
        sequs,
        aba,
        atividades_smas,
        beneficios,
        orgaos
    from retirar_vazios_evolucao
    where regexp_contains(atividades_smas, r'(i?)Cadastro/Atualização Cadúnico')
    or regexp_contains(beneficios, r'(i?)BPC - Idoso|BPC - PCD')
    or regexp_contains(orgaos, r'(i?)CREAS')
)

select * from filtro_evolucao

/*
total as (
        select
        sequs,
        count(atividades_smas) as total_atv,
        count(beneficios) as total_benef,
        count(orgaos) as total_org
    from filtro_evolucao
    group by sequs
),
*/