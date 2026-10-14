-- depends_on: {{ ref('mart_planilha_unificada_centro_pop') }}

with casos as (
    select * from unnest([
        struct('s' as resposta, 'Sim' as esperado),
        (' SIM ', 'Sim'),
        ('n', 'Não'),
        (' NÃO ', 'Não'),
        ('nao', 'Não'),
        (null, 'Não Informado'),
        ('', 'Não Informado'),
        ('undefined', 'Não Informado'),
        ('não sabe', 'Não Informado'),
        ('0', 'Não Informado')
    ])
),

resultados as (
    select resposta, esperado, {{ map_resposta_sim_nao('resposta') }} as obtido
    from casos
)

select * from resultados
where obtido is distinct from esperado
