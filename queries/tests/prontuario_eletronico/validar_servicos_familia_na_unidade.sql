{{ config(tags=['planilha_unificada']) }}
-- depends_on: {{ ref('prontuario_eletronico') }}

with familia as (
    select [
        struct(
            20 as id_unidade,
            'Unidade B' as nome_unidade,
            'Serviço B' as nome_servico,
            date '2026-01-01' as data_cadastro,
            cast(null as date) as data_cancelamento
        ),
        struct(cast(null as int64), cast(null as string), 'Serviço sem unidade', date '2026-01-02', cast(null as date))
    ] as servicos
),

unidades as (
    select id_unidade from unnest([10, 20, cast(null as int64)]) as id_unidade
),

candidato as (
    select
        u.id_unidade,
        {{ servicos_da_familia_na_unidade('f.servicos', 'u.id_unidade') }} as servicos
    from familia as f cross join unidades as u
)

select id_unidade
from candidato
where
    array_length(servicos) != if(id_unidade = 20, 1, 0)
    or (id_unidade = 20 and servicos[safe_offset(0)].nome_servico is distinct from 'Serviço B')
