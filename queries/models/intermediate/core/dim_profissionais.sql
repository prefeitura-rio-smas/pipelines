with profissionais as (
    select * from {{ ref('raw_profissionais') }}
),
ocupacoes as (
    -- Evitando duplicidade de profissionais que possuem múltiplos CBOs
    select
        id_profissional,
        min(codigo_cbo) as codigo_cbo
    from {{ ref('raw_profissionais_ocupacoes') }}
    group by 1
),
cbo as (
    select * from {{ ref('raw_cbo') }}
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.id_profissional']) }} as id_profissional_sk,
        p.id_profissional,
        p.nome,
        p.cpf,
        p.matricula,
        c.descricao as ocupacao_principal
    from profissionais p
    left join ocupacoes o on p.id_profissional = o.id_profissional
    left join cbo c on o.codigo_cbo = c.codigo_cbo
)
select * from final
