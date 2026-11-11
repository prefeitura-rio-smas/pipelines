with linhas as (
    select
        id_usuario,
        concat(cast(seq_atendimento as string), modulo) as evento
    from {{ ref('int_atendimentos_bi') }}
    where tipo_atendimento is distinct from {{ classificar_atendimento('nome_atendimento_original', 'profissional', 'profissional_cbo') }}
),

eventos as (
    select
        cast(p.id_usuario as string) as id_usuario,
        a.id_atendimento_modulo as evento
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    where a.classificacao_atendimento is distinct from (
        select
            case
                when countif(pr.classificacao_atendimento = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
                when countif(pr.classificacao_atendimento = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
                else 'Outros Atendimentos'
            end
        from unnest(a.profissionais) as pr
    )
)

select
    cast(id_usuario as string) as id_usuario,
    evento
from linhas
union all
select
    id_usuario,
    evento
from eventos
