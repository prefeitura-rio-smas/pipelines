-- Teste: valida a lógica de unificação de capacidade das unidades
-- Unidades com flag_administra_leitos = 'S' devem ter total_vagas vindo da soma vagas_*
-- Unidades sem auto-gestão devem ter total_vagas = total_leitos

with int_capacidade as (
    select * from {{ ref('int_capacidade_unidades') }}
),

validacao as (
    select
        id_unidade,
        total_vagas,
        total_leitos,
        flag_administra_leitos,
        vagas_homens,
        vagas_mulheres,
        vagas_neutras,
        case
            when flag_administra_leitos = 'S'
             and total_vagas != coalesce(vagas_homens, 0) + coalesce(vagas_mulheres, 0) + coalesce(vagas_neutras, 0)
                then 'ERRO: total_vagas difere da soma vagas_* do gh_us_smas'
            when flag_administra_leitos = 'S'
             and total_vagas = 0
                then 'ERRO: unidade auto-gestão com 0 vagas'
            when (flag_administra_leitos = 'N' or flag_administra_leitos is null)
             and total_vagas != coalesce(total_leitos, 0)
                then 'ERRO: total_vagas difere de total_leitos do gh_us_config'
            when total_vagas < vagas_disponiveis
                then 'ERRO: vagas_disponiveis maior que total_vagas'
        end as status
    from int_capacidade
)

select * from validacao
where status is not null
