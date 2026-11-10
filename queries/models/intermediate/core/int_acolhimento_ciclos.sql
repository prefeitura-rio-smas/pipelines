-- Camada Intermediate: Acolhimentos institucionais, grão: 1 linha por id_ciclo
-- (sem explosao temporal). Input das fcts `fct_acolhimento_ciclos` e
-- `fct_acolhimento_diaria`; as surrogate keys e joins de dimensão ficam nas marts.
-- `dias_acolhimento` segue contagem inclusiva em ambos extremos (DATE_DIFF + 1),
--   padrão AcolheRio onde cada dia = 1 ciclo.

with acolhimentos as (
    select * from {{ ref('raw_usuarios_acolhimentos') }}
),

datas_solicitacao as (
    select * from {{ ref('int_solicitacao') }}
),

datas_regulacao as (
    select * from {{ ref('int_regulacao') }}
),

final as (
    select
        a.id_ciclo,
        a.id_usuario,
        a.id_unidade,
        a.data_entrada,
        a.hora_entrada,
        a.data_saida,
        a.hora_saida,
        a.id_usuario_origem,
        a.id_login_entrada,
        a.id_login_saida,
        a.indicador_ciclo,
        a.motivo_saida,
        solicitacao.data_solicitacao,
        regulacao.data_regulacao,
        (date_diff(a.data_saida, a.data_entrada, day) + 1) as dias_acolhimento,
        case when a.data_saida is null then 1 else 0 end as flag_em_acolhimento
    from acolhimentos as a
    left join datas_solicitacao as solicitacao
        on a.id_ciclo = solicitacao.id_ciclo
    left join datas_regulacao as regulacao
        on a.id_ciclo = regulacao.id_ciclo
    where a.data_entrada <= current_date()
)

select * from final
