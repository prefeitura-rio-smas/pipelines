-- Camada Intermediate: Acolhimentos no grao (id_ciclo x data_referencia).
-- Faz a explosao temporal via GENERATE_DATE_ARRAY (cada dia = 1 linha por ciclo)
-- sobre int_acolhimento_ciclos. A fct_acolhimento_diaria (mart) adiciona as
-- surrogate keys e os joins com dim_usuarios (cpf) e dim_unidades (tipo_publico).
-- Filtro: data_referencia <= current_date() (descarta datas futuras de ciclos em aberto
--   e ciclos corrompidos com data_entrada ~0202).

with ciclos as (
    select * from {{ ref('int_acolhimento_ciclos') }}
),

explodido as (
    select
        c.id_ciclo,
        c.id_usuario,
        c.id_unidade,
        c.data_entrada,
        c.data_saida,
        c.data_solicitacao,
        c.data_regulacao,
        c.dias_acolhimento,
        c.flag_em_acolhimento,
        c.indicador_ciclo,
        c.motivo_saida,
        data_referencia
    from ciclos c
    cross join unnest(
        generate_date_array(
            c.data_entrada,
            coalesce(c.data_saida, current_date())
        )
    ) as data_referencia
    where data_referencia <= current_date()
)

select * from explodido
