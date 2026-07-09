-- Fato de acolhimentos no grao (id_ciclo x data_referencia).
-- Faz a explosao temporal via GENERATE_DATE_ARRAY (cada dia = 1 linha por ciclo).
-- JOINs com dim_usuarios/dim_unidades para expor cpf e tipo_publico,
--   que sao os unicos campos downstream (mart_acolhimento_diaria e mart_meta_acolhimento)
--   precisam alem das chaves de ciclo/datas.
-- Filtro: data_referencia <= current_date() (descarta datas futuras de ciclos em aberto
--   e ciclos corrompidos com data_entrada ~0202).

with ciclos as (
    select * from {{ ref('fct_acolhimento_ciclos') }}
),

usuarios as (
    select * from {{ ref('dim_usuarios') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

explodido as (
    select
        c.id_acolhimento_sk,
        c.id_usuario_sk,
        c.id_unidade_sk,
        c.id_ciclo,
        c.id_usuario,
        c.id_unidade,
        c.data_entrada,
        c.data_saida,
        c.dias_acolhimento,
        c.flag_em_acolhimento,
        c.indicador_ciclo,
        c.motivo_saida,
        usr.cpf,
        un.tipo_publico,
        data_referencia
    from ciclos c
    left join usuarios usr on c.id_usuario_sk = usr.id_usuario_sk
    left join unidades un  on c.id_unidade_sk = un.id_unidade_sk
    cross join unnest(
        generate_date_array(
            c.data_entrada,
            coalesce(c.data_saida, current_date())
        )
    ) as data_referencia
    where data_referencia <= current_date()
)

select * from explodido