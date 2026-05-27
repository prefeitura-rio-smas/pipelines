with acolhimentos as (
    select * from {{ ref('fct_acolhimentos') }}
),

usuarios as (
    select * from {{ ref('dim_usuarios') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

explodido as (
    select
        a.id_acolhimento_sk,
        a.id_usuario_sk,
        a.id_unidade_sk,
        a.id_usuario,
        usr.nome as nome_usuario,
        usr.cpf,
        case when coalesce(usr.cpf, '') = '' then 'Não' else 'Sim' end as flag_cpf,
        a.id_ciclo,
        a.data_entrada,
        a.data_saida,
        a.dias_acolhimento,
        a.flag_em_acolhimento,
        a.indicador_ciclo,
        a.motivo_saida,
        un.id_unidade as id_unidade,
        un.nome_unidade,
        un.nome_tipo,
        un.classe,
        un.total_vagas,
        un.vagas_disponiveis,
        un.vagas_homens,
        un.vagas_mulheres,
        un.vagas_neutras,
        un.tipo_publico,
        data_referencia
    from acolhimentos a
    left join usuarios usr on a.id_usuario_sk = usr.id_usuario_sk
    left join unidades un on a.id_unidade_sk = un.id_unidade_sk
    cross join unnest(
        generate_date_array(
            a.data_entrada,
            coalesce(a.data_saida, current_date())
        )
    ) as data_referencia
)

select
    *,
    extract(year from data_referencia) as ano
from explodido
