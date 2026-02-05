-- Tabela que contém todos os atendimentos do acolherio. Sem testes.


-- Retirar os usuários que estão repetidos e com famílias diferentes
-- por conta do erro de unificação de relatório do acolherio.
-- O erro altera a família do usuário mas ele ainda permanece na família antiga.

-- Id criado para pegar apenas 1 membro e retirar usuários repetidos.
with criar_id_usuario_repetidos_membros_familia as (
    select 
    *, 
    row_number() over(
    partition  by seqpac order by seqpac desc
    ) as n_usuario
    from {{ ref('stg_membros_familia_acolherio') }}
    where data_saida_membro_familia is null
),

usuarios_unicos_membro_familia as (
    select
        *
    from criar_id_usuario_repetidos_membros_familia
    where n_usuario = 1
),

atendimentos_modulo_familia as (
    select
        sequs,
        seqatend_modulo,
        seqatend,
        seqpac,
        seqfamil,
        seqprof,
        seqlogincad,
        seqtpatend,
        modulo,
        seqprof_atendimento_compartilhado,
        local_atendimento,
        data_atendimento,
        hora_atendimento,
        data_saida_atendimento,
        hora_saida_atendimento
    from  {{ ref('stg_atendimentos_familias') }}
),

-- Incluindo a coluna seqfamil nos atendimentos do módulo usuário
atendimentos_modulo_usuario as (
    select
        a.sequs,
        a.seqatend_modulo,
        a.seqatend,
        a.seqpac,
        b.seqfamil,
        a.seqprof,
        a.seqlogincad,
        a.seqtpatend,
        a.modulo,
        a.seqprof_atendimento_compartilhado,
        a.local_atendimento,
        a.data_atendimento,
        a.hora_atendimento,
        a.data_saida_atendimento,
        a.hora_saida_atendimento
    from {{ ref('stg_atendimentos_usuarios') }} a
    left join usuarios_unicos_membro_familia b on a.seqpac = b.seqpac
),

total_atendimentos as (
    select
        *
    from atendimentos_modulo_familia

    union all

    select
        *
    from atendimentos_modulo_usuario
)

select * from total_atendimentos
