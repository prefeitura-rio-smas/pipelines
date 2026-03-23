-- Tabela para alimentar o BI de atendimentos.

{{ config(materialized='table') }}

with total_atendimentos_dev as (

    select
        seqatend_modulo,
        seqpac_sk,
        seqprof_sk,
        sequs_sk,
        seqtpatend_sk
    from {{ ref('fact_atendimentos') }}

    union all

    select
        seqatend_modulo,
        seqpac_sk,
        seqprof_sk,
        sequs_sk,
        seqtpatend_sk
    from {{ ref('dim_atendimento_compartilhado') }}

),

total_atendimentos_dev_com_seqatend_original as (

    select
        c.cas,
        c.sequs,
        c.unidade,
        a.seqatend_modulo,
        a.seqpac_sk,
        a.seqprof_sk,
        a.sequs_sk,
        a.seqtpatend_sk,
        b.seqtpatend,
        b.seqatend,
        b.modulo,
        b.seqpac,
        b.data_atendimento,
        b.hora_atendimento,
        b.flag_atendimento_compartilhado,
        d.cpf as cpf_profissional,
        d.seqlogin as seqlogin_profissional,
        d.seqprof,
        d.nome_profissional,
        e.nome_usuario,
        e.nome_social,
        e.cpf_com_ponto,
        e.cpf_sem_ponto,
        e.sexo,
        e.orientacao_sexual,
        e.genero,
        e.raca,
        e.estado_civil,
        e.filiacao_mae,
        e.flag_trabalho,
        e.vinculo_trabalhista,
        e.profissao,
        e.flag_atvd_gera_renda,
        e.flag_frequencia_escola,
        e.serie_escola,
        e.escolaridade,
        e.flag_recebe_beneficio,
        e.tipo_beneficio,
        e.flag_deficiencia,
        e.tipo_deficiencia,
        f.codcbo
    from total_atendimentos_dev a
    inner join {{ ref('int_atendimentos_no_row_number') }} b on a.seqatend_modulo = b.seqatend_modulo
    left join {{ ref('dim_unidades') }} c on a.sequs_sk = c.sequs_sk
    left join {{ ref('dim_profissionais') }} d on a.seqprof_sk = d.seqprof_sk
    left join {{ ref('dim_usuarios') }} e on a.seqpac_sk = e.seqpac_sk
    left join  {{ ref('dim_cbo') }} f on a.seqprof_sk = f.seqprof_sk
    
),

tirar_duplicadas as (
    select 
        *,
        row_number() over(
            partition by
                seqprof,
                hora_atendimento,
                seqtpatend,
                seqpac,
                unidade
            order by seqatend asc
        ) as rn_v2
    from total_atendimentos_dev_com_seqatend_original
)

select * from tirar_duplicadas
where not regexp_contains(nome_usuario, '(?i)teste')
