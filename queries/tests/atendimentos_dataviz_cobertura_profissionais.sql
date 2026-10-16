-- Cada grupo elegível deve continuar representado, inclusive quando o
-- profissional é compartilhado com atendimentos de principais diferentes.
with esperados as (
    select distinct
        if(p.flag_cadastrado, p.id_profissional, null) as id_profissional,
        a.hora_de_atendimento as hora,
        a.data_de_atendimento as data_atendimento,
        a.nome_atendimento_original as nome_atendimento,
        a.id_usuario,
        a.unidade_atendimento as unidade
    from {{ ref('int_atendimentos_dataviz') }} as a,
        unnest(a.profissionais) as p
    where a.flag_elegivel_dataviz
),

publicados as (
    select
        profissional_id as id_profissional,
        hora_de_atendimento as hora,
        data_de_atendimento as data_atendimento,
        nome_atendimento_original as nome_atendimento,
        id_usuario,
        unidade_atendimento as unidade
    from {{ ref('mart_atendimentos') }}
)

select * from esperados
except distinct
select * from publicados
