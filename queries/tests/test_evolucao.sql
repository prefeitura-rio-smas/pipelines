-- Teste para verificar se não foi capturado algum dado do encaminhamento à atividades SMAS.
with filtro_titulo as (
select
    dscevopac,
    regexp_contains(dscevopac, r'(?i)Encaminhamentos - Atividades SMAS') as flag_atv_smas,
    regexp_contains(dscevopac, r'(?i)Encaminhamentos - Benefícios') as flag_benef,
    regexp_contains(dscevopac, r'(?i)Encaminhamentos Órgãos') as flag_orgaos,
    atividades_smas,
    beneficios,
    orgaos
from {{ref('stg_filtro_evolucao')}}
where atividades_smas is not null
and beneficios is not null
and orgaos is not null
),

total_ocorrencias_smas as (
select 
    count(atividades_smas) as total_atv_smas,
    count(flag_atv_smas) as total_flag_atv_smas,
    count(beneficios) as total_benef,
    count(flag_benef) as total_flag_benef,
    count(orgaos) as total_orgaos,
    count(flag_orgaos) as total_flag_orgaos 
from filtro_titulo
),

condicao_ocorrencias as (
    select
        total_atv_smas,
        total_flag_atv_smas,
        total_benef,
        total_flag_benef,
        total_orgaos,
        total_flag_orgaos,
        case
            when total_atv_smas != total_flag_atv_smas
            then 'Não válido'
            else 'Válido'
        end as verificacao_atv_smas,
        case
            when total_benef != total_flag_benef
            then 'Não válido'
            else 'Válido'
        end as verificacao_benef,
        case
            when total_orgaos != total_flag_orgaos
            then 'Não válido'
            else 'Válido'
        end as verificacao_orgaos
    from total_ocorrencias_smas
)

select * from condicao_ocorrencias
where verificacao_atv_smas != 'Válido'
or verificacao_atv_smas != 'Válido'
or verificacao_orgaos != 'Válido'