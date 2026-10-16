{{ config(alias='atendimentos', tags=['acolherio_atendimentos_refresh']) }}

-- noqa: disable=CP02,AL09,ST06
-- Contrato V2: atendimento × profissional, com o cadastro social atual.
with profissionais as (
    select
        a.* except (FLAG_ELEGIVEL_DATAVIZ) replace (
            -- A V2 expõe NULL quando o profissional não existe no cadastro.
            if(p.flag_cadastrado, p.id_profissional, null) as PROFISSIONAL_ID,
            p.nome as PROFISSIONAL,
            p.cbo_original as PROFISSIONAL_CBO_ORIGINAL,
            p.cbo as PROFISSIONAL_CBO,
            p.tipo_atendimento as TIPO_ATENDIMENTO,
            p.nome_atendimento as NOME_ATENDIMENTO,
            concat(a.SEQ_ATENDIMENTO, '-', if(p.flag_cadastrado, p.id_profissional, null)) as ID_ATENDIMENTO,
            {{ dbt_utils.generate_surrogate_key(['a.ID_ATENDIMENTO_MODULO', 'p.id_profissional']) }} as ID_ATENDIMENTO_SK,
            p.cbos as PROFISSIONAL_CBOS,
            p.nome_cadastral as PROFISSIONAL_NOME_CADASTRAL,
            p.flag_cadastrado as FLAG_PROFISSIONAL_CADASTRADO,
            p.flag_teste as FLAG_PROFISSIONAL_TESTE
        ),
        p.flag_principal as FLAG_PROFISSIONAL_PRINCIPAL
    from {{ ref('int_atendimentos_dataviz') }} as a,
        unnest(a.PROFISSIONAIS) as p
    where a.FLAG_ELEGIVEL_DATAVIZ
)

select
    *,
    -- O CBO já foi reduzido a um valor determinístico na intermediate.
    1 as cbo_unico_rank,
    row_number() over (
        partition by
            PROFISSIONAL_ID, HORA_DE_ATENDIMENTO, DATA_DE_ATENDIMENTO,
            NOME_ATENDIMENTO_ORIGINAL, ID_USUARIO, UNIDADE_ATENDIMENTO
        order by DATA_CADASTRO_ATENDIMENTO, SEQ_ATENDIMENTO, modulo
    ) as atendimento_unico_rank
from profissionais
qualify atendimento_unico_rank = 1
