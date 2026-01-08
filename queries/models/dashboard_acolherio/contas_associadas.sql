{{ config(materialized = 'table') }}

WITH contas_associadas AS (
    SELECT
    us.dscus AS unidade,
    contas.seqlogin,
    contas.nompess AS operador,
    cbo.dsccbo AS profissional,
    prof.cpfprof AS cpf,
    prof.dtnasc AS data_nascimento,
    contas.dscemail AS email,
    prof.dsctel AS telefone,
    contas_us.datacesso,
    {{ map_coluna_perfil_acesso('contas.indnivel') }},
    {{ map_coluna_status_conta('contas.indstatuser')}}
    FROM {{ source('brutos_acolherio_staging', 'gh_contas')}} contas
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_prof')}} prof ON prof.seqlogin = contas.seqlogin
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_contas_us')}} contas_us ON contas_us.seqlogin = prof.seqlogin
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_us')}} us ON contas_us.sequs = us.sequs
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_profocup')}} ocup ON ocup.seqprof = prof.seqprof
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_cbo')}} cbo ON cbo.codcbo = ocup.codcbo
)

SELECT * FROM contas_associadas
