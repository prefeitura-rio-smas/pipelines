{{ config(materialized = 'table') }}

SELECT
    u.dscus AS nome_unidade,
    c.nompess AS nome_usuario,
    p.cpfprof AS CPF,
    c.dscemail AS Email,
    p.dsctel AS Telefone,
   c.indnivel,
    CASE c.indstatuser
        WHEN '1' THEN 'Ativa'
        WHEN '2' THEN 'Desativada'
        ELSE 'Sem informação'
    END AS "Status da Conta"
FROM
    {{ source('brutos_acolherio_staging', 'gh_contas_us') }} cu
JOIN
    {{ source('brutos_acolherio_staging', 'gh_contas') }} c ON cu.seqlogin = c.seqlogin
JOIN
    {{ source('brutos_acolherio_staging', 'gh_us') }} u ON cu.sequs = u.sequs
LEFT JOIN
    {{ source('brutos_acolherio_staging', 'gh_contas_modulos') }} cm ON c.seqlogin = cm.seqlogin
LEFT JOIN
    {{ source('brutos_acolherio_staging', 'gh_perfil_grupos') }} g ON cm.seqgrupo = g.seqgrupo
LEFT JOIN
    {{ source('brutos_acolherio_staging', 'gh_prof') }} p ON cm.seqprof = p.seqprof
ORDER BY
    nome_unidade, nome_usuario;