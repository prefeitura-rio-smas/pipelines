-- Camada Raw: Atividades de grupo

with source as (
    select
        seqativgrp as id_atividade,
        nmativgrp as nome_atividade,
        dtativ as data_inicio,
        horaativ as hora_inicio,
        dtativfim as data_fim,
        horaativfim as hora_fim,
        seqtpativ as id_tipo_atividade,
        sequs as id_unidade,
        seqlocal as id_local,
        dscperiodativ as recorrencia,
        indmatri as indicador_matricula,
        obsmatri as observacao_matricula
    from {{ source('prontuario_carioca_assistencia_social', 'gh_ativgrp') }}
)

select * from source
