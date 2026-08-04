-- Camada Raw: Presenças de profissionais em atividades de grupo

with source as (
    select
        seqativgrp as id_atividade,
        seqprof as id_profissional,
        codcboprof as codigo_cbo,
        dtpresdia as data_presenca,
        horpresdia as hora_presenca
    from {{ source('prontuario_carioca_assistencia_social', 'gh_ativ_prof') }}
)

select * from source
