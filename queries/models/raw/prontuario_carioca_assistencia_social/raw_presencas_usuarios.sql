-- Camada Raw: Presenças de usuários em atividades de grupo

with source as (
    select
        seqativgrp as id_atividade,
        seqpac as id_usuario,
        dtpresdia as data_presenca,
        horpresdia as hora_presenca,
        sequs as id_unidade,
        seqsetor as id_setor
    from {{ source('brutos_acolherio_staging', 'gh_ativ_paciente') }}
)

select * from source
