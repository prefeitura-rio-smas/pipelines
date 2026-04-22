-- Camada Raw: Ciclos de acolhimento dos usuários
with source as (
    select
        seqciclo as id_ciclo,
        seqpac as id_usuario,
        sequs as id_unidade,
        seqlogin as id_login_cadastro,
        dtentrada as data_entrada,
        horaent as hora_entrada,
        dtsaida as data_saida,
        horasai as hora_saida,
        seqmotivo as id_motivo_acolhimento,
        seqmotsai as id_motivo_saida,
        indlocal as local_acolhimento
    from {{ source('brutos_acolherio_staging', 'gh_pac_ciclos') }}
)
select * from source
