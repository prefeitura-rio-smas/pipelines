-- Camada Raw: Ciclos de acolhimento dos usuários
with source as (
    select
        seqciclo as id_ciclo,
        seqpac as id_usuario,
        sequs as id_unidade,
        dtentrada as data_entrada,
        horentrada as hora_entrada,
        dtsaida as data_saida,
        horsaida as hora_saida,
        sequssaida as id_unidade_saida,
        seqpacorig as id_usuario_origem,
        seqloginentr as id_login_entrada,
        seqloginsaid as id_login_saida,
        indciclo as indicador_ciclo,
        indmotivsaida as motivo_saida
    from {{ source('brutos_acolherio_staging', 'gh_pac_ciclos') }}
)
select * from source
