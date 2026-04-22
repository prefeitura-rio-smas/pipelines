-- Camada Raw: Vínculos de membros com famílias
with source as (
    select
        seqpac as id_paciente,
        seqfamil as id_familia,
        seqmembro as id_membro,
        seqfamilnova as id_familia_nova,
        seqmotivsaida as id_motivo_saida,
        datentrada as data_entrada,
        datsaida as data_saida,
        parentesco_responsavel_familia as parentesco_responsavel
    from  {{ source('brutos_acolherio_staging', 'gh_familias_membros') }}
)
select * from source
