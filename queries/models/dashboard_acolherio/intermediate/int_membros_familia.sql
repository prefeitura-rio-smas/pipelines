
with filtrar_membros_familias_atuais as (
    select 
        seqpac,
        seqfamil
    from {{ ref('stg_membros_familia_acolherio') }}
    where  data_saida_membro_familia is null
)

select * from filtrar_membros_familias_atuais