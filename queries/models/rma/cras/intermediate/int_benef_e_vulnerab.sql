-- Tabela responsável por trazer dados de benefícios e se a família está em descumprimento de condicionalidades PBF. Retorna famílias inseridas no aocmpanhamento PAIF.
with membros_familias as (
    select
        a.seqfamil,
        a.unidade,
        b.seqpac,
        b.data_nascimento,
        b.idade
    from {{ ref('int_paif_famil') }} a
    inner join {{ ref('stg_retirar_usuario_teste') }} b on a.seqfamil = b.seqfamil
),

membros_benef as (
    select
        a.seqfamil,
        a.unidade,
        a.seqpac,
        a.data_nascimento,
        a.idade,
        b.beneficio,
        c.seqvulnerab
    from membros_familias a
    inner join {{ ref('stg_beneficios') }} b on a.seqpac = b.seqpac 
    left join {{ ref('stg_vulnerab') }} c on a.seqfamil = c.seqfamil
)

select * from membros_benef