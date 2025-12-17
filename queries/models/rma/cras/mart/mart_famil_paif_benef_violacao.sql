-- Todos os membros de cada família em acompanhamento PAIF
with membros_familias as (
    select
        a.seqfamil,
        a.unidade,
        b.seqpac,
        b.data_nascimento,
        b.idade
    from {{ ref('mart_paif_famil') }} a
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
        c.flag_descumprimento_condicionalidade_bf
    from membros_familias a
    inner join {{ ref('stg_beneficios') }} b on a.seqpac = b.seqpac 
    left join {{ ref('stg_vulnerab') }} c on a.seqfamil = c.seqfamil
)

select 
    seqfamil,
    unidade,
    seqpac,
    data_nascimento,
    idade,
    case
        when beneficio = 'Bolsa Família'
        then 'Sim'
        else 'Não'
    end as flag_bf,
    case 
        when beneficio = 'BPC-Benefício de Prestação Continuada'
        then 'Sim'
        else 'Não'
    end as flag_bpc,
    case
        when flag_descumprimento_condicionalidade_bf is null
        then 'Não'
    end as flag_descumprimento_condicionalidade_bf
 from membros_benef
where beneficio = 'BPC-Benefício de Prestação Continuada'
or beneficio = 'Bolsa Família'