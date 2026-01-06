-- Garantindo que a tabela só retorna famílias em acompanhamento paif.

with filtro_famil_paif as (
    select
        seqfamil,
        servassist
    from {{ ref('stg_servassist') }}
    where servassist != 'Acompanhamento Paif'
)

select * from filtro_famil_paif