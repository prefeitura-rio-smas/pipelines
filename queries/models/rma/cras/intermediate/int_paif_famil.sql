-- Tabela com todos os dados das famílias inseridades em acompanhamento PAIF.
with unidade_operador as (
    select
        a.seqlogin,
        b.unidade
    from {{ ref('stg_contas_por_unidade') }} a
    left join {{ ref('stg_unidades') }} b on b.sequs = a.sequs
),

filtro_familia_paif as (
    select
        a.seqfamil,
        a.flag_acomp_paif,
        a.data_original,
        a.mes_cadastro,
        a.ano_cadastro,
        a.seqlogincad,
        b.unidade
    from {{ ref ('stg_servassist') }} a
    inner join unidade_operador b on a.seqlogincad = b.seqlogin
)
select 
    * 
from filtro_familia_paif
where seqfamil in (select distinct(seqfamil) from {{ ref('stg_retirar_usuario_teste') }})