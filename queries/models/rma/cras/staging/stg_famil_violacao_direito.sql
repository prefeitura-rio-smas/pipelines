with usuarios_violacao_direito as (
    select
        a.seqfamil,
        a.seqpac,
        a.seqmembro,
        a.nome,
        a.idade,
        b.violacao_direito
    from {{ ref('stg_retirar_usuario_teste') }} a
    inner join {{ ref('stg_violacao_direito') }} b on a.seqpac = b.seqpac
)

select 
    * 
from usuarios_violacao_direito