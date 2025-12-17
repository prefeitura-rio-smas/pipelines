select
    nome_usuario,
    seqpac,
    violacao_direito 
from {{ ref('violacao_direito')}}
where not regexp_contains(nome_usuario, r'(?i)teste')

