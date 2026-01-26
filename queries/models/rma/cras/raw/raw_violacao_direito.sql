-- Tabela responsável por retornar todos as violações de direito de cada usuário;
-- Não contém usuários testes;

with violacoes_direito_sem_usuarios_testes as (
select
    nome_usuario as usuario,
    seqpac,
    viol_direito
from {{ source('cras_rma_relatorio', 'violacao_direito')}}
where not regexp_contains(nome_usuario, r'(?i)teste')
)

select * from violacoes_direito_sem_usuarios_testes