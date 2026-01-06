-- Teste para verificar se a tabela stg_violacao_direito possui algum usuário teste.

with filtro_usuario_teste as (
    select 
        nome_usuario
    from {{ ref('stg_violacao_direito') }}
    where regexp_contains(nome_usuario, r'(?i)teste')
)

select * from filtro_usuario_teste