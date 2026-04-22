-- Tabela responsável por retornar todos os benefícios de cada usuário;
-- Não contém usuários testes;

with beneficios_sem_usuarios_testes as (
    select 
        nome_usuario as usuario,
        id_usuario as seqpac,
        beneficio
    from {{ source('cras_rma_dev', 'tipo_beneficio')}}
    where not regexp_contains(nome_usuario, r'(?i)teste')
)

select * from beneficios_sem_usuarios_testes