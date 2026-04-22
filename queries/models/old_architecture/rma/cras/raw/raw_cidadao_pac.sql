-- Tabela responsável por retornar informações de todos os usuários do sistema;
-- Usuários testes foram retirados;

with usuarios_sem_teste as (
    select
        seqpac,
        dscnomepac as nome_usuario,
        datnascim as data_nascimento,
        indsexo as sexo,
        racacor
    from {{source('cras_rma_prod', 'gh_cidadao_pac')}}
    where not regexp_contains(dscnomepac, r'(?i)teste')
)

select * from usuarios_sem_teste
