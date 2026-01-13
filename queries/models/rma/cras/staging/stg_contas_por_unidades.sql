-- Tabela retorna todas as contas do sistema acolherio e suas respectivas unidades;
-- Retorna contas que não possuem unidades;
-- Retira contas de testes, admin e suporte;

with contas_por_unidade as (
    select
        a.login_operador,
        a.operador,
        a.seqlogin,
        b.sequs
    from {{ ref ('base_contas') }} a
    left join  {{ ref ('base_contas_unidades') }} b on a.seqlogin = b.seqlogin
)


select * from contas_por_unidade
where not regexp_contains(operador, r'(?i)teste|admin|suporte')