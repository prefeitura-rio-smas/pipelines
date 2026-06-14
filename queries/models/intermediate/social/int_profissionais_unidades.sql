-- Camada Intermediate: Profissionais vinculados às unidades
-- Contém toda a lógica de negócio (joins, agregação de CBO, filtro anti-teste)
-- Grão: 1 linha por (conta × unidade × CBO)

with operadores as (
    select * from {{ ref('raw_operadores') }}
),

operadores_unidades as (
    select * from {{ ref('raw_operadores_unidades') }}
),

unidades as (
    select * from {{ ref('raw_unidades') }}
),

profissionais as (
    select * from {{ ref('raw_profissionais') }}
),

profissionais_ocupacoes as (
    -- Evita duplicidade de profissionais com múltiplos CBOs
    select
        id_profissional,
        min(codigo_cbo) as codigo_cbo
    from {{ ref('raw_profissionais_ocupacoes') }}
    group by 1
),

cbo as (
    select * from {{ ref('raw_cbo') }}
),

joined as (
    select
        -- Conta / Operador
        ope.id_login,

        -- Unidade
        uni.id_unidade,
        uni.nome_unidade,
        uni.cas as territorio,

        -- Profissional
        prof.id_profissional,
        coalesce(ope.nome_operador, prof.nome) as nome_profissional,
        prof.cpf,
        ope.email,
        prof.telefone,
        prof.matricula,
        prof.flag_ativo,

        -- CBO
        ocup.codigo_cbo as cbo_codigo,
        cbo.descricao as cbo_descricao,

        -- Datas da conta
        ope.data_cadastro,
        ope.data_ultimo_acesso

    from operadores ope
    left join operadores_unidades ope_uni
        on ope.id_login = ope_uni.id_login
    left join unidades uni
        on ope_uni.id_unidade = uni.id_unidade
    left join profissionais prof
        on ope.id_login = prof.id_login
    left join profissionais_ocupacoes ocup
        on prof.id_profissional = ocup.id_profissional
    left join cbo
        on ocup.codigo_cbo = cbo.codigo_cbo
)

select *
from joined
where
    -- Remove registros com "teste" no nome (padrão do projeto)
    lower(coalesce(nome_profissional, '')) not like '%teste%'
