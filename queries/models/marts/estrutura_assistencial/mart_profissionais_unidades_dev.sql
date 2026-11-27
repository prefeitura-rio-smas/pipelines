{{ config(enabled=target.name in ["dev", "ci"]) }}

with operadores as (
    select * from {{ ref('int_operadores') }}
),

profissionais as (
    select * from {{ ref('int_profissionais_dev') }}
)

select
    ope.login_operadores,
    ope.id_login,
    ope.nome_operador,
    ope.email,
    ope.cpf,
    ope.nivel_conta,
    ope.perfil_acesso,
    ope.tipo_acesso_codigo,
    ope.status_conta_codigo,
    ope.data_cadastro,
    ope.data_ultimo_acesso,
    ope.dias_ultimo_acesso,
    ope.id_unidade,
    ope.nome_unidade,
    ope.cas,
    ope.esfera,
    ope.email_unidade,
    ope.email_filtro,
    ope.flag_unidade_ativa,
    ope.id_tipo_unidade,
    ope.nome_tipo,
    ope.classe,
    ope.tipo_unidade,
    ope.flag_eixo_adulto,
    ope.flag_eixo_familia,
    ope.flag_eixo_idoso,
    p.id_profissional,
    p.flag_sem_conta,
    p.telefone,
    p.flag_ativo,
    p.qtde_cbos,
    p.codigos_cbo,
    p.descricoes_cbo,
    p.flag_multi_cbo,
    p.cbo_principal_codigo,
    p.cbo_principal_descricao,
    p.id_perfil_acesso,
    p.perfil_acesso_nome
from operadores as ope
left join profissionais as p on ope.id_login = p.id_login
