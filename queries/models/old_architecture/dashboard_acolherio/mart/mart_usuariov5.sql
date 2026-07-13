-- Tabela onde há dados relacionados aos usuários do acolherio

-- Join para pegar o id_familia de cada usuário
with usuarios_e_id_familia as  (
    select
        a.*,
        b.seqfamil,
        c.dscoripcsm as origem_demanda,
        d.operador,
        d.login_operador
    from {{ ref('int_dados_usuarios') }} a
    left join {{ ref('int_membros_familia') }} b on a.seqpac = b.seqpac
    left join {{ source('source_dashboard_acolherio', 'gh_origens') }} c on a.codorigem = c.codorigem
    left join {{ ref('stg_contas_operadores') }} d on a.seqlogincad = d.seqlogin
    where not regexp_contains(a.nome_usuario, '(?i)teste')
    
)



select * from usuarios_e_id_familia