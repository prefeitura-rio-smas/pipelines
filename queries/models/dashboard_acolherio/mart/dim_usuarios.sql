-- Tabela onde há dados relacionados aos usuários do acolherio
-- Dimensão usuários
{{ config(materialized='table') }}

with dados_usuarios as (
    select
        {{ dbt_utils.generate_surrogate_key(['seqpac', 'data_cadastro_usuario']) }} as seqpac_sk,   
        *
    from {{ ref('int_dados_usuarios') }}
),


-- Join para pegar o id_familia de cada usuário
usuarios_e_id_familia as  (
    select
    a.seqpac_sk,
    a.seqpac,
    b.seqfamil,
    a.prontuario,
    a.pais_origem_descricao,
    a.nacionalidade,
    a.condicao_estrangeira,
    a.nome_usuario,
    a.nome_social,
    a.data_nascimento,
    a.bairro,
    a.cpf,
    a.sexo,
    a.orientacao_sexual,
    a.genero,
    a.raca,
    a.estado_civil,
    a.filiacao_mae,
    a.flag_trabalho,
    a.vinculo_trabalhista,
    a.profissao,
    a.flag_atvd_gera_renda,
    a.flag_frequencia_escola,
    a.serie_escola,
    a.escolaridade,
    a.flag_recebe_beneficio,
    a.tipo_beneficio,
    a.flag_deficiencia,
    a.tipo_deficiencia,
    a.saude_mental_comprometida,
    a.violacao_direito,
    a.flag_curatela,
    a.tipo_curatela,
    a.numero_processo_decisao_apoiada,
    a.nome_apoiador,
    a.flag_situacao_rua,
    a.flag_cadunico,
    a.grau_dependencia,
    a.pontuacao,
    a.motivo_acolhimento,
    a.codorigem,
    a.seqlogincad,
    d.operador,
    d.login_operador,  
    a.data_cadastro_usuario,
    c.dscoripcsm as origem_demanda
    from dados_usuarios a
    left join {{ ref('int_membros_familia') }} b on a.seqpac = b.seqpac
    left join {{ source('source_dashboard_acolherio', 'gh_origens') }} c on a.codorigem = c.codorigem
    left join {{ ref('stg_contas_operadores') }} d on a.seqlogincad = d.seqlogin
)


select * from usuarios_e_id_familia