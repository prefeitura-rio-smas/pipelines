-- Entidade de usuários com perfil cadastral/social disponível, inclusive IDs órfãos.
-- A dimensão preserva seu recorte de usuários cadastrados e sem nome TESTE.
-- Perfil atual não representa um snapshot histórico do atendimento.
-- noqa: disable=ST06
with base as (
    select * from {{ ref('raw_usuarios') }}
),

usuarios as (
    select id_paciente as id_usuario from base
    union distinct
    select id_paciente from {{ ref('raw_usuarios_detalhes') }}
    union distinct
    select id_paciente from {{ ref('raw_usuarios_saude_mental') }}
    union distinct
    select id_usuario from {{ ref('raw_usuarios_informacoes_medicas') }}
    union distinct
    select id_usuario from {{ ref('int_usuarios_violacoes') }}
),

informacoes_medicas as (
    select * from {{ ref('raw_usuarios_informacoes_medicas') }}
),

detalhes as (
    select * from {{ ref('raw_usuarios_detalhes') }}
),

saude_mental as (
    select * from {{ ref('raw_usuarios_saude_mental') }}
),

origens as (
    select * from {{ ref('raw_origens') }}
),

violacoes as (
    select * from {{ ref('int_usuarios_violacoes') }}
),

deficiencias as (
    select * from {{ ref('int_deficiencias_agregadas') }}
),

beneficios as (
    select * from {{ ref('int_beneficios_agregados') }}
),

projetos as (
    select * from {{ ref('int_usuarios_projetos_sociais') }}
),

final as (
    select
        u.id_usuario,
        base.nome,
        base.nome_social,
        base.nome_mae as filiacao_mae,
        {{ map_coluna_nacionalidade('base.nacionalidade') }} as nacionalidade,
        {{ map_coluna_cond_estrangeiro('base.condicao_estrangeira') }} as condicao_estrangeira,
        base.pais_origem,
        base.cpf,
        base.data_nascimento,
        base.sexo,
        {{ map_coluna_raca('base.raca_cor') }} as raca_cor,
        {{ map_coluna_genero('base.genero') }} as genero,
        base.bairro,
        base.prontuario_papel as prontuario,
        det.flag_cadunico,
        det.violacao_direito as violacao_direito_bruto,
        det.pontuacao,
        det.grau_dependencia,
        det.nome_apoiador,
        det.numero_processo_decisao_apoiada,
        det.renda_ativa,
        case
            when det.renda_ativa != 0 then "Sim"
            else "Não"
        end as atvd_remunerada,
        {{ map_coluna_decisao_apoiada('det.numero_processo_decisao_apoiada') }} as flag_decisao_apoiada,
        {{ map_coluna_saude_mental('det.flag_saude_mental_comprometida') }} as flag_saude_mental_comprometida,
        {{ map_coluna_tipo_motiv_acolhimento('det.id_motivo_acolhimento') }} as motivo_acolhimento,
        {{ map_coluna_orientacao_sexual('det.orientacao_sexual') }} as orientacao_sexual,
        {{ map_coluna_vinculo_trabalhista('det.vinculo_trabalhista') }} as vinculo_trabalhista,
        sm.flag_trabalha,
        sm.profissao,
        sm.flag_frequenta_escola,
        {{ map_coluna_escolaridade('sm.escolaridade') }} as escolaridade_indice,
        sm.flag_recebe_beneficio,
        sm.tipo_beneficio,
        sm.flag_curatela,
        {{ map_coluna_tipo_curatela('sm.tipo_curatela') }} as tipo_curatela,
        sm.flag_deficiencia,
        sm.tipo_deficiencia,
        sm.flag_situacao_rua,
        sm.codigo_origem,
        ori.descricao_origem as origem_demanda,
        -- Enriquecimento com Structs e Flags
        if(v.id_usuario is not null, "Sim", "Não") as flag_possui_violacao_direito,
        v.violacoes,
        d.deficiencia,
        b.beneficio,
        proj.projetos_sociais,
        base.id_unidade_referencia,
        base.id_login_cadastro,
        base.id_paciente is not null as flag_usuario_cadastrado,
        coalesce(upper(base.nome) like "%TESTE%", false) as flag_usuario_teste,
        struct(
            case
                when array_length(v.violacoes) > 0 then "Sim"
                when upper(trim(det.violacao_direito)) = "N" then "Não"
                else "Não Informado"
            end as flag_possui_violacao_direito,
            {{ descricoes_sociais('v.violacoes') }} as violacoes,
            v.violacoes as violacoes_detalhes,
            {{ map_indicador_social('sm.flag_recebe_beneficio') }} as flag_recebe_beneficio,
            b.beneficios as beneficios,
            b.beneficios_detalhes as beneficios_detalhes,
            {{ map_indicador_social('sm.flag_deficiencia') }} as flag_deficiencia,
            d.deficiencias as deficiencias,
            d.deficiencias_detalhes as deficiencias_detalhes,
            coalesce(sm.flag_situacao_rua, "Não Informado") as flag_situacao_rua,
            {{ map_indicador_social('med.flag_possui_comorbidade') }} as flag_possui_comorbidade,
            nullif(trim(med.descricao_comorbidades), "") as comorbidades,
            med.data_informacao_comorbidades,
            nullif(trim(med.local_tratamento_comorbidades), "") as local_tratamento_comorbidades,
            case
                when med.id_usuario is not null then [struct(
                    "gh_pac_info_med" as origem,
                    med.flag_possui_comorbidade as indicador_original,
                    med.descricao_comorbidades as descricao_original,
                    med.data_informacao_comorbidades as data_informacao,
                    med.local_tratamento_comorbidades as local_tratamento_original,
                    med.data_cadastro,
                    med.id_unidade_cadastro
                )]
            end as comorbidades_detalhes
        ) as perfil_social
    from usuarios as u
    left join base on u.id_usuario = base.id_paciente
    left join detalhes as det on u.id_usuario = det.id_paciente
    left join saude_mental as sm on u.id_usuario = sm.id_paciente
    left join origens as ori on sm.codigo_origem = ori.id_origem
    left join violacoes as v on u.id_usuario = v.id_usuario
    left join deficiencias as d on u.id_usuario = d.id_usuario
    left join beneficios as b on u.id_usuario = b.id_usuario
    left join projetos as proj on u.id_usuario = proj.id_usuario
    left join informacoes_medicas as med on u.id_usuario = med.id_usuario
    where u.id_usuario is not null
)

select * from final
