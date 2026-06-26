with base as (
    select * from {{ ref('raw_usuarios') }}
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
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['base.id_paciente']) }} as id_usuario_sk,
        base.id_paciente as id_usuario,
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
        det.nome_apoiador,
        det.numero_processo_decisao_apoiada,
        {{ map_coluna_decisao_apoiada('det.numero_processo_decisao_apoiada') }} as flag_decisao_apoiada,
        {{ map_coluna_saude_mental('det.flag_saude_mental_comprometida') }} as flag_saude_mental_comprometida,
        {{ map_coluna_tipo_motiv_acolhimento('det.id_motivo_acolhimento') }} as motivo_acolhimento,
        {{ map_coluna_orientacao_sexual('det.orientacao_sexual') }} as orientacao_sexual,
        {{ map_coluna_vinculo_trabalhista('det.vinculo_trabalhista') }} as vinculo_trabalhista,
        sm.flag_trabalha,
        sm.profissao,
        sm.flag_frequenta_escola,
        {{ map_coluna_escolaridade('sm.serie_escolar') }} as escolaridade_indice,
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
        if(v.id_usuario is not null, 'Sim', 'Não') as flag_possui_violacao_direito,
        v.violacoes
    from base
    left join detalhes det on base.id_paciente = det.id_paciente
    left join saude_mental sm on base.id_paciente = sm.id_paciente
    left join origens ori on sm.codigo_origem = ori.id_origem
    left join violacoes v on base.id_paciente = v.id_usuario
    where base.nome not like '%TESTE%'
)

select * from final
