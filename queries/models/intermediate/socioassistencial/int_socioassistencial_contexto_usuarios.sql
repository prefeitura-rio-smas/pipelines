-- Cadastro atual, explicitamente separado dos fatos históricos. Evita
-- int_usuarios/dim_usuarios porque o contrato dessas entidades exclui TESTE.
with vinculos as (
    select
        id_paciente as id_usuario,
        array_agg(struct(
            id_membro, id_familia, data_entrada, data_saida,
            id_familia_nova, id_motivo_saida, parentesco_responsavel
        ) order by data_entrada, id_membro) as vinculos_familiares
    from {{ ref('raw_membros_familia') }}
    group by id_paciente
)

select
    u.id_paciente as id_usuario,
    struct(
        u.nome,
        u.nome_social,
        u.nome_mae,
        u.data_nascimento,
        u.sexo,
        {{ map_coluna_genero('u.genero') }} as genero,
        {{ map_coluna_raca('u.raca_cor') }} as raca_cor,
        {{ map_coluna_orientacao_sexual('d.orientacao_sexual') }} as orientacao_sexual,
        u.cpf,
        u.uf_nascimento,
        u.nacionalidade,
        u.condicao_estrangeira,
        u.pais_origem,
        u.bairro,
        u.logradouro,
        u.numero_endereco,
        u.complemento_endereco,
        u.ponto_referencia,
        u.data_cadastro,
        u.id_unidade_referencia,
        coalesce(upper(u.nome) like '%TESTE%', false) as flag_teste,
        'SNAPSHOT_ATUAL' as temporalidade
    ) as usuario,
    struct(
        p.flag_possui_violacao_direito,
        p.violacoes_detalhes as violacoes,
        p.flag_recebe_beneficio,
        p.beneficios_detalhes as beneficios,
        p.flag_deficiencia,
        p.deficiencias_detalhes as deficiencias,
        p.flag_situacao_rua,
        p.flag_possui_comorbidade,
        p.comorbidades,
        p.data_informacao_comorbidades,
        p.local_tratamento_comorbidades,
        p.comorbidades_detalhes,
        d.flag_cadunico,
        documentos as documentos_cadunico_atual,
        d.data_cadunico as data_cadunico_declarada,
        d.renda_ativa,
        d.renda_beneficio,
        d.flag_saude_mental_comprometida,
        s.flag_trabalha,
        s.profissao,
        s.flag_frequenta_escola,
        s.serie_escolar,
        {{ map_coluna_escolaridade('s.escolaridade') }} as escolaridade,
        v.vinculos_familiares,
        'SNAPSHOT_ATUAL' as temporalidade
    ) as perfil_social
from {{ ref('raw_usuarios') }} as u
left join {{ ref('raw_usuarios_detalhes') }} as d on u.id_paciente = d.id_paciente
left join {{ ref('raw_usuarios_saude_mental') }} as s on u.id_paciente = s.id_paciente
left join {{ ref('int_perfil_social_usuarios') }} as p on u.id_paciente = p.id_usuario
left join vinculos as v on u.id_paciente = v.id_usuario
left join {{ ref('int_documentos_cadunico_por_cpf') }} as documentos
    on nullif(regexp_replace(coalesce(u.cpf, ''), r'[^0-9]', ''), '') = documentos.cpf_normalizado
