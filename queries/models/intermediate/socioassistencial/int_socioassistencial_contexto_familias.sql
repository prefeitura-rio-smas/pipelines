with acolhimentos as (
    select
        id_usuario,
        array_agg(struct(data_entrada, data_saida) order by data_entrada, data_saida) as ciclos
    from {{ ref('raw_usuarios_acolhimentos') }}
    group by id_usuario
),

membros as (
    select
        m.id_familia,
        array_agg(struct(
            m.id_membro, m.id_paciente as id_usuario, m.data_entrada, m.data_saida,
            m.parentesco_responsavel,
            u.data_nascimento,
            u.nome not like '%TESTE%' as flag_usuario_elegivel_dimensao,
            u.sexo,
            p.beneficios_detalhes as beneficios_cadastro_atual,
            p.violacoes_detalhes as violacoes_cadastro_atual,
            p.deficiencias_detalhes as deficiencias_cadastro_atual,
            a.ciclos as acolhimentos
        ) order by m.data_entrada, m.id_membro) as membros
    from {{ ref('raw_membros_familia') }} as m
    left join {{ ref('raw_usuarios') }} as u on m.id_paciente = u.id_paciente
    left join {{ ref('int_perfil_social_usuarios') }} as p on m.id_paciente = p.id_usuario
    left join acolhimentos as a on m.id_paciente = a.id_usuario
    group by m.id_familia
),

vulnerabilidades as (
    select
        id_familia,
        array_agg(struct(
            id_vulnerabilidade, data_cadastro, data_cancelamento, id_login_cadastro
        ) order by data_cadastro, id_vulnerabilidade) as vulnerabilidades
    from {{ ref('raw_familias_vulnerabilidades') }}
    group by id_familia
),

projetos as (
    select
        id_familia,
        array_agg(struct(
            id_projeto_social,
            {{ map_nome_projeto_social('id_projeto_social') }} as nome,
            id_unidade, id_login_cadastro, data_cadastro, data_cancelamento, indicador_ativo
        ) order by data_cadastro, id_projeto_social) as projetos
    from {{ ref('raw_familias_projetos_sociais') }}
    group by id_familia
)

select
    f.id_familia,
    struct(
        f.id_usuario_responsavel,
        f.data_ultima_modificacao,
        f.flag_ativo,
        m.membros,
        struct(
            renda.renda_media_pc as per_capita_cadunico_atual,
            renda.renda_media_pc_prontuario as per_capita_prontuario_atual,
            renda.renda_familiar_prontuario,
            renda.qtd_membros_ativos,
            renda.qtd_membros_informaram
        ) as renda_atual,
        v.vulnerabilidades,
        p.projetos,
        s.servicos_socioassistenciais_historico as servicos,
        'SNAPSHOT_ATUAL_COM_VIGENCIAS_DECLARADAS' as temporalidade
    ) as familia
from {{ ref('raw_familias') }} as f
left join membros as m on f.id_familia = m.id_familia
left join vulnerabilidades as v on f.id_familia = v.id_familia
left join projetos as p on f.id_familia = p.id_familia
left join {{ ref('int_servicos_familia_completos') }} as s on f.id_familia = s.id_familia
left join {{ ref('int_familias_extrema_pobreza') }} as renda on f.id_familia = renda.id_familia
