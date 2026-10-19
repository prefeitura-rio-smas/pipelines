-- noqa: disable=ST06
with usuarios as (
    select id_paciente as id_usuario from {{ ref('raw_usuarios_detalhes') }}
    union distinct
    select id_paciente as id_usuario from {{ ref('raw_usuarios_saude_mental') }}
    union distinct
    select id_usuario from {{ ref('raw_usuarios_informacoes_medicas') }}
    union distinct
    select id_usuario from {{ ref('int_usuarios_violacoes') }}
)

select
    u.id_usuario,
    case
        when array_length(v.violacoes) > 0 then 'Sim'
        when upper(trim(d.violacao_direito)) = 'N' then 'Não'
        else 'Não Informado'
    end as flag_possui_violacao_direito,
    {{ descricoes_sociais('v.violacoes') }} as violacoes,
    v.violacoes as violacoes_detalhes,
    {{ map_indicador_social('s.flag_recebe_beneficio') }} as flag_recebe_beneficio,
    b.beneficios as beneficios,
    b.beneficios_detalhes,
    {{ map_indicador_social('s.flag_deficiencia') }} as flag_deficiencia,
    df.deficiencias as deficiencias,
    df.deficiencias_detalhes,
    coalesce(s.flag_situacao_rua, 'Não Informado') as flag_situacao_rua,
    {{ map_indicador_social('m.flag_possui_comorbidade') }} as flag_possui_comorbidade,
    nullif(trim(m.descricao_comorbidades), '') as comorbidades,
    m.data_informacao_comorbidades,
    nullif(trim(m.local_tratamento_comorbidades), '') as local_tratamento_comorbidades,
    case
        when m.id_usuario is not null then [struct(
            'gh_pac_info_med' as origem,
            m.flag_possui_comorbidade as indicador_original,
            m.descricao_comorbidades as descricao_original,
            m.data_informacao_comorbidades as data_informacao,
            m.local_tratamento_comorbidades as local_tratamento_original,
            m.data_cadastro,
            m.id_unidade_cadastro
        )]
    end as comorbidades_detalhes
from usuarios as u
left join {{ ref('raw_usuarios_detalhes') }} as d on u.id_usuario = d.id_paciente
left join {{ ref('raw_usuarios_saude_mental') }} as s on u.id_usuario = s.id_paciente
left join {{ ref('raw_usuarios_informacoes_medicas') }} as m on u.id_usuario = m.id_usuario
left join {{ ref('int_beneficios_agregados') }} as b on u.id_usuario = b.id_usuario
left join {{ ref('int_deficiencias_agregadas') }} as df on u.id_usuario = df.id_usuario
left join {{ ref('int_usuarios_violacoes') }} as v on u.id_usuario = v.id_usuario
