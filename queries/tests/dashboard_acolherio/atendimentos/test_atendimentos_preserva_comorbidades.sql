-- O registro médico original deve chegar inteiro ao usuário correto.
select a.id_atendimento_modulo
from {{ ref('mart_atendimentos') }} as a
left join {{ ref('raw_usuarios_informacoes_medicas') }} as m on a.id_usuario = m.id_usuario
where
    if(m.id_usuario is null, 0, 1) != coalesce(array_length(a.comorbidades_detalhes), 0)
    or (
        m.id_usuario is not null
        and to_json_string(struct(
            'gh_pac_info_med' as origem,
            m.flag_possui_comorbidade as indicador_original,
            m.descricao_comorbidades as descricao_original,
            m.data_informacao_comorbidades as data_informacao,
            m.local_tratamento_comorbidades as local_tratamento_original,
            m.data_cadastro,
            m.id_unidade_cadastro
        )) is distinct from to_json_string(a.comorbidades_detalhes[safe_offset(0)])
    )
