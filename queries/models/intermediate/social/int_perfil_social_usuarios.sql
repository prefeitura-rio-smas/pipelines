-- Uma linha por usuário; preserva cadastros TESTE e IDs sem raw_usuarios.
-- noqa: disable=ST06
with usuarios as (
    select id_paciente as id_usuario from {{ ref('raw_usuarios_detalhes') }}
    union distinct
    select id_paciente as id_usuario from {{ ref('raw_usuarios_saude_mental') }}
    union distinct
    select id_usuario from {{ ref('raw_usuarios_informacoes_medicas') }}
    union distinct
    select id_usuario from {{ ref('int_usuarios_violacoes') }}
),

beneficios as (
    select
        id_usuario,
        array_agg(struct(codigo, descricao) order by codigo) as beneficios_detalhes,
        string_agg(distinct descricao, ', ' order by descricao) as beneficios
    from (
        select distinct
            b.id_usuario,
            item.codigo,
            coalesce(item.descricao, format('Benefício não mapeado (código %s)', item.codigo)) as descricao
        from {{ ref('int_beneficios_agregados') }} as b,
            unnest(b.beneficio) as item
        where upper(trim(item.codigo)) not in ('', 'N')
    )
    group by id_usuario
),

deficiencias as (
    select
        id_usuario,
        array_agg(struct(codigo, descricao) order by codigo) as deficiencias_detalhes,
        string_agg(distinct descricao, ', ' order by descricao) as deficiencias
    from (
        select distinct
            d.id_usuario,
            item.codigo,
            coalesce(item.descricao, format('Deficiência não mapeada (código %s)', item.codigo)) as descricao
        from {{ ref('int_deficiencias_agregadas') }} as d,
            unnest(d.deficiencia) as item
        where upper(trim(item.codigo)) not in ('', 'N')
    )
    group by id_usuario
),

violacoes as (
    select
        id_usuario,
        violacoes as violacoes_detalhes,
        (
            select string_agg(distinct item.descricao, ', ' order by item.descricao)
            from unnest(violacoes) as item
        ) as violacoes
    from {{ ref('int_usuarios_violacoes') }}
)

select
    u.id_usuario,
    case
        when array_length(v.violacoes_detalhes) > 0 then 'Sim'
        when upper(trim(d.violacao_direito)) = 'N' then 'Não'
        else 'Não Informado'
    end as flag_possui_violacao_direito,
    v.violacoes,
    v.violacoes_detalhes,
    case upper(trim(s.flag_recebe_beneficio))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        else 'Não Informado'
    end as flag_recebe_beneficio,
    b.beneficios,
    b.beneficios_detalhes,
    case upper(trim(s.flag_deficiencia))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        else 'Não Informado'
    end as flag_deficiencia,
    df.deficiencias,
    df.deficiencias_detalhes,
    case
        when upper(trim(s.tipo_moradia)) = '5' then 'Sim'
        when upper(trim(s.tipo_moradia)) in ('1', '2', '3', '4', '6', '7', '8', '9', 'A', 'B', 'C', 'D') then 'Não'
        else 'Não Informado'
    end as flag_situacao_rua,
    case upper(trim(m.flag_possui_comorbidade))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        else 'Não Informado'
    end as flag_possui_comorbidade,
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
left join beneficios as b on u.id_usuario = b.id_usuario
left join deficiencias as df on u.id_usuario = df.id_usuario
left join violacoes as v on u.id_usuario = v.id_usuario
