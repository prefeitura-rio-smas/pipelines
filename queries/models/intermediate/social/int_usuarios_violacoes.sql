with source as (
    select
        id_paciente as id_usuario,
        violacao_direito
    from {{ ref('raw_usuarios_detalhes') }}
    where
        violacao_direito is not null
        and violacao_direito != ''
        and violacao_direito != 'N'
),

codigos_separados as (
    select
        id_usuario,
        trim(codigo) as codigo
    from source,
        unnest(split(violacao_direito, ',')) as codigo
),

violacoes_checkbox as (
    select
        id_usuario,
        'checkbox' as origem,
        codigo,
        {{ map_violacao_direito_descricao('codigo') }} as descricao,
        cast(null as int64) as id_evolucao,
        cast(null as string) as origem_modulo,
        cast(null as datetime) as data_evolucao,
        cast(null as string) as titulo_formulario,
        cast(null as string) as label,
        cast(null as string) as valor
    from codigos_separados
    where codigo != ''
),

evolucoes as (
    select
        id_usuario,
        id_evolucao,
        data_evolucao,
        origem_modulo,
        descricao_evolucao
    from {{ ref('int_evolucoes') }}
    where
        data_cancelamento is null
        and id_usuario is not null
),

campos_label as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes)",
        id_cols = ['id_usuario', 'id_evolucao', 'data_evolucao', 'origem_modulo'],
        col_html = 'descricao_evolucao'
    ) }}
),

violacoes_label as (
    select
        id_usuario,
        'label' as origem,
        {{ map_label_violacao('titulo_formulario', 'label', 'valor') }} as codigo,
        {{ map_violacao_direito_descricao(
            map_label_violacao('titulo_formulario', 'label', 'valor')
        ) }} as descricao,
        id_evolucao,
        origem_modulo,
        data_evolucao,
        titulo_formulario,
        label,
        valor
    from campos_label
    where
        label is not null
        and trim(label) != ''
),

violacoes_unificadas as (
    select * from violacoes_checkbox
    union all
    select * from violacoes_label
),

violacoes_agregadas as (
    select
        id_usuario,
        array_agg(
            struct(
                origem,
                codigo,
                descricao,
                id_evolucao,
                origem_modulo,
                data_evolucao,
                titulo_formulario,
                label,
                valor
            )
            order by origem, codigo, data_evolucao
        ) as violacoes
    from violacoes_unificadas
    where id_usuario is not null
    group by id_usuario
),

violacoes_por_codigo as (
    select
        id_usuario,
        codigo,
        max(descricao) as descricao,
        array_agg(distinct origem order by origem) as origens,
        array_agg(
            struct(
                origem,
                codigo,
                descricao,
                id_evolucao,
                origem_modulo,
                data_evolucao,
                titulo_formulario,
                label,
                valor
            )
            order by origem, data_evolucao
        ) as evidencias
    from violacoes_unificadas
    where
        id_usuario is not null
        and codigo is not null
    group by id_usuario, codigo
),

violacoes_json as (
    select
        id_usuario,
        array_agg(
            to_json(
                struct(
                    codigo,
                    descricao,
                    origens,
                    evidencias
                )
            )
            order by codigo
        ) as violacoes_json
    from violacoes_por_codigo
    group by id_usuario
),

final as (
    select
        va.id_usuario,
        va.violacoes,
        vj.violacoes_json
    from violacoes_agregadas as va
    left join violacoes_json as vj on va.id_usuario = vj.id_usuario
)

select * from final
