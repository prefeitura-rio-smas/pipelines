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

violacoes_cadastro as (
    select
        id_usuario,
        'cadastro' as origem,
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

campos_evolucao as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes)",
        id_cols = ['id_usuario', 'id_evolucao', 'data_evolucao', 'origem_modulo'],
        col_html = 'descricao_evolucao'
    ) }}
),

violacoes_evolucao as (
    select
        id_usuario,
        'evolucao' as origem,
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
    from campos_evolucao
    where
        label is not null
        and trim(label) != ''
),

violacoes_unificadas as (
    select * from violacoes_cadastro
    union all
    select * from violacoes_evolucao
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

resumo_violacoes as (
    select
        id_usuario,
        array_agg(
            struct(
                codigo,
                descricao,
                origens
            )
            order by codigo
        ) as todas_violacoes
    from violacoes_por_codigo
    group by id_usuario
),

violacoes_json as (
    select
        vp.id_usuario,
        array_agg(
            to_json(
                struct(
                    vp.codigo,
                    vp.descricao,
                    vp.origens,
                    vp.evidencias,
                    rv.todas_violacoes
                )
            )
            order by vp.codigo
        ) as violacoes_json
    from violacoes_por_codigo as vp
    left join resumo_violacoes as rv on vp.id_usuario = rv.id_usuario
    group by vp.id_usuario
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
