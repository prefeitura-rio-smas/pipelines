with violacoes_cadastro as (
    select
        id_paciente as id_usuario,
        'cadastro' as origem,
        trim(codigo) as codigo,
        {{ map_violacao_direito_descricao('codigo') }} as descricao
    from {{ ref('raw_usuarios_detalhes') }},
        unnest(split(violacao_direito, ',')) as codigo
    where
        violacao_direito is not null
        and violacao_direito != ''
        and violacao_direito != 'N'
        and trim(codigo) != ''
),

evolucoes as (
    select
        id_usuario,
        descricao_evolucao
    from {{ ref('int_evolucoes') }}
    where
        data_cancelamento is null
        and id_usuario is not null
),

campos_evolucao as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes)",
        id_cols = ['id_usuario'],
        col_html = 'descricao_evolucao'
    ) }}
),

violacoes_evolucao as (
    select
        id_usuario,
        origem,
        codigo,
        {{ map_violacao_direito_descricao('codigo') }} as descricao
    from (
        select
            id_usuario,
            'evolucao' as origem,
            {{ map_label_violacao(
                titulo_formulario = 'titulo_formulario',
                regras = [
                    {'codigo': '20', 'titulos': ['Adolescente em trabalho', 'PETI']},
                    {'codigo': '11', 'titulos': ['MSE - Medidas Socioeducativas', 'MSE - RETORNO', 'Penas e Medidas Alternativas']}
                ]
            ) }} as codigo
        from campos_evolucao
        where
            label is not null
            and trim(label) != ''
    )
    where codigo is not null
),

unificadas as (
    select
        id_usuario,
        origem,
        codigo,
        descricao
    from violacoes_cadastro
    union all
    select
        id_usuario,
        origem,
        codigo,
        descricao
    from violacoes_evolucao
),

agregadas as (
    select
        id_usuario,
        array_agg(
            struct(codigo, descricao, origem)
            order by origem, codigo
        ) as violacoes
    from unificadas
    where id_usuario is not null
    group by id_usuario
),

origens_por_codigo as (
    select
        id_usuario,
        codigo,
        max(descricao) as descricao,
        array_agg(distinct origem order by origem) as origens
    from unificadas
    where
        id_usuario is not null
        and codigo is not null
    group by id_usuario, codigo
),

por_codigo as (
    select
        id_usuario,
        array_agg(
            struct(codigo, descricao, origens)
            order by codigo
        ) as violacoes_por_origem
    from origens_por_codigo
    group by id_usuario
),

violacoes_json as (
    select
        id_usuario,
        array_agg(
            to_json(
                struct(codigo, descricao, origens)
            )
            order by codigo
        ) as violacoes_json
    from origens_por_codigo
    group by id_usuario
),

final as (
    select
        a.id_usuario,
        a.violacoes,
        p.violacoes_por_origem,
        j.violacoes_json
    from agregadas as a
    left join por_codigo as p on a.id_usuario = p.id_usuario
    left join violacoes_json as j on a.id_usuario = j.id_usuario
)

select * from final
