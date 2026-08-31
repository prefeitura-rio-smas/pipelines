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

traducao as (
    select
        id_usuario,
        codigo,
        {{ map_violacao_direito_descricao('codigo') }} as descricao
    from codigos_separados
    where codigo != ''
),

evolucoes_administrativas as (
    select
        id_paciente as id_usuario,
        id_evolucao,
        cast(data_evolucao as datetime) as data_evolucao,
        cast(data_cancelamento as datetime) as data_cancelamento,
        descricao_evolucao,
        'administrativa' as origem_modulo
    from {{ ref('raw_evolucoes_administrativas') }}
),

evolucoes_usuarios as (
    select
        id_usuario,
        id_evolucao,
        cast(data_evolucao as datetime) as data_evolucao,
        cast(data_cancelamento as datetime) as data_cancelamento,
        descricao_evolucao,
        'usuario' as origem_modulo
    from {{ ref('raw_evolucoes_usuarios') }}
),

evolucoes_familias as (
    select
        id_paciente as id_usuario,
        id_evolucao,
        cast(data_evolucao as datetime) as data_evolucao,
        cast(data_cancelamento as datetime) as data_cancelamento,
        descricao_evolucao,
        'familia' as origem_modulo
    from {{ ref('raw_evolucoes_familias') }}
),

evolucoes_grupo as (
    select
        safe_cast(trim(pac) as int64) as id_usuario,
        id_evolucao,
        cast(data_evolucao as datetime) as data_evolucao,
        cast(data_cancelamento as datetime) as data_cancelamento,
        descricao_evolucao,
        'grupo' as origem_modulo
    from {{ ref('raw_evolucoes_grupo') }}
    cross join
        unnest(
            split(replace(replace(lista_pacientes, '(', ''), ')', ''), ',')
        ) as pac
    where
        lista_pacientes != '()'
        and lista_pacientes is not null
        and lista_pacientes != ''
        and trim(pac) != ''
),

formularios as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes_administrativas)",
        id_cols = ['id_usuario', 'id_evolucao', 'data_evolucao', 'data_cancelamento', 'origem_modulo'],
        col_html = 'descricao_evolucao'
    ) }}
    union all
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes_usuarios)",
        id_cols = ['id_usuario', 'id_evolucao', 'data_evolucao', 'data_cancelamento', 'origem_modulo'],
        col_html = 'descricao_evolucao'
    ) }}
    union all
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes_familias)",
        id_cols = ['id_usuario', 'id_evolucao', 'data_evolucao', 'data_cancelamento', 'origem_modulo'],
        col_html = 'descricao_evolucao'
    ) }}
    union all
    select * from {{ extrair_campos_html_evolucao(
        source_relation = "(select * from evolucoes_grupo)",
        id_cols = ['id_usuario', 'id_evolucao', 'data_evolucao', 'data_cancelamento', 'origem_modulo'],
        col_html = 'descricao_evolucao'
    ) }}
),

formularios_ativos as (
    select
        id_usuario,
        id_evolucao,
        data_evolucao,
        origem_modulo,
        titulo_formulario,
        label,
        valor
    from formularios
    where
        data_cancelamento is null
        and id_usuario is not null
),

campos_evolucao as (
    select
        id_usuario,
        id_evolucao,
        origem_modulo,
        max(data_evolucao) as data_evolucao,
        max(titulo_formulario) as titulo_formulario,
        array_agg(
            struct(
                label,
                valor
            )
            order by label
        ) as campos
    from formularios_ativos
    group by id_usuario, id_evolucao, origem_modulo
),

ocorrencias as (
    select
        id_usuario,
        array_agg(
            struct(
                id_evolucao,
                origem_modulo,
                data_evolucao,
                titulo_formulario,
                campos
            )
            order by data_evolucao
        ) as ocorrencias
    from campos_evolucao
    group by id_usuario
),

violacoes_agregadas as (
    select
        id_usuario,
        array_agg(
            struct(
                codigo,
                descricao
            )
        ) as violacoes
    from traducao
    group by id_usuario
),

final as (
    select
        v.violacoes,
        o.ocorrencias,
        coalesce(v.id_usuario, o.id_usuario) as id_usuario
    from violacoes_agregadas as v
    full outer join ocorrencias as o on v.id_usuario = o.id_usuario
)

select * from final
