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
        -- AED: 'N' vaza como item do split (ex.: campo '20,N'); filtra no item,
        -- nao apenas no campo inteiro
        and trim(codigo) != 'N'
),

campos_evolucao as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = ref('int_evolucoes'),
        id_cols = ['id_usuario'],
        col_html = 'descricao_evolucao',
        extra_where = 'src.data_cancelamento is null and src.id_usuario is not null'
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

-- Dedup real: 1 item por (id_usuario, codigo). A multiplicidade de evolucoes,
-- de campos do formulario e o cadastro sujo ('20,20') colapsam no par
-- (id_usuario, codigo). A origem converge para 'ambas' quando o codigo aparece
-- nas duas origens; senao mantem a origem unica.
-- Codigos desconhecidos (ex.: '1204', '2104' no cadastro) sao MANTIDOS com
-- descricao 'Codigo Desconhecido' - decisao de barra-los nao foi tomada pelo
-- Leone; apenas documentada aqui.
deduplicadas as (
    select
        id_usuario,
        codigo,
        max(descricao) as descricao,
        case
            when count(distinct origem) = 2 then 'ambas'
            else max(origem)
        end as origem
    from unificadas
    where
        id_usuario is not null
        and codigo is not null
    group by id_usuario, codigo
),

agregadas as (
    select
        id_usuario,
        array_agg(
            struct(codigo, descricao, origem)
            order by codigo
        ) as violacoes
    from deduplicadas
    group by id_usuario
),

final as (
    select
        id_usuario,
        violacoes
    from agregadas
)

select * from final
