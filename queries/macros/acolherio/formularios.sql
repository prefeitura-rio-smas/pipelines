-- Macros de formularios: nomes e regras preservados.

{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    extra_where = '',
    table_alias = 'src'
) %}
(
    with evolucoes as (
        select
            {{ id_cols | join(', ') }},
            split(regexp_replace(
                {{ table_alias }}.{{ col_html }}, r'(?is)<h3\b[^>]*>', '\u001e'
            ), '\u001e') as secoes_html
        from {{ source_relation }} as {{ table_alias }}
        where regexp_contains({{ table_alias }}.{{ col_html }}, r'(?i)<b\b[^>]*>')
        {% if extra_where %}
            and {{ extra_where }}
        {% endif %}
    ),

    secoes as (
        select
            {{ id_cols | join(', ') }},
            secao,
            secoes_html[safe_offset(posicao + 1)] as proxima_secao
        from evolucoes, unnest(secoes_html) as secao with offset as posicao
        where posicao > 0
    ),

    formularios as (
        select
            {{ id_cols | join(', ') }},
            trim(regexp_replace(
                regexp_extract(secao, r'(?is)^(.*?)</h3\s*>'), r'<[^>]*>', ''
            )) as titulo_formulario,
            regexp_replace(secao, r'(?is)^.*?</h3\s*>', '') as bloco_campos,
            -- A seção seguinte pode conter as observações deste formulário.
            case
                when upper(trim(regexp_replace(
                    regexp_extract(proxima_secao, r'(?is)^(.*?)</h3\s*>'),
                    r'<[^>]*>', ''
                ))) in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
                    then regexp_replace(proxima_secao, r'(?is)^.*?</h3\s*>', '')
            end as observacoes
        from secoes
    ),

    campos as (
        select
            {{ id_cols | join(', ') }},
            titulo_formulario,
            observacoes,
            field
        from formularios,
            unnest(regexp_extract_all(
                regexp_replace(
                    bloco_campos,
                    r'(?is)</?(?:span|em|i|u|font|small|s|strike|mark|a)\b[^>]*>', ''
                ),
                r'(?is)([^<>]+?:?\s*<b\b[^>]*>.*?</b\s*>)'
            )) as field
        where upper(titulo_formulario) not in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
    )

    select
        {{ id_cols | join(', ') }},
        titulo_formulario,
        trim(regexp_replace(
            replace(regexp_extract(field, r'(?is)^(.*?)<b\b[^>]*>'), '&nbsp;', ' '),
            r':\s*$', ''
        )) as label,
        -- Tags de bloco separam palavras; tags de formatação só são removidas.
        trim(replace(replace(regexp_replace(
            regexp_replace(
                regexp_extract(field, r'(?is)<b\b[^>]*>(.*?)</b\s*>'),
                r'(?is)</?(?:br|p|div|li)\b[^>]*>', ' '
            ), r'<[^>]*>', ''
        ), '&nbsp;', ' '), '&amp;', '&')) as valor,
        observacoes
    from campos
)
{% endmacro %}

{% macro extrair_formulario(
    source_relation,
    group_cols,
    codigo_abrangencia,
    titulo_formulario,
    latest_by=none,
    flag_col=none,
    campos=[]
) %}
{% set id_cols = group_cols + ([latest_by] if latest_by else []) %}
{% set not_null_checks = group_cols | join(' is not null and ') %}
{% if not_null_checks %}
{% set extra_where = 'codigo_abrangencia = ' ~ codigo_abrangencia ~ ' and ' ~ not_null_checks ~ ' is not null' %}
{% else %}
{% set extra_where = 'codigo_abrangencia = ' ~ codigo_abrangencia %}
{% endif %}

(
    select
        {{ group_cols | join(', ') }},
        {% if flag_col %}
        true as {{ flag_col }},
        {% endif %}
        {% for campo in campos %}
        {%- set t = campo.type | default('string') -%}
        {%- set cn = campo.col -%}
        {%- if t == 'array' %}
        {%- set sep = campo.separator | default(',') %}
        case
            when b.{{ cn }}_raw is not null
            then array(
                select trim(x)
                from unnest(split(b.{{ cn }}_raw, '{{ sep }}')) as x
                where trim(x) != ''
            )
            else []
        end as {{ cn }}
        {%- elif t == 'array_agg' %}
        coalesce(b.{{ cn }}_raw, []) as {{ cn }}
        {%- elif t == 'boolean' %}
        {%- set true_val = campo.true_value | default('Sim') %}
        b.{{ cn }}_raw = '{{ true_val }}' as {{ cn }}
        {%- elif t == 'date' %}
        {%- set date_fmt = campo.format | default('%d/%m/%Y') %}
        safe.parse_date('{{ date_fmt }}', b.{{ cn }}_raw) as {{ cn }}
        {%- elif t == 'exists' %}
        b.{{ cn }}_raw is not null as {{ cn }}
        {%- elif t == 'number' %}
        {%- set cast_type = campo.cast_type | default('int64') %}
        safe_cast(b.{{ cn }}_raw as {{ cast_type }}) as {{ cn }}
        {%- else %}
        b.{{ cn }}_raw as {{ cn }}
        {%- endif %}
        {%- if not loop.last %},{% endif -%}
        {% endfor %}
    from (
        select
            {{ group_cols | join(', ') }},
            {% for campo in campos %}
            {%- if campo.type == 'array_agg' %}
            array_agg(distinct case when label like '{{ campo.label }}' and valor != 'undefined' and valor is not null then valor end ignore nulls) as {{ campo.col }}_raw
            {%- else %}
            max(case when label like '{{ campo.label }}' then valor end) as {{ campo.col }}_raw
            {%- endif %}
            {%- if not loop.last %},{% endif -%}
            {% endfor %}
        from (
            select
                {{ group_cols | join(', ') }}{% if latest_by %}, {{ latest_by }}{% endif %},
                label,
                valor
            from {{ extrair_campos_html_evolucao(
                source_relation = source_relation,
                id_cols = id_cols,
                col_html = 'descricao_evolucao',
                extra_where = extra_where
            ) }}
            where titulo_formulario = '{{ titulo_formulario }}'
            {% if latest_by %}
            qualify rank() over (
                partition by {{ group_cols | join(', ') }}
                order by {{ latest_by }} desc
            ) = 1
            {% endif %}
        )
        group by {{ group_cols | join(', ') }}
    ) b
)
{% endmacro %}

{% macro extrair_ultima_atualizacao(tabela) %}
    (select max(datetime(timestamp(data_extracao_origem), 'America/Sao_Paulo')) from {{ ref(tabela) }})
{% endmacro %}

{% macro map_label_violacao(titulo_formulario, regras=[]) %}
    CASE
        {% for r in regras %}
        WHEN (
            {% for t in r.titulos %}{{ titulo_formulario }} LIKE '%{{ t }}%'{% if not loop.last %} OR {% endif %}{% endfor %}
        ) THEN '{{ r.codigo }}'
        {% endfor %}
        ELSE NULL
    END
{% endmacro %}

{% macro map_resposta_sim_nao(coluna) %}
    case
        when lower(trim({{ coluna }})) in ('s', 'sim') then 'Sim'
        when lower(trim({{ coluna }})) in ('n', 'nao', 'não') then 'Não'
        else 'Não Informado'
    end
{% endmacro %}

{% macro map_violacao_direito_descricao(codigo) %}
    CASE
        WHEN {{ codigo }} = '01' THEN 'Violação Sexual'
        WHEN {{ codigo }} = '02' THEN 'Violência psicológica'
        WHEN {{ codigo }} = '03' THEN 'Violência financeira ou patrimonial'
        WHEN {{ codigo }} = '04' THEN 'Negligência e abandono'
        WHEN {{ codigo }} = '05' THEN 'Violência medicamentosa'
        WHEN {{ codigo }} = '06' THEN 'Violência doméstica'
        WHEN {{ codigo }} = '07' THEN 'Violência por tráfico de seres humanos'
        WHEN {{ codigo }} = '08' THEN 'Violência por trabalho escravo'
        WHEN {{ codigo }} = '09' THEN 'Violência institucional'
        WHEN {{ codigo }} = '10' THEN 'Adoção Ilegal'
        WHEN {{ codigo }} = '11' THEN 'Afastamento do convívio familiar devido a aplicação de MSE ou medida de proteção'
        WHEN {{ codigo }} = '12' THEN 'Auto Negligência'
        WHEN {{ codigo }} = '13' THEN 'Cárcere Privado'
        WHEN {{ codigo }} = '14' THEN 'Conflitos Territoriais / Conflitos Urbanos'
        WHEN {{ codigo }} = '15' THEN 'Discriminação Étnica/Racial'
        WHEN {{ codigo }} = '16' THEN 'Discriminação Religiosa'
        WHEN {{ codigo }} = '17' THEN 'Discriminação Sexual / Gênero'
        WHEN {{ codigo }} = '18' THEN 'Exploração Sexual'
        WHEN {{ codigo }} = '19' THEN 'Outras violências sexuais'
        WHEN {{ codigo }} = '20' THEN 'Trabalho Infantil'
        WHEN {{ codigo }} = '21' THEN 'Violência em razão de questões de saúde mental'
        WHEN {{ codigo }} = '22' THEN 'Violência Física'
        WHEN {{ codigo }} = '23' THEN 'Violência Obstétrica'
        WHEN {{ codigo }} = '24' THEN 'Vítima de Tráfico de Órgãos'
        WHEN {{ codigo }} = '25' THEN 'Situação de grave ameaça no território'
        WHEN {{ codigo }} = '28' THEN 'Violência Intrafamiliar'
        WHEN {{ codigo }} = '27' THEN 'Já Passou por Violações de direitos antes'
        WHEN {{ codigo }} = '26' THEN 'Outras Violações de Direitos'
        ELSE 'Código Desconhecido'
    END
{% endmacro %}
