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
