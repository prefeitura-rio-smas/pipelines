{% macro obter_particao_cadunico() %}
    {%- if execute -%}
        {%- set query = "
            select
                parse_date('%Y%m', max(partition_id)) as mes_inicio,
                date_add(parse_date('%Y%m', max(partition_id)), interval 1 month) as mes_fim
            from `rj-smas.protecao_social_cadunico.documento_membro$__PARTITIONS_SUMMARY__`
        " -%}
        {%- set result = run_query(query) -%}
        {%- do return({"mes_inicio": result.columns[0].values()[0], "mes_fim": result.columns[1].values()[0]}) -%}
    {%- else -%}
        {%- do return({"mes_inicio": none, "mes_fim": none}) -%}
    {%- endif -%}
{% endmacro %}

{% macro filtro_particao_cadunico() %}
    {%- set p = obter_particao_cadunico() -%}
    {%- if execute -%}
        data_particao >= date('{{ p.mes_inicio }}') and data_particao < date('{{ p.mes_fim }}')
    {%- else -%}
        data_particao >= date('1900-01-01')
    {%- endif -%}
{% endmacro %}
