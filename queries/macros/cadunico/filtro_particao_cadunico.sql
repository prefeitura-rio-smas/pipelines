{% macro obter_particao_cadunico() %}
    {%- set m = run_query("select parse_date('%Y%m', max(partition_id)) from `rj-smas.protecao_social_cadunico.documento_membro$__PARTITIONS_SUMMARY__`").columns[0].values()[0] if execute else none -%}
    {%- do return(m) -%}
{% endmacro %}

{% macro filtro_particao_cadunico() %}
    {%- set p = obter_particao_cadunico() -%}
    {%- if p -%}data_particao >= date('{{ p }}') and data_particao < date_add(date('{{ p }}'), interval 1 month){%- endif -%}
{% endmacro %}
