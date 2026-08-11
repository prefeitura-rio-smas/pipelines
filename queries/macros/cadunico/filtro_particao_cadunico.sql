{% macro obter_particao_cadunico() %}
    {%- set m = run_query("select max(data_particao) from `rj-smas.protecao_social_cadunico.prefeitura`").columns[0].values()[0] if execute else none -%}
    {%- do return(m) -%}
{% endmacro %}

{% macro filtro_particao_cadunico() %}
    {%- set p = obter_particao_cadunico() -%}
    {%- if p -%}data_particao = date('{{ p }}'){%- endif -%}
{% endmacro %}
