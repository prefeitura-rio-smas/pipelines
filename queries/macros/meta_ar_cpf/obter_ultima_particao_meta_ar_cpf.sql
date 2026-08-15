-- Macro da última partição do meta_ar_cpf (padrão cadunico/filtro_particao_cadunico).
-- Retorna a data da partição mais recente da tabela _dev informada.
-- Uso: {{ obter_ultima_particao_meta_ar_cpf('rj-smas.pequenos_cariocas.meta_acordo_resultados_cpf') }}
{% macro obter_ultima_particao_meta_ar_cpf(tabela) %}
    {%- set m = run_query("select max(data_particao) from `" ~ tabela ~ "`").columns[0].values()[0] if execute else none -%}
    {%- do return(m) -%}
{% endmacro %}
