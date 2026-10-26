{% macro texto_html_legivel(coluna) %}
-- Remove marcação antes de decodificar: &lt;exemplo&gt; permanece texto literal.
{% set limpo %}
regexp_replace(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(cast({{ coluna }} as string), r'(?s)<!--.*?-->', ''),
                r'(?is)<script\b[^>]*>.*?</script\s*>|<style\b[^>]*>.*?</style\s*>', ''
            ),
            r'(?i)<br\b[^>]*>|</(?:p|div|h[1-6]|li|tr|blockquote)\s*>', '\n'
        ),
        r'<[^>]+>', ''
    ), r'\r\n?', '\n'
)
{% endset %}
trim(regexp_replace(coalesce({{ decodificar_entidades_html(limpo) }}, ''), r'\n[ \t]*\n(?:[ \t]*\n)+', '\n\n'))
{% endmacro %}
