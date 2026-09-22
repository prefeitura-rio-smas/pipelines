-- Macro genérica de e-mail: consolida N fontes (unidade OU pessoa) em UMA
-- string canônica, no grão do chamador.
--
-- Contrato:
--   * A macro NÃO faz join nem agregação por grão. Recebe expressões JÁ
--     RESOLVIDAS pelo modelo (CASE do CAS, planilha já agregada por unidade,
--     cadastro da unidade, e-mail da conta/profissional) e devolve a string
--     normalizada. Quem junta e agrega é a dim/mart chamadora.
--   * O nome da COLUNA de saída é sempre o do modelo (email, email_unidade,
--     email_filtro, email_cas, email_planilha) — a macro não renomeia nada.
--
-- Normalização (determinística):
--   Fontes concatenadas com concat(coalesce(f,''), ',', ...) — concat_ws NAO existe
--   neste projeto BigQuery (Function not found: CONCAT_WS).
--   split por r'[,;\n\r\t]' -> trim -> lower -> descarta vazio ('' / espaços)
--   -> DISTINCT -> ORDER BY asc -> string_agg(', ')
--   -> se nenhum token sobrou: NULL (NUNCA ',,' , nunca ',, ', nunca '')
--
-- Casos de borda:
--   * token malformado (ex.: 'craftjobim.smas@prefeitura rio', com espaço no
--     domínio) é PRESERVADO: e-mail é endereço, não descartamos em silêncio;
--   * 'cas' com caixa alta ou string vazia: resolvido em filtro_email_cas
--     (lower(trim()) + coalesce -> NULL quando não é 01..10);
--   * pessoa (1 e-mail) = lista de 1 token: mesmo caminho, sem caso especial;
--   * ordem alfabética garante diff zero entre builds (o string_agg sem
--     ORDER BY da dim era não determinístico e sujava o Recce).
--
-- Uso:
--   {{ filtro_email(['pe.email_planilha', 'un.email_unidade']) }}
--   {{ filtro_email([filtro_email_cas('un.cas'), 'email_planilha']) }}
--   {{ filtro_email(['ope.email']) }}          -- pessoa (1 token)

{% macro filtro_email(fontes) %}
    {%- set expressoes = [] -%}
    {%- for f in fontes -%}
        {%- do expressoes.append("coalesce(" ~ f ~ ", '')") -%}
    {%- endfor -%}
    nullif(
        (
            select string_agg(x, ', ' order by x)
            from (
                select distinct lower(trim(token)) as x
                from unnest(regexp_extract_all(concat({{ expressoes | join(", ',', ") }}), r'[^,;\n\r\t]+')) as token
                where trim(token) is not null
                  and trim(token) != ''
            )
        ),
        ''
    )
{%- endmacro %}


-- Macro do CAS: código do território (gh_us.apus / coluna 'cas') -> e-mail do
-- CAS. Variante canônica, idêntica à que dim_unidades já usava: lower(trim()).
-- Cobre caixa alta e string vazia. Sem 'else' por design: GE / sem AP -> NULL
-- (não inventa e-mail).
{% macro filtro_email_cas(coluna) %}
    case lower(trim(coalesce({{ coluna }}, '')))
        when '01' then 'cas1@prefeitura.rio'
        when '02' then 'cas2@prefeitura.rio'
        when '03' then 'cas3@prefeitura.rio'
        when '04' then 'cas4@prefeitura.rio'
        when '05' then 'cas5@prefeitura.rio'
        when '06' then 'cas6@prefeitura.rio'
        when '07' then 'cas7@prefeitura.rio'
        when '08' then 'cas8@prefeitura.rio'
        when '09' then 'cas9@prefeitura.rio'
        when '10' then 'cas10@prefeitura.rio'
    end
{%- endmacro %}
