{# Recebe expressões SQL das três fontes, na ordem CAS, planilha e unidade. #}
{% macro filtro_email(email_cas, email_planilha, email_unidade, formato='padrao') -%}
    {%- if formato == 'programada' -%}
    concat(
        coalesce({{ email_cas }}, ''), ',',
        coalesce({{ email_unidade }}, ''), ',',
        coalesce({{ email_planilha }}, '')
    )
    {%- else -%}
    concat(
        coalesce({{ email_cas }}, ''), ',',
        coalesce({{ email_planilha }}, ''), ', ',
        coalesce({{ email_unidade }}, '')
    )
    {%- endif -%}
{%- endmacro %}

{# Enriquece registros sem multiplicá-los; as relações são fornecidas pelo chamador. #}
{% macro aplicar_filtro_email(registros, unidades, planilha) %}
with unidades as (
    select
        id_unidade,
        if(count(distinct to_json_string(struct(cas, email_unidade))) > 1,
            error('E-mails ou CAS conflitantes para a mesma unidade'),
            array_agg(struct(cas, email_unidade) limit 1)[offset(0)]) as dados
    from {{ unidades }}
    group by id_unidade
), planilha as (
    select
        lower(trim(unidade_atendimento)) as nome_unidade,
        if(count(distinct to_json_string(struct(email))) > 1,
            error('E-mails da planilha conflitantes para a mesma unidade'),
            max(email)) as email_planilha
    from {{ planilha }}
    group by 1
), emails as (
    select
        s.* except (email_cas, email_unidade, email),
        coalesce({{ email_cas('u.dados.cas') }}, 'gdados.smas@prefeitura.rio') as email_cas,
        u.dados.email_unidade,
        p.email_planilha
    from {{ registros }} as s
    left join unidades as u on s._id_unidade = u.id_unidade
    left join planilha as p on lower(trim(s.unidade_atendimento)) = p.nome_unidade
)
select
    * except (email_planilha),
    {{ filtro_email('email_cas', 'email_planilha', 'email_unidade', formato='programada') }} as email
from emails
{% endmacro %}
