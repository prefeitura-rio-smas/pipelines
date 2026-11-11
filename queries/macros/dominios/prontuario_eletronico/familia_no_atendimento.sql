{% macro pu_ids_familias(coluna) -%}
    coalesce(string_agg(distinct cast({{ coluna }} as string), ', ' order by cast({{ coluna }} as string)), '')
{%- endmacro %}

{% test pu_familia_no_atendimento_paridade(model) %}
with esperado as (
    select
        cast(id_usuario as string) as id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        {{ pu_ids_familias('id_familia') }} as id_familia
    from {{ ref('int_atendimentos_bi') }}
    group by id_usuario, id_atendimento_modulo
), obtido as (
    select p.usuario.id_usuario, a.id_atendimento_modulo, a.id_familia
    from {{ model }} as p, unnest(p.atendimentos.registros) as a
), comparacao as (
    -- A referência precisa estar integralmente representada no fato existente.
    select e.id_usuario, e.id_atendimento_modulo
    from esperado as e
    left join obtido as o using (id_usuario, id_atendimento_modulo)
    group by e.id_usuario, e.id_atendimento_modulo, e.id_familia
    having count(o.id_atendimento_modulo) != 1
        or countif(o.id_familia = e.id_familia) != 1
    union all
    select o.id_usuario, o.id_atendimento_modulo
    from obtido as o
    left join esperado as e using (id_usuario, id_atendimento_modulo)
    where e.id_atendimento_modulo is null and o.id_familia != ''
)
select * from comparacao
{% endtest %}
