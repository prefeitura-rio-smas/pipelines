{% macro pu_familia_no_atendimento_sql(source_relation) %}
-- Só publica um valor escalar quando os valores conhecidos concordam.
-- Repetições por profissional e família ausente não escolhem uma família nova.
select
    id_usuario,
    concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
    if(
        count(distinct id_familia) > 1,
        error('Mais de uma familia na referencia do atendimento'),
        coalesce(max(cast(id_familia as string)), '')
    ) as id_familia,
    if(
        count(distinct coalesce(email, '')) > 1,
        error('Emails conflitantes na referencia do atendimento'),
        max(coalesce(email, ''))
    ) as email
from {{ source_relation }}
group by id_usuario, id_atendimento_modulo
{% endmacro %}

{% test pu_familia_no_atendimento_paridade(model) %}
with esperado as (
    select
        cast(id_usuario as string) as id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        coalesce(string_agg(distinct cast(id_familia as string)), '') as id_familia,
        max(coalesce(email, '')) as email
    from {{ pu_programada() }}
    group by id_usuario, id_atendimento_modulo
), obtido as (
    select p.usuario.id_usuario, a.id_atendimento_modulo, a.id_familia, a.email
    from {{ model }} as p, unnest(p.atendimentos.registros) as a
), comparacao as (
    -- A referência precisa estar integralmente representada no fato existente.
    select e.id_usuario, e.id_atendimento_modulo
    from esperado as e
    left join obtido as o using (id_usuario, id_atendimento_modulo)
    group by e.id_usuario, e.id_atendimento_modulo, e.id_familia, e.email
    having count(o.id_atendimento_modulo) != 1
        or countif(o.id_familia = e.id_familia and o.email = e.email) != 1
    union all
    select o.id_usuario, o.id_atendimento_modulo
    from obtido as o
    left join esperado as e using (id_usuario, id_atendimento_modulo)
    where e.id_atendimento_modulo is null and o.id_familia != ''
)
select * from comparacao
{% endtest %}
