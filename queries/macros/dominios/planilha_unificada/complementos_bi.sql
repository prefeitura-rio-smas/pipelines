{% macro pu_bi_enderecos_sql() %}
with foto as (
    select distinct id_usuario, endereco as logradouro, endereco_numero as numero,
        endereco_complemento as complemento, referencia_ou_comunidade
    from {{ pu_programada() }}
), cadastro as (
    select distinct * from {{ pu_ref('raw_usuarios_enderecos') }}
)
select u.id_usuario,
    struct(
        {% for campo in ['logradouro', 'numero', 'complemento', 'referencia_ou_comunidade'] %}
        if(f.id_usuario is not null, f.{{ campo }}, c.{{ campo }}) as {{ campo }}{% if not loop.last %},{% endif %}
{% endfor %}
    ) as endereco,
    case when f.id_usuario is not null then 'referencia_bi'
        when c.id_paciente is not null then 'cadastro' else 'sem_registro' end as origem_endereco
from {{ pu_ref('dim_usuarios') }} u
left join foto f on u.id_usuario = f.id_usuario
left join cadastro c on u.id_usuario = c.id_paciente
{% endmacro %}

{% macro pu_bi_complementos_sql() %}
with referencia as (
    select * from {{ pu_programada() }}
), eventos as (
    select distinct concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        id_usuario, cas, email_unidade, email_cas, email, cadastrante, data_cadastro_atendimento
    from referencia
), profissionais as (
    select distinct concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        profissional_id as id_profissional, id_familia as id_familia_referencia,
        id_atendimento as id_atendimento_legado
    from referencia where profissional_id is not null
),
profissionais_por_evento as (
    select id_atendimento_modulo,
        array_agg(struct(id_profissional, id_familia_referencia, id_atendimento_legado)
            order by id_profissional, id_familia_referencia, id_atendimento_legado) as profissionais
    from profissionais group by id_atendimento_modulo
)
select e.*, coalesce(p.profissionais, []) as profissionais
from eventos e left join profissionais_por_evento p on e.id_atendimento_modulo = p.id_atendimento_modulo
{% endmacro %}

{% test pu_bi_profissionais_unicos(model) %}
select c.id_atendimento_modulo, p.id_profissional
from {{ model }} c, unnest(c.profissionais) p
group by 1, 2
having count(*) != 1 or p.id_profissional is null
{% endtest %}
