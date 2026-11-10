{% macro pu_bi_enderecos_sql() %}
with foto as (
    select distinct id_usuario, endereco as logradouro, endereco_numero as numero,
        endereco_complemento as complemento, referencia_ou_comunidade
    from {{ ref('int_atendimentos_bi') }}
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
