{% macro pu_programada() %}
    {% set origem = source('acolherio_referencia', 'atendimentos_programada') %}
    {% if execute == true and var('pu_referencia', false) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier='atendimentos_programada') }}
    {% else %}
        {{ origem }}
    {% endif %}
{% endmacro %}

{% macro pu_atendimentos_referencia_sql() %}
with referencia as (
    select distinct
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        seq_atendimento as id_atendimento,
        id_usuario,
        case modulo when 'u' then 'usuario' when 'f' then 'familia' end as origem_modulo,
        unidade_atendimento as nome_unidade,
        nome_atendimento_original as tipo_atendimento_descricao,
        data_de_atendimento as data_atendimento,
        {{ normalizar_hora_atendimento('hora_de_atendimento') }} as hora_atendimento
    from {{ pu_programada() }}
),
origens as (
    select distinct id_atendimento_modulo, id_unidade, id_tipo_atendimento, flag_cancelado
    from {{ pu_ref('raw_atendimentos_usuarios') }}
    union distinct
    select distinct id_atendimento_modulo, id_unidade, id_tipo_atendimento, flag_cancelado
    from {{ pu_ref('raw_atendimentos_familias') }}
),
eventos as (
    select r.*, o.id_unidade, o.id_tipo_atendimento, o.flag_cancelado,
        {{ pu_tipo_servico('u.tipo_unidade') }} as tipo_servico
    from referencia as r
    left join origens as o on r.id_atendimento_modulo = o.id_atendimento_modulo
    left join {{ pu_ref('dim_unidades') }} as u on o.id_unidade = u.id_unidade
),
ocupacoes_distintas as (
    select distinct p.id_profissional, cast(p.codigo_cbo as string) as codigo_cbo, c.descricao
    from {{ pu_ref('raw_profissionais_ocupacoes') }} as p
    left join {{ pu_ref('raw_cbo') }} as c on p.codigo_cbo = c.codigo_cbo
),
ocupacoes as (
    select id_profissional,
        array_agg(codigo_cbo ignore nulls order by codigo_cbo, descricao) as codigos_cbo,
        array_agg(descricao ignore nulls order by codigo_cbo, descricao) as descricoes_cbo
    from ocupacoes_distintas group by id_profissional
),
profissionais_distintos as (
    select distinct
        concat(cast(r.seq_atendimento as string), r.modulo) as id_atendimento_modulo,
        r.profissional_id as id_profissional,
        r.profissional as nome_profissional,
        coalesce(o.codigos_cbo, []) as codigos_cbo,
        coalesce(o.descricoes_cbo, []) as descricoes_cbo,
        o.codigos_cbo[safe_offset(0)] as cbo_principal_codigo,
        r.profissional_cbo_original as cbo_principal_descricao
    from {{ pu_programada() }} as r
    left join ocupacoes as o on r.profissional_id = o.id_profissional
    where r.profissional_id is not null
),
profissionais as (
    select id_atendimento_modulo,
        array_agg(struct(
            id_profissional, nome_profissional, codigos_cbo, descricoes_cbo,
            cbo_principal_codigo, cbo_principal_descricao
        ) order by id_profissional, nome_profissional, to_json_string(codigos_cbo),
            to_json_string(descricoes_cbo), cbo_principal_codigo, cbo_principal_descricao) as profissionais
    from profissionais_distintos group by id_atendimento_modulo
)
select e.id_usuario,
    array_agg(struct(
        e.id_atendimento_modulo, e.id_atendimento, e.origem_modulo, e.id_unidade,
        e.nome_unidade, e.tipo_servico, e.id_tipo_atendimento, e.tipo_atendimento_descricao,
        e.data_atendimento, e.hora_atendimento, e.flag_cancelado,
        coalesce(p.profissionais, []) as profissionais
    ) order by e.data_atendimento, e.hora_atendimento, e.id_atendimento_modulo,
        e.id_unidade, e.id_tipo_atendimento, e.tipo_atendimento_descricao, e.flag_cancelado) as atendimentos
from eventos as e
left join profissionais as p on e.id_atendimento_modulo = p.id_atendimento_modulo
group by e.id_usuario
{% endmacro %}
