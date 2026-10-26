{% if var('pu_referencia', false) %}
{{ pu_atendimentos_referencia_sql() }}
{% else %}
with candidatos as (
    select
        a.id_usuario,
        a.id_atendimento_modulo,
        a.id_atendimento,
        a.origem_modulo,
        a.id_unidade,
        un.nome_unidade,
        {{ pu_tipo_servico('un.tipo_unidade') }} as tipo_servico,
        a.id_tipo_atendimento,
        a.tipo_atendimento_descricao,
        a.data_atendimento,
        a.hora_atendimento,
        a.flag_cancelado,
        a.id_profissional,
        p.nome as nome_profissional,
        coalesce(p.codigos_cbo, []) as codigos_cbo,
        p.cbo_principal_codigo,
        p.cbo_principal_descricao,
        coalesce(p.descricoes_cbo, []) as descricoes_cbo
    from {{ pu_ref('fct_atendimentos') }} as a
    left join {{ pu_ref('dim_profissionais') }} as p on a.id_profissional_sk = p.id_profissional_sk
    left join {{ pu_ref('dim_unidades') }} as un on a.id_unidade = un.id_unidade
    where
        a.id_usuario is not null
        and upper(trim(coalesce(a.flag_cancelado, ''))) != 'S'
),

-- Deduplica apenas cópias idênticas dos atributos do evento. Conflitos
-- permanecem visíveis nos testes de identidade, sem escolha arbitrária.
eventos as (
    select distinct
        id_usuario,
        id_atendimento_modulo,
        id_atendimento,
        origem_modulo,
        id_unidade,
        nome_unidade,
        tipo_servico,
        id_tipo_atendimento,
        tipo_atendimento_descricao,
        data_atendimento,
        hora_atendimento,
        flag_cancelado
    from candidatos
),

profissionais_distintos as (
    select distinct
        id_usuario,
        id_atendimento_modulo,
        id_profissional,
        nome_profissional,
        codigos_cbo,
        descricoes_cbo,
        cbo_principal_codigo,
        cbo_principal_descricao
    from candidatos
    where id_profissional is not null
),

profissionais as (
    select
        id_usuario,
        id_atendimento_modulo,
        array_agg(struct(
            id_profissional,
            nome_profissional,
            codigos_cbo,
            descricoes_cbo,
            cbo_principal_codigo,
            cbo_principal_descricao
        ) order by id_profissional, nome_profissional, to_json_string(codigos_cbo), cbo_principal_codigo, cbo_principal_descricao) as profissionais
    from profissionais_distintos
    group by id_usuario, id_atendimento_modulo
)

select
    e.id_usuario,
    array_agg(struct(
        e.id_atendimento_modulo,
        e.id_atendimento,
        e.origem_modulo,
        e.id_unidade,
        e.nome_unidade,
        e.tipo_servico,
        e.id_tipo_atendimento,
        e.tipo_atendimento_descricao,
        e.data_atendimento,
        e.hora_atendimento,
        e.flag_cancelado,
        coalesce(p.profissionais, []) as profissionais
    ) order by e.data_atendimento, e.hora_atendimento, e.id_atendimento_modulo, e.id_unidade, e.id_tipo_atendimento, e.tipo_atendimento_descricao, e.flag_cancelado) as atendimentos
from eventos as e
left join profissionais as p on e.id_usuario = p.id_usuario and e.id_atendimento_modulo = p.id_atendimento_modulo
group by e.id_usuario

{% endif %}
