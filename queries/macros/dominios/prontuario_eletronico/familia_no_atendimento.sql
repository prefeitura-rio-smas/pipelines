{% macro pu_ids_familias(coluna) -%}
    coalesce(string_agg(distinct cast({{ coluna }} as string), ', ' order by cast({{ coluna }} as string)), '')
{%- endmacro %}

{% test pu_familia_no_atendimento_paridade(model) %}
with esperado as (
    select
        cast(id_usuario as string) as id_usuario,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        {{ pu_ids_familias('id_familia') }} as id_familia
    from (
with eventos as (
    select
        'u' as modulo,
        id_atendimento,
        id_unidade,
        id_tipo_atendimento,
        flag_cancelado
    from {{ pu_ref('raw_atendimentos_usuarios') }}
    union all
    select
        'f' as modulo,
        id_atendimento,
        id_unidade,
        id_tipo_atendimento,
        flag_cancelado
    from {{ pu_ref('raw_atendimentos_familias') }}
),

contextos as (
    select
        modulo,
        id_atendimento,
        if(
            count(distinct to_json_string(struct(id_unidade, id_tipo_atendimento, flag_cancelado))) > 1,
            error('Contextos internos conflitantes para atendimento da programada'),
            array_agg(struct(id_unidade, id_tipo_atendimento, flag_cancelado) limit 1)[offset(0)]
        ) as contexto
    from eventos
    group by modulo, id_atendimento
),

cbos as (
    select
        p.id_profissional,
        c.descricao,
        if(count(distinct p.codigo_cbo) = 1, max(p.codigo_cbo), null) as codigo_cbo
    from {{ pu_ref('raw_profissionais_ocupacoes') }} as p
    inner join {{ pu_ref('raw_cbo') }} as c on p.codigo_cbo = c.codigo_cbo
    group by p.id_profissional, c.descricao
),

selecionados as (
    select
        s.*,
        e.contexto.id_unidade as _id_unidade,
        e.contexto.id_tipo_atendimento as _id_tipo_atendimento,
        e.contexto.flag_cancelado as _flag_cancelado,
        c.codigo_cbo as _codigo_cbo
    from {{ ref('raw_atendimentos_programada') }} as s
    left join contextos as e on s.modulo = e.modulo and s.seq_atendimento = e.id_atendimento
    left join cbos as c on s.profissional_id = c.id_profissional and s.profissional_cbo_original = c.descricao
),

enriquecidos as (
    {{ aplicar_filtro_email('selecionados', pu_ref('raw_unidades'), pu_filtro_email()) }}
),

datas as (
    select
        modulo,
        id_atendimento,
        id_usuario,
        id_unidade,
        data_atendimento,
        origem_data_atendimento
    from {{ ref('int_datas_atendimento') }}
)

select
    b.modulo,
    b.cas,
    b.tipo_unidade,
    b.unidade_atendimento,
    b.email_unidade,
    b.email_cas,
    b.seq_atendimento,
    datas.data_atendimento as data_de_atendimento,
    b.hora_de_atendimento_original,
    b.hora_de_atendimento,
    b.id_usuario,
    b.id_familia,
    b.nome_usuario,
    b.documentacao,
    b.numero_documento,
    b.endereco,
    b.endereco_numero,
    b.endereco_complemento,
    b.bairro,
    b.referencia_ou_comunidade,
    b.nome_atendimento_original,
    b.data_nascimento,
    b.idade,
    b.raca_cor,
    b.sexo,
    b.profissional_id,
    b.profissional,
    b.profissional_cbo_original,
    b.profissional_cbo,
    b.cadastrante,
    b.data_cadastro_atendimento,
    b.tipo_atendimento,
    b.nome_atendimento,
    b.idade_faixa,
    b.id_atendimento,
    b.email,
    b.cbo_unico_rank,
    b.atendimento_unico_rank,
    b.projetos_sociais,
    b._id_unidade,
    b._id_tipo_atendimento,
    b._flag_cancelado,
    b._codigo_cbo,
    b.data_de_atendimento as _data_de_atendimento_base,
    coalesce(datas.origem_data_atendimento, 'sem_correspondencia') as _origem_data_atendimento
from enriquecidos as b
left join datas on
    b.modulo = datas.modulo and b.seq_atendimento = datas.id_atendimento
    and b.id_usuario = datas.id_usuario and b._id_unidade = datas.id_unidade

)
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
