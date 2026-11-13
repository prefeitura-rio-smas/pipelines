select
    b.id_usuario,
    b.seq_atendimento,
    b.modulo
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
            ocupacao.id_profissional,
            c.descricao,
            if(count(distinct ocupacao.codigo_cbo) = 1, max(ocupacao.codigo_cbo), null) as codigo_cbo
        from {{ pu_ref('raw_profissionais_ocupacoes') }} as ocupacao
        inner join {{ pu_ref('raw_cbo') }} as c on ocupacao.codigo_cbo = c.codigo_cbo
        group by ocupacao.id_profissional, c.descricao
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
        b.* replace (datas.data_atendimento as data_de_atendimento),
        b.data_de_atendimento as _data_de_atendimento_base,
        coalesce(datas.origem_data_atendimento, 'sem_correspondencia') as _origem_data_atendimento
    from enriquecidos as b
    left join datas on
        b.modulo = datas.modulo and b.seq_atendimento = datas.id_atendimento
        and b.id_usuario = datas.id_usuario and b._id_unidade = datas.id_unidade

) as b
left join {{ ref('int_datas_atendimento') }} as d
    on
        b.modulo = d.modulo and b.seq_atendimento = d.id_atendimento
        and b.id_usuario = d.id_usuario and b._id_unidade = d.id_unidade
where
    b.data_de_atendimento is distinct from d.data_atendimento
    or (b._origem_data_atendimento != 'atendimento' and b.data_de_atendimento is not null)
union all
select
    id_usuario,
    id_atendimento,
    modulo
from {{ ref('int_datas_atendimento') }}
group by modulo, id_atendimento, id_usuario, id_unidade
having count(*) != 1
