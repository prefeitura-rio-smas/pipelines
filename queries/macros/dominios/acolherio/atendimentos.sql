{% macro normalizar_profissional_atendimento(coluna) -%}
    safe_cast(trim(cast({{ coluna }} as string)) as int64)
{%- endmacro %}

{% macro agrupar_atendimentos() %}
with linhas_preparadas as (
    with eventos_origem as (
        select
            modulo,
            id_atendimento,
            id_paciente as id_usuario,
            id_unidade,
            id_tipo_atendimento,
            flag_cancelado,
            data_atendimento
        from {{ ref('raw_atendimentos_usuarios') }}
        union all
        select
            modulo,
            id_atendimento,
            id_paciente as id_usuario,
            id_unidade,
            id_tipo_atendimento,
            flag_cancelado,
            data_atendimento
        from {{ ref('raw_atendimentos_familias') }}
    ),

    contextos as (
        select
            modulo,
            id_atendimento,
            if(
                count(distinct to_json_string(struct(id_unidade, id_tipo_atendimento, flag_cancelado))) > 1,
                error('Contextos internos conflitantes para atendimento selecionado'),
                array_agg(struct(id_unidade, id_tipo_atendimento, flag_cancelado) limit 1)[offset(0)]
            ) as contexto
        from eventos_origem
        group by modulo, id_atendimento
    ),

    cbos as (
        select
            p.id_profissional,
            c.descricao,
            if(count(distinct p.codigo_cbo) = 1, max(p.codigo_cbo), null) as codigo_cbo
        from {{ ref('raw_profissionais_ocupacoes') }} as p
        inner join {{ ref('raw_cbo') }} as c on p.codigo_cbo = c.codigo_cbo
        group by p.id_profissional, c.descricao
    ),

    selecionados as (
        select
            s.*,
            e.contexto.id_unidade as _id_unidade,
            e.contexto.id_tipo_atendimento as _id_tipo_atendimento,
            e.contexto.flag_cancelado as _flag_cancelado,
            c.codigo_cbo as _codigo_cbo
        from {{ ref('int_acolherio_atendimentos') }} as s
        left join contextos as e on s.modulo = e.modulo and s.seq_atendimento = e.id_atendimento
        left join cbos as c on s.profissional_id = c.id_profissional and s.profissional_cbo_original = c.descricao
    ),

    enriquecidos as (
        {{ aplicar_filtro_email('selecionados', ref('raw_unidades'), source('dashboard_acolherio', 'filtro_email_dev')) }}
    ),

    datas as (
        select
            modulo,
            id_atendimento,
            id_usuario,
            id_unidade,
            if(count(distinct data_atendimento) > 1, error('Datas conflitantes para atendimento'), min(data_atendimento)) as data_atendimento
        from eventos_origem
        group by modulo, id_atendimento, id_usuario, id_unidade
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
        b.hora_de_atendimento,
        b.id_usuario,
        b.id_familia,
        b.nome_atendimento_original,
        b.profissional_id,
        b.profissional,
        b.profissional_cbo_original,
        b.profissional_cbo,
        b.cadastrante,
        b.data_cadastro_atendimento,
        b.tipo_atendimento,
        b.nome_atendimento,
        b.id_atendimento,
        b.email,
        b.projetos_sociais,
        b._id_unidade,
        b._id_tipo_atendimento,
        b._flag_cancelado,
        b._codigo_cbo
    from enriquecidos as b
    left join datas on
        b.modulo = datas.modulo and b.seq_atendimento = datas.id_atendimento
        and b.id_usuario = datas.id_usuario and b._id_unidade = datas.id_unidade

),

eventos as (
    select  -- noqa: ST06
        id_usuario,
        seq_atendimento as id_atendimento,
        _id_unidade as id_unidade,
        unidade_atendimento as nome_unidade,
        _id_tipo_atendimento as id_tipo_atendimento,
        nome_atendimento_original as tipo_atendimento_descricao,
        data_de_atendimento as data_atendimento,
        hora_de_atendimento as hora_atendimento,
        _flag_cancelado as flag_cancelado,
        cas,
        email_unidade,
        email_cas,
        email,
        tipo_unidade,
        if(tipo_unidade in ('CRAS', 'CREAS', 'CENTRO POP') and unidade_atendimento != 'Negado', 'Sim', 'Não') as elegivel_indicador,
        cadastrante,
        data_cadastro_atendimento,
        concat(cast(seq_atendimento as string), modulo) as id_atendimento_modulo,
        case modulo when 'u' then 'usuario' when 'f' then 'familia' end as origem_modulo,
        case tipo_unidade
            when 'CRAS' then 'cras' when 'CREAS' then 'creas'
            when 'CENTRO POP' then 'centro_pop' else 'outros'
        end as tipo_servico,
        coalesce(string_agg(distinct cast(id_familia as string), ', ' order by cast(id_familia as string)), '') as id_familia,
        max(projetos_sociais) as projetos_sociais_familia,
        case
            when countif(tipo_atendimento = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
            when countif(tipo_atendimento = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
            else 'Outros Atendimentos'
        end as classificacao_atendimento,
        array_agg(struct(
            profissional_id as id_profissional,
            profissional as nome_profissional,
            array(
                select cast(codigo as string)
                from unnest([_codigo_cbo]) as codigo
                where codigo is not null
            ) as codigos_cbo,
            array(
                select descricao
                from unnest([profissional_cbo_original]) as descricao
                where descricao is not null
            ) as descricoes_cbo,
            cast(_codigo_cbo as string) as cbo_principal_codigo,
            profissional_cbo_original as cbo_principal_descricao,
            profissional_cbo,
            tipo_atendimento as classificacao_atendimento,
            nome_atendimento,
            id_familia as id_familia_referencia,
            id_atendimento as id_atendimento_legado
        ) order by profissional_id, id_atendimento) as profissionais,
        if(
            count(distinct nullif(trim(cast(profissional_id as string)), '')) >= 2,
            'Sim',
            'Não'
        ) as atendimento_compartilhado
    from linhas_preparadas
    where {{ nao_cancelado('_flag_cancelado') }}
    group by
        id_usuario, id_atendimento_modulo, id_atendimento, origem_modulo,
        id_unidade, nome_unidade, tipo_servico, id_tipo_atendimento,
        tipo_atendimento_descricao, data_atendimento, hora_atendimento, flag_cancelado,
        cas, email_unidade, email_cas, email, cadastrante, data_cadastro_atendimento, tipo_unidade
)

select
    id_usuario,
    array_agg((select as struct e.* except (id_usuario)) order by data_atendimento, hora_atendimento, id_atendimento_modulo) as atendimentos
from eventos as e
group by id_usuario
{% endmacro %}
