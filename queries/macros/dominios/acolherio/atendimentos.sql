{% macro normalizar_profissional_atendimento(coluna) -%}
    safe_cast(trim(cast({{ coluna }} as string)) as int64)
{%- endmacro %}

{% macro selecionar_participacoes_atendimento(relacao, campos_comuns=none, campos_internos=none) %}
    {% set campos_comuns = campos_comuns if campos_comuns is not none else [
        'modulo', 'cas', 'tipo_unidade', 'unidade_atendimento', 'email_unidade', 'email_cas',
        'seq_atendimento', 'data_de_atendimento', 'hora_de_atendimento_original', 'hora_de_atendimento',
        'id_usuario', 'id_familia', 'nome_usuario', 'documentacao', 'numero_documento',
        'endereco', 'endereco_numero', 'endereco_complemento', 'bairro', 'referencia_ou_comunidade',
        'nome_atendimento_original', 'data_nascimento', 'idade', 'raca_cor', 'sexo',
        'profissional_id', 'profissional', 'profissional_cbo_original', 'profissional_cbo',
        'cadastrante', 'data_cadastro_atendimento', 'tipo_atendimento', 'nome_atendimento',
        'idade_faixa', 'id_atendimento', 'email'
    ] %}
    {% set campos_internos = campos_internos if campos_internos is not none else [
        '_id_unidade', '_id_tipo_atendimento', '_flag_cancelado'
    ] %}
    {% set desempate_comum = 'to_json_string(struct(a.' ~ campos_comuns | join(', a.') ~ ')) asc' %}
    {% set ordem_interna = [] %}
    {% if campos_internos %}
        {% set ordem_interna = ['to_json_string(struct(a.' ~ campos_internos | join(', a.') ~ ')) asc'] %}
    {% endif %}
    {% set ordem = [
        'a.data_cadastro_atendimento asc nulls first',
        'a.seq_atendimento asc',
        'a.modulo asc',
        desempate_comum
    ] %}
    select
        a.*,
        -- Cópias idênticas podem liderar o primeiro ranking. O segundo mantém só uma.
        rank() over (
            partition by id_atendimento
            order by
                {{ ordem | join(",\n                ") }}
        ) as cbo_unico_rank,
        row_number() over (
            partition by
                profissional_id, hora_de_atendimento, data_de_atendimento,
                nome_atendimento_original, id_usuario, unidade_atendimento
            order by
                {{ (ordem + ordem_interna) | join(",\n                ") }}
        ) as atendimento_unico_rank
    from {{ relacao }} as a
    qualify cbo_unico_rank = 1 and atendimento_unico_rank = 1
{% endmacro %}

{% macro agrupar_atendimentos() %}
with linhas_preparadas as (
    with cbos as (
        select
            p.id_profissional,
            c.descricao,
            if(count(distinct p.codigo_cbo) = 1, max(p.codigo_cbo), null) as codigo_cbo
        from {{ ref('raw_profissionais_ocupacoes') }} as p
        inner join {{ ref('raw_cbo') }} as c on p.codigo_cbo = c.codigo_cbo
        group by p.id_profissional, c.descricao
    )

    select
        b.modulo,
        b.cas,
        b.tipo_unidade,
        b.unidade_atendimento,
        b.email_unidade,
        b.email_cas,
        b.seq_atendimento,
        b.data_de_atendimento,
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
        c.codigo_cbo as _codigo_cbo
    from {{ ref('int_acolherio_atendimentos') }} as b
    left join cbos as c on b.profissional_id = c.id_profissional and b.profissional_cbo_original = c.descricao

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
