{{ config(materialized='table', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Preserva a seleção publicada; enriquece IDs internos e recalcula os e-mails.
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
)

select
    modulo,
    cas,
    tipo_unidade,
    unidade_atendimento,
    email_unidade,
    email_cas,
    seq_atendimento,
    data_de_atendimento,
    hora_de_atendimento_original,
    hora_de_atendimento,
    id_usuario,
    id_familia,
    nome_usuario,
    documentacao,
    numero_documento,
    endereco,
    endereco_numero,
    endereco_complemento,
    bairro,
    referencia_ou_comunidade,
    nome_atendimento_original,
    data_nascimento,
    idade,
    raca_cor,
    sexo,
    profissional_id,
    profissional,
    profissional_cbo_original,
    profissional_cbo,
    cadastrante,
    data_cadastro_atendimento,
    tipo_atendimento,
    nome_atendimento,
    idade_faixa,
    id_atendimento,
    email,
    cbo_unico_rank,
    atendimento_unico_rank,
    projetos_sociais,
    _id_unidade,
    _id_tipo_atendimento,
    _flag_cancelado,
    _codigo_cbo
from enriquecidos
