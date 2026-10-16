-- Preserva respostas datadas, sem transformar marcações de PMA/PAI em vínculos
-- ativos de LA/PSC ou acompanhamento. Um formulário pode conter várias respostas.
with evolucoes as (
    select
        e.origem_modulo,
        e.id_evolucao,
        e.id_familia,
        e.id_unidade,
        e.data_evolucao,
        e.data_cancelamento,
        e.codigo_abrangencia,
        e.tipo_evolucao,
        e.descricao_evolucao,
        e.id_atividade,
        cast(null as string) as lista_participantes_original,
        concat(e.origem_modulo, ':', cast(e.id_evolucao as string)) as id_evolucao_modulo,
        coalesce(e.id_usuario, e.id_paciente_familia) as id_usuario
    from {{ ref('int_evolucoes') }} as e
    where e.origem_modulo != 'grupo'

    union all

    -- int_evolucoes expande grupos por participante e remove cancelados.
    -- Aqui o formulário pertence à evolução original, sem essa multiplicação.
    select
        'grupo' as origem_modulo,
        e.id_evolucao,
        cast(null as int64) as id_familia,
        a.id_unidade,
        e.data_evolucao,
        cast(e.data_cancelamento as datetime) as data_cancelamento,
        e.codigo_abrangencia,
        e.tipo_evolucao,
        e.descricao_evolucao,
        e.id_atividade,
        e.lista_pacientes as lista_participantes_original,
        concat('grupo:', cast(e.id_evolucao as string)) as id_evolucao_modulo,
        cast(null as int64) as id_usuario
    from {{ ref('raw_evolucoes_grupo') }} as e
    left join {{ ref('raw_atividades_grupo') }} as a on e.id_atividade = a.id_atividade
),

campos as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = 'evolucoes',
        id_cols = [
            'id_evolucao_modulo', 'origem_modulo', 'id_evolucao', 'id_usuario',
            'id_familia', 'id_unidade', 'data_evolucao', 'data_cancelamento',
            'codigo_abrangencia', 'tipo_evolucao', 'id_atividade', 'lista_participantes_original'
        ]
    ) }}
),

respostas_distintas as (
    select distinct
        id_evolucao_modulo,
        origem_modulo,
        id_evolucao,
        id_usuario,
        id_familia,
        id_unidade,
        data_evolucao,
        data_cancelamento,
        codigo_abrangencia,
        tipo_evolucao,
        id_atividade,
        lista_participantes_original,
        titulo_formulario,
        label,
        valor,
        observacoes
    from campos
)

select
    'FORMULARIO' as tipo_registro,
    concat('prontuario:evolucao:', origem_modulo) as origem_registro,
    {{ dbt_utils.generate_surrogate_key(['id_evolucao_modulo', 'titulo_formulario']) }} as id_registro_origem,
    id_usuario,
    id_familia,
    id_unidade,
    date(data_evolucao) as data_evento,
    {{ inicio_mes('data_evolucao') }} as competencia,
    date(data_evolucao) as data_referencia,
    struct(
        id_evolucao_modulo,
        origem_modulo,
        id_evolucao,
        data_evolucao,
        data_cancelamento,
        codigo_abrangencia,
        tipo_evolucao,
        id_atividade,
        lista_participantes_original,
        titulo_formulario,
        array_agg(struct(label, valor, observacoes) order by label, valor, observacoes) as respostas
    ) as formulario
from respostas_distintas
group by
    id_evolucao_modulo, origem_modulo, id_evolucao, id_usuario,
    id_familia, id_unidade, data_evolucao, data_cancelamento,
    codigo_abrangencia, tipo_evolucao, id_atividade, lista_participantes_original, titulo_formulario
