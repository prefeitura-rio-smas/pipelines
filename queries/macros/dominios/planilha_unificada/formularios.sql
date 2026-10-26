{% macro pu_formularios_sql(source_relation) %}
with campos_extraidos as (
    select *
    from {{ extrair_campos_html_evolucao(
        source_relation = source_relation,
        id_cols = ['id_usuario', 'id_evolucao_modulo', 'id_evolucao', 'origem_modulo',
                   'id_familia', 'id_unidade', 'id_profissional', 'codigo_abrangencia',
                   'id_atividade', 'data_evolucao', 'data_cancelamento', 'tipo_evolucao',
                   'nome_profissional', 'nome_atividade']
    ) }}
),

-- Primeiro interpreta a estrutura HTML; só depois normaliza os textos. Assim,
-- &lt; e &gt; literais não passam a ser tratados como tags do formulário.
campos as (
    select distinct
        c.* replace (
            {{ decodificar_entidades_html('c.titulo_formulario') }} as titulo_formulario,
            {{ decodificar_entidades_html('c.label') }} as label,
            {{ decodificar_entidades_html('c.valor') }} as valor,
            {{ decodificar_entidades_html('c.observacoes') }} as observacoes
        )
    from campos_extraidos as c
),

formularios as (
    select
        id_usuario,
        id_evolucao_modulo,
        id_evolucao,
        origem_modulo,
        id_familia,
        id_unidade,
        id_profissional,
        codigo_abrangencia,
        id_atividade,
        data_evolucao,
        data_cancelamento,
        tipo_evolucao,
        nome_profissional,
        nome_atividade,
        titulo_formulario,
        observacoes,
        array_agg(struct(label, valor) order by label, valor) as campos
    from campos
    group by
        id_usuario, id_evolucao_modulo, id_evolucao, origem_modulo,
        id_familia, id_unidade, id_profissional, codigo_abrangencia,
        id_atividade, data_evolucao, data_cancelamento, tipo_evolucao,
        titulo_formulario, observacoes, nome_profissional, nome_atividade
)

select
    id_usuario,
    array_agg(struct(
        id_evolucao_modulo,
        id_evolucao,
        origem_modulo,
        id_familia,
        id_unidade,
        id_profissional,
        codigo_abrangencia,
        id_atividade,
        data_evolucao,
        data_cancelamento,
        tipo_evolucao,
        nome_profissional,
        nome_atividade,
        titulo_formulario,
        observacoes,
        campos
    ) order by data_evolucao, id_evolucao_modulo, titulo_formulario, observacoes, id_unidade, id_profissional, data_cancelamento) as formularios
from formularios
group by id_usuario

{% endmacro %}
