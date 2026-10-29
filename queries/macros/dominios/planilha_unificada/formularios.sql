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
),

datas as (
    select f.*, m.titulo_formulario is not null as mapeado,
        array(select c.valor from unnest(f.campos) c where trim(c.label) in (
            select label from unnest(m.labels) label
        ) and nullif(trim(c.valor), '') is not null) as valores_datas
    from formularios f
    left join (select titulo_formulario, array_agg(label_data_ocorrencia) labels from {{ ref('formularios_datas_ocorrencia') }} group by titulo_formulario) m using(titulo_formulario)
),

datas_parseadas as (
    select d.*, array(select as struct valor, {{ data_ocorrencia_parse('valor') }} as data from unnest(valores_datas) valor) as datas
    from datas d
),

datas_avaliadas as (
    select d.*,
        case when not mapeado then 'nao_mapeado'
            when array_length(datas) = 0 then 'ausente'
            when exists(select 1 from unnest(datas) where data is null) then 'invalida'
            when (select count(distinct data) from unnest(datas)) > 1 then 'conflitante'
            else 'valida' end as situacao_data_ocorrencia
    from datas_parseadas d
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
        campos,
        if(situacao_data_ocorrencia = 'valida', (select min(data) from unnest(datas)), cast(data_evolucao as date)) as data_ocorrencia,
        if(situacao_data_ocorrencia = 'valida', 'formulario', 'fonte') as origem_data_ocorrencia,
        situacao_data_ocorrencia
    ) order by data_evolucao, id_evolucao_modulo, titulo_formulario, observacoes, id_unidade, id_profissional, data_cancelamento) as formularios
from datas_avaliadas
group by id_usuario

{% endmacro %}
