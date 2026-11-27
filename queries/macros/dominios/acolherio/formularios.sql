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
            {{ decodificar_entidades_html('c.valor') }} as valor
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
    left join (select titulo_formulario, array_agg(label_data_ocorrencia) labels from unnest([
        struct("Centro POP - Atendimento Social" as titulo_formulario, "Data do atendimento" as label_data_ocorrencia),
        struct("Centro POP - Plano de Atendimento Individual (PAI)" as titulo_formulario, "Data do atendimento" as label_data_ocorrencia),
        struct("Centro POP - Desligamento PAI" as titulo_formulario, "Data do desligamento" as label_data_ocorrencia),
        struct("Escritório Social - Registro de evolução" as titulo_formulario, "Data do atendimento" as label_data_ocorrencia),
        struct("Escritório Social - Identificação" as titulo_formulario, "Data do atendimento" as label_data_ocorrencia),
        struct("Lares Cariocas - Plano de desenvolvimento individual e familiar" as titulo_formulario, "Data do atendimento" as label_data_ocorrencia),
        struct("1. Pequenos Cariocas - Busca Ativa" as titulo_formulario, "Data em que a Busca Ativa foi realizada" as label_data_ocorrencia),
        struct("Lares Cariocas - Entrevista inicial de avaliação" as titulo_formulario, "Data da entrevista" as label_data_ocorrencia),
        struct("Tô de Boa - Justificativa de Falta" as titulo_formulario, "Data da falta" as label_data_ocorrencia),
        struct("Tô de Boa - Cancelamento das atividades" as titulo_formulario, "Data do cancelamento" as label_data_ocorrencia),
        struct("CRAS - PAIF - Plano de Acompanhamento Familiar" as titulo_formulario, "Data do atendimento inicial do Acompanhamento Familiar" as label_data_ocorrencia),
        struct("CREAS - PAEFI - Plano de Acompanhamento Familiar" as titulo_formulario, "Data de inclusão no PAEFI" as label_data_ocorrencia),
        struct("CREAS - MSE - Medidas Socioeducativas" as titulo_formulario, "Data de inclusão no MSE" as label_data_ocorrencia),
        struct("CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil" as titulo_formulario, "Data da inclusão no PETI" as label_data_ocorrencia),
        struct("CREAS - MSE - RETORNO" as titulo_formulario, "Data do desligamento" as label_data_ocorrencia),
        struct("CRAS - PAIF - RETORNO - Plano de Acompanhamento Familiar" as titulo_formulario, "Data do desligamento" as label_data_ocorrencia),
        struct("CREAS - PAEFI - RETORNO - Plano de Acompanhamento Familiar" as titulo_formulario, "Data do desligamento" as label_data_ocorrencia),
        struct("Lares Cariocas - Locação do imóvel" as titulo_formulario, "Data da assinatura do termo de compromisso" as label_data_ocorrencia),
        struct("Lares Cariocas - Locação do imóvel" as titulo_formulario, "Data da entrega das chaves/entrada no imóvel" as label_data_ocorrencia)
    ]) group by titulo_formulario) m using(titulo_formulario)
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
    -- A identidade usa o HTML original; a apresentação é normalizada uma vez.
    observacoes as observacoes_html,
    {{ texto_html_legivel('observacoes') }} as observacoes,
    campos,
    if(situacao_data_ocorrencia = 'valida', (select min(data) from unnest(datas)), cast(data_evolucao as date)) as data_ocorrencia,
    if(situacao_data_ocorrencia = 'valida', 'formulario', 'fonte') as origem_data_ocorrencia,
    situacao_data_ocorrencia
from datas_avaliadas

{% endmacro %}

{% macro agrupar_formularios(relation) %}
select id_usuario,
    array_agg(
        (select as struct f.* except (id_usuario))
        order by data_evolucao, id_evolucao_modulo, titulo_formulario, observacoes_html, id_unidade, id_profissional, data_cancelamento
    ) as formularios
from {{ relation }} as f
group by id_usuario
{% endmacro %}
