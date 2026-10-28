{% macro pu_cpop_texto(expr) %}
    case when lower(trim({{ expr }})) in ('', '-', 'undefined', 'null', 'não informado', 'nao informado', 'não informada', 'nao informada') then null else trim({{ expr }}) end
{% endmacro %}

{% macro pu_cpop_data(expr) %}
    coalesce(
        safe.parse_date('%d/%m/%Y', regexp_extract(trim({{ expr }}), r'^\d{2}/\d{2}/\d{4}')),
        safe_cast(regexp_extract(trim({{ expr }}), r'^\d{4}-\d{2}-\d{2}') as date)
    )
{% endmacro %}

{% macro pu_cpop_valores(campos, labels, modo='texto') %}
    array(
        select distinct valor from (
            select
                {% if modo == 'flag' %}
                    case lower(trim(c.valor)) when 's' then 'Sim' when 'sim' then 'Sim' when 'n' then 'Não' when 'não' then 'Não' when 'nao' then 'Não' end
                {% elif modo == 'data' %}
                    cast({{ pu_cpop_data('c.valor') }} as string)
                {% else %}
{{ pu_cpop_texto('c.valor') }}
{% endif %} as valor
            from unnest({{ campos }}) as c
            where regexp_replace(trim(c.label), r':\s*$', '') in (
                {% for label in labels %}'{{ label }}'{% if not loop.last %}, {% endif %}{% endfor %}
            )
        ) where valor is not null order by valor
    )
{% endmacro %}

{% macro pu_cpop_campos() %}
    {{ return({
        'data_inclusao_pai': 'DATE', 'data_inclusao_informada': 'STRING',
        'motivo_principal_permanencia_rua': 'STRING', 'motivo_secundario_permanencia_rua': 'STRING',
        'possui_referencias_familiares': 'STRING', 'territorio_referencia_familia': 'STRING',
        'possibilidade_reinsercao_familiar': 'STRING', 'uso_substancias_psicoativas': 'STRING',
        'situacao_saude': 'STRING', 'local_tratamento': 'STRING',
        'possui_problema_saude': 'STRING', 'realiza_acompanhamento_saude': 'STRING',
        'data_desligamento': 'DATE', 'data_desligamento_informada': 'STRING',
        'motivo_desligamento': 'STRING', 'motivo_desligamento_outros': 'STRING',
        'demandas': 'ARRAY<STRING>', 'encaminhamentos': 'ARRAY<STRUCT<label STRING, valor STRING>>',
        'observacoes': 'STRING', 'empregabilidade_imediata': 'STRING', 'capacidade_habilidades': 'STRING',
        'interesse_curso': 'STRING', 'resultado_acesso': 'STRING', 'resultado_descricao': 'STRING',
        'id_atividade': 'INT64', 'nome_atividade': 'STRING', 'data_presenca': 'DATE', 'hora_presenca': 'STRING'
    }) }}
{% endmacro %}

{% macro pu_cpop_classificacao(evento) %}
    case when {{ evento }}.tipo_servico != 'centro_pop' then ''
        when exists (select 1 from unnest({{ evento }}.profissionais) p, unnest(p.codigos_cbo) codigo where starts_with(trim(codigo), '2')) then 'Atendimento Técnico'
        when {{ evento }}.tipo_atendimento_descricao like '%Recepção%'
            or exists (select 1 from unnest({{ evento }}.profissionais) p where p.nome_profissional = 'ATENDIMENTO RECEPÇÃO'
                or (p.cbo_principal_descricao in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista') and {{ evento }}.tipo_atendimento_descricao like '%CadÚnico%')) then 'Atendimento Recepção'
        else 'Outros Atendimentos'
    end
{% endmacro %}

{% macro pu_cpop_registros_sql() %}
with formularios as (
    select p.id_usuario, f.*, posicao
    from {{ pu_ref('int_pu_formularios') }} p, unnest(p.formularios) f with offset posicao
),
presencas as (
    select distinct p.id_usuario, p.id_unidade, p.id_atividade,
        safe_cast(p.data_presenca as date) data_presenca,
        {{ normalizar_hora_atendimento('cast(p.hora_presenca as string)') }} hora_presenca, a.nome_atividade
    from {{ pu_ref('raw_presencas_usuarios') }} p
    join {{ pu_ref('dim_unidades') }} u on p.id_unidade = u.id_unidade and u.tipo_unidade = 'Centro POP'
    join {{ pu_ref('dim_usuarios') }} c on p.id_usuario = c.id_usuario
    left join {{ pu_ref('raw_atividades_grupo') }} a on p.id_atividade = a.id_atividade
),
publico as (
    select p.id_usuario from {{ pu_ref('int_pu_atendimentos') }} p, unnest(p.atendimentos) a where a.tipo_servico = 'centro_pop'
    union distinct select id_usuario from formularios where titulo_formulario in ('Centro POP - Plano de Atendimento Individual (PAI)', 'Centro POP - Atendimento Social', 'Centro POP - Desligamento PAI')
    union distinct select id_usuario from presencas
),
valores as (
    select f.*,
        {{ pu_cpop_valores('f.campos', ['Data do atendimento'], 'data') }} inclusoes,
        {{ pu_cpop_valores('f.campos', ['Data do atendimento']) }} inclusoes_originais,
        {{ pu_cpop_valores('f.campos', ['Motivo secundário da ida às ruas']) }} motivos_rua,
        {{ pu_cpop_valores('f.campos', ['Possui referências familiares?'], 'flag') }} referencias,
        {{ pu_cpop_valores('f.campos', ['Território da referência familiar']) }} territorios,
        {{ pu_cpop_valores('f.campos', ['Existe possibilidade de reinserção familiar?'], 'flag') }} reinsercoes,
        {{ pu_cpop_valores('f.campos', ['Faz uso de substâncias psicoativas?'], 'flag') }} substancias,
        {{ pu_cpop_valores('f.campos', ['Situação']) }} situacoes,
        {{ pu_cpop_valores('f.campos', ['Local onde faz tratamento']) }} tratamentos,
        {{ pu_cpop_valores('f.campos', ['Data do desligamento'], 'data') }} desligamentos,
        {{ pu_cpop_valores('f.campos', ['Data do desligamento']) }} desligamentos_originais,
        {{ pu_cpop_valores('f.campos', ['Motivo do desligamento']) }} motivos_desligamento,
        {{ pu_cpop_valores('f.campos', ['Outros']) }} outros_desligamento,
        {{ pu_cpop_valores('f.campos', ['Demanda inicial', 'Identificação das demandas apresentadas pelo usuário']) }} demandas_extraidas,
        array(select distinct as struct regexp_replace(trim(c.label), r':\s*$', '') label, {{ pu_cpop_texto('c.valor') }} valor
            from unnest(f.campos) c where starts_with(trim(c.label), 'Encaminhamentos') and {{ pu_cpop_texto('c.valor') }} is not null order by label, valor) encaminhamentos_extraidos
    from formularios f join publico p on f.id_usuario = p.id_usuario
    join {{ pu_ref('dim_usuarios') }} u on f.id_usuario = u.id_usuario
    where f.data_cancelamento is null and f.titulo_formulario in (
        'Centro POP - Plano de Atendimento Individual (PAI)', 'Centro POP - Atendimento Social', 'Centro POP - Desligamento PAI', 'Situação de Saúde'
    )
),
html as (
    select v.id_usuario, v.id_unidade, 'formulario_html' origem,
        case v.titulo_formulario when 'Situação de Saúde' then 'saude' when 'Centro POP - Plano de Atendimento Individual (PAI)' then 'pai' when 'Centro POP - Desligamento PAI' then 'desligamento' else 'atendimento_social' end tipo_registro,
        if(v.titulo_formulario = 'Situação de Saúde', 'pessoa', 'unidade') abrangencia,
        v.id_evolucao_modulo, v.titulo_formulario,
        cast(v.data_evolucao as datetime) data_registro, cast(v.data_evolucao as datetime) data_resposta,
        to_json_string(struct(v.id_evolucao_modulo, v.id_unidade, v.titulo_formulario, v.observacoes, v.campos)) chave_origem,
        {% set valores_html = {
            'data_inclusao_pai': "if(v.titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)', safe_cast(if(array_length(v.inclusoes) = 1, v.inclusoes[safe_offset(0)], null) as date), null)",
            'data_inclusao_informada': "if(v.titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)', array_to_string(v.inclusoes_originais, ' | '), null)",
            'motivo_secundario_permanencia_rua': 'if(array_length(v.motivos_rua) = 1, v.motivos_rua[safe_offset(0)], null)',
            'possui_referencias_familiares': 'if(array_length(v.referencias) = 1, v.referencias[safe_offset(0)], null)', 'territorio_referencia_familia': 'if(array_length(v.territorios) = 1, v.territorios[safe_offset(0)], null)',
            'possibilidade_reinsercao_familiar': 'if(array_length(v.reinsercoes) = 1, v.reinsercoes[safe_offset(0)], null)', 'uso_substancias_psicoativas': 'if(array_length(v.substancias) = 1, v.substancias[safe_offset(0)], null)',
            'situacao_saude': "nullif(array_to_string(v.situacoes, ' | '), '')", 'local_tratamento': "nullif(array_to_string(v.tratamentos, ' | '), '')",
            'data_desligamento': 'safe_cast(if(array_length(v.desligamentos) = 1, v.desligamentos[safe_offset(0)], null) as date)', 'data_desligamento_informada': "array_to_string(v.desligamentos_originais, ' | ')",
            'motivo_desligamento': 'if(array_length(v.motivos_desligamento) = 1, v.motivos_desligamento[safe_offset(0)], null)', 'motivo_desligamento_outros': 'if(array_length(v.outros_desligamento) = 1, v.outros_desligamento[safe_offset(0)], null)',
            'demandas': 'v.demandas_extraidas', 'encaminhamentos': 'v.encaminhamentos_extraidos'
        } %}
        {% set grupos_html = {
            'data_inclusao_pai': 'Centro POP - Plano de Atendimento Individual (PAI)', 'data_inclusao_informada': 'Centro POP - Plano de Atendimento Individual (PAI)',
            'motivo_secundario_permanencia_rua': 'Centro POP - Atendimento Social', 'possui_referencias_familiares': 'Centro POP - Atendimento Social',
            'territorio_referencia_familia': 'Centro POP - Atendimento Social', 'possibilidade_reinsercao_familiar': 'Centro POP - Atendimento Social',
            'uso_substancias_psicoativas': 'Situação de Saúde', 'situacao_saude': 'Situação de Saúde', 'local_tratamento': 'Situação de Saúde',
            'data_desligamento': 'Centro POP - Desligamento PAI', 'data_desligamento_informada': 'Centro POP - Desligamento PAI',
            'motivo_desligamento': 'Centro POP - Desligamento PAI', 'motivo_desligamento_outros': 'Centro POP - Desligamento PAI'
        } %}
{% for campo, tipo in pu_cpop_campos().items() %}
{% if campo == 'possui_problema_saude' or campo == 'realiza_acompanhamento_saude' %}
                {% set respostas = 'v.situacoes' if campo == 'possui_problema_saude' else 'v.tratamentos' %}
                case (select logical_or({{ interpretar_resposta_saude('resposta') }}) from unnest({{ respostas }}) resposta) when true then 'Sim' when false then 'Não' end
            {% elif campo == 'observacoes' %}
                {{ pu_cpop_texto(texto_html_legivel('v.observacoes')) }}
            {% elif campo in valores_html %}
{% if campo in grupos_html %}if(v.titulo_formulario = '{{ grupos_html[campo] }}', {{ valores_html[campo] }}, cast(null as {{ tipo }})){% else %}{{ valores_html[campo] }}{% endif %}
            {% else %}cast(null as {{ tipo }}){% endif %} as {{ campo }},
        {% endfor %}
        (select countif(tamanho > 1) from unnest([
            if(v.titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)', array_length(v.inclusoes), 0),
            if(v.titulo_formulario = 'Centro POP - Atendimento Social', array_length(v.motivos_rua), 0),
            if(v.titulo_formulario = 'Centro POP - Atendimento Social', array_length(v.referencias), 0),
            if(v.titulo_formulario = 'Centro POP - Atendimento Social', array_length(v.territorios), 0),
            if(v.titulo_formulario = 'Centro POP - Atendimento Social', array_length(v.reinsercoes), 0),
            if(v.titulo_formulario = 'Situação de Saúde', array_length(v.substancias), 0),
            if(v.titulo_formulario = 'Centro POP - Desligamento PAI', array_length(v.desligamentos), 0),
            if(v.titulo_formulario = 'Centro POP - Desligamento PAI', array_length(v.motivos_desligamento), 0),
            if(v.titulo_formulario = 'Centro POP - Desligamento PAI', array_length(v.outros_desligamento), 0)
        ]) tamanho) conflitos
    from valores v
),
questionarios as (
    select distinct q.id_prontuario id_usuario, cast(null as int64) id_unidade, 'questionario_estruturado' origem,
        'situacao_rua' tipo_registro, 'pessoa' abrangencia, cast(null as string) id_evolucao_modulo,
        'Situação do usuário / motivo da ida às ruas' titulo_formulario,
        cast(q.data_evolucao as datetime) data_registro, cast(r.data_resposta as datetime) data_resposta,
        to_json_string(struct(q.id_modulo, q.id_template, q.id_evolucao, r.id_questao, r.data_resposta, r.resposta)) chave_origem,
        {% for campo, tipo in pu_cpop_campos().items() %}
{% if campo == 'motivo_principal_permanencia_rua' %}{{ pu_cpop_texto('r.resposta') }}{% else %}cast(null as {{ tipo }}){% endif %} as {{ campo }},
        {% endfor %}
        0 conflitos
    from {{ pu_ref('raw_evolucoes_questionario') }} q
    join {{ pu_ref('raw_evolucoes_questionario_lista') }} r on q.id_evolucao = r.id_evolucao and q.id_template = r.id_template and q.id_modulo is not distinct from r.id_modulo
    join publico p on q.id_prontuario = p.id_usuario
    join {{ pu_ref('dim_usuarios') }} u on q.id_prontuario = u.id_usuario
    where q.id_template = 3 and r.id_questao = 18 and {{ pu_cpop_texto('r.resposta') }} is not null
),
atividades as (
    select p.id_usuario, p.id_unidade, 'presenca' origem, 'participacao' tipo_registro, 'unidade' abrangencia,
        cast(null as string) id_evolucao_modulo, 'Presença em atividade coletiva' titulo_formulario,
        cast(p.data_presenca as datetime) data_registro, cast(p.data_presenca as datetime) data_resposta,
        to_json_string(struct(p.id_atividade, p.data_presenca, p.hora_presenca, p.id_unidade)) chave_origem,
        {% for campo, tipo in pu_cpop_campos().items() %}
{% if campo in ['id_atividade', 'nome_atividade', 'data_presenca', 'hora_presenca'] %}p.{{ campo }}{% else %}cast(null as {{ tipo }}){% endif %} as {{ campo }},
        {% endfor %}
        0 conflitos
    from presencas p
),
registros as (
    select * from html union all select * from questionarios union all select * from atividades
)
select distinct to_hex(sha256(to_json_string(struct(r.id_usuario, r.origem, r.chave_origem)))) id_registro,
    r.* except(chave_origem), u.nome_unidade
from registros r left join {{ pu_ref('dim_unidades') }} u on r.id_unidade = u.id_unidade
{% endmacro %}

{% macro pu_cpop_historico_publico(alias) %}
{{ alias }}.origem, {{ alias }}.tipo_registro, {{ alias }}.abrangencia,
    coalesce({{ alias }}.id_evolucao_modulo, '') as id_evolucao_modulo,
    coalesce({{ alias }}.titulo_formulario, '') as titulo_formulario,
    coalesce({{ alias }}.nome_unidade, '') as nome_unidade,
    coalesce(cast({{ alias }}.data_registro as string), '') as data_registro,
    coalesce(cast({{ alias }}.data_resposta as string), '') as data_resposta,
    {% for campo, tipo in pu_cpop_campos().items() if campo not in ['id_atividade', 'nome_atividade', 'data_presenca', 'hora_presenca'] %}
{% if campo == 'demandas' %}coalesce({{ alias }}.demandas, [])
        {% elif campo == 'encaminhamentos' %}array(select as struct coalesce(e.label, '') label, coalesce(e.valor, '') valor from unnest({{ alias }}.encaminhamentos) e order by label, valor)
        {% else %}coalesce(cast({{ alias }}.{{ campo }} as string), ''){% endif %} as {{ campo }}{% if not loop.last %},{% endif %}
{% endfor %}
{% endmacro %}


{% macro pu_cpop_perfil_assuntos() %}
    {{ return({
        'pai': ['data_inclusao_pai', 'data_inclusao_informada', 'demandas', 'encaminhamentos', 'observacoes'],
        'atendimento_social': ['motivo_secundario_permanencia_rua', 'possui_referencias_familiares', 'territorio_referencia_familia', 'possibilidade_reinsercao_familiar', 'demandas', 'encaminhamentos', 'observacoes'],
        'saude': ['uso_substancias_psicoativas', 'situacao_saude', 'local_tratamento', 'possui_problema_saude', 'realiza_acompanhamento_saude', 'observacoes'],
        'situacao_rua': ['motivo_principal_permanencia_rua'],
        'desligamento': ['data_desligamento', 'data_desligamento_informada', 'motivo_desligamento', 'motivo_desligamento_outros', 'observacoes']
    }) }}
{% endmacro %}

{% macro pu_cpop_perfil_nome(assunto, campo) -%}
{{ campo ~ '_' ~ assunto if campo in ['demandas', 'encaminhamentos', 'observacoes'] else campo }}
{%- endmacro %}



{% macro pu_cpop_perfil_publico(alias) %}
    coalesce({{ alias }}.unidades_atendimento, []) as unidades_atendimento,
    coalesce({{ alias }}.possui_registro_pai, '') as possui_registro_pai
    {% for assunto, campos in pu_cpop_perfil_assuntos().items() %}
        , coalesce(cast({{ alias }}.data_registro_{{ assunto }} as string), '') as data_registro_{{ assunto }}
        , coalesce(cast({{ alias }}.data_resposta_{{ assunto }} as string), '') as data_resposta_{{ assunto }}
{% for campo in campos %}
{% set nome = pu_cpop_perfil_nome(assunto, campo) %}
            , {% if campo in ['demandas', 'encaminhamentos'] %}coalesce({{ alias }}.{{ nome }}, [])
              {% else %}coalesce(cast({{ alias }}.{{ nome }} as string), ''){% endif %} as {{ nome }}
{% endfor %}
{% endfor %}
{% endmacro %}
