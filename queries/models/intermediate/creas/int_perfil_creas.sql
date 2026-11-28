{{ config(tags=['planilha_unificada', 'pu_dimensoes']) }}
{% set campos_por_assunto = {
    'mse': [
        'mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia',
        'mse_com_quem_moraria',
        'mse_1o_bimestre_documentacao',
        'mse_1o_bimestre_beneficios',
        'mse_1o_bimestre_cadunico',
        'mse_1o_bimestre_localizacao',
        'mse_1o_bimestre_outras_acoes',
        'mse_1o_bimestre_saude',
        'mse_adolescente_em_reinfracao',
        'mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade',
        'mse_avaliacao_tecnica_bimestral_1o_bimestre',
        'mse_cras_de_referencia',
        'mse_creas_de_referencia',
        'mse_carga_horaria',
        'mse_com_quem_residia_anteriormente',
        'mse_data',
        'mse_data_de_inclusao_no_mse',
        'mse_desdobramento',
        'mse_em_caso_de_cumprimento_de_mse_qual_o_status',
        'mse_em_caso_de_cumprimento_de_psc_qual_o_local',
        'mse_endereco_cras',
        'mse_endereco_creas',
        'mse_especialidade',
        'mse_gostaria_de_retornar_para_casa_ou_convivio_anterior',
        'mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria',
        'mse_local',
        'mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia',
        'mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia',
        'mse_medida_aplicada',
        'mse_nome_do_local_de_cumprimento_de_psc',
        'mse_outras_atividades',
        'mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia',
        'mse_procedimento',
        'mse_profissional_cras',
        'mse_profissional_creas',
        'mse_quais_atividades_gostaria_de_participar',
        'mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido',
        'mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc',
        'mse_telefone_cras',
        'mse_telefone_creas',
        'mse_tempo_de_cumprimento',
        'mse_tipo',
        'mse_tipo_de_ato_infracional',
        'mse_usuario_interditado',
    ],
    'mse_retorno': [
        'mse_retorno_adolescente_em_reinfracao',
        'mse_retorno_data_do_desligamento',
        'mse_retorno_desligamento',
        'mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status',
        'mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local',
        'mse_retorno_motivo_do_desligamento',
        'mse_retorno_nome_do_local_de_cumprimento_de_psc',
    ],
    'peti': [
        'peti_data_da_inclusao_no_peti',
        'peti_demanda_inicial',
        'peti_forma_de_acesso_informe_a_origem_de_acesso',
        'peti_marcacao_nos_campo_cadunico',
        'peti_orientacoes_e_encaminhamentos',
        'peti_outras_demandas_vulnerabilidades',
        'peti_outras_formas_de_acesso',
        'peti_outros',
        'peti_tipo_de_trabalho_infantil',
        'peti_e_acompanhado_em_outro_creas',
        'peti_e_necessario_realizar_a_gestao_de_beneficios',
    ],
    'scfv': [
        'scfv_data_da_solicitacao_do_acolhimento',
        'scfv_demanda_de_acolhimento_institucional_por_decisao_judicial',
        'scfv_nome_do_cras_responsavel',
        'scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos',
        'scfv_oriundo_de_instituicao_de_acolhimento_interditado',
        'scfv_usuario_interditado',
        'scfv_orgao_de_origem_do_acolhimento',
    ],
    'peti_desligamento': [
        'peti_desligamento_data_do_desligamento_do_peti',
        'peti_desligamento_motivo_do_desligamento',
        'peti_desligamento_outros',
    ],
} %}
{# Concatena os campos na ordem dos assuntos, preservando a ordem das agregações. #}
{% set campos_respostas = campos_por_assunto.values() | sum(start=[]) %}
with formularios as (
    select
        p.id_usuario,
        array_agg(f order by f.data_evolucao, f.id_evolucao_modulo) as historico
    from ({{ agrupar_formularios(ref('int_formularios')) }}) as p, unnest(p.formularios) as f
    where regexp_contains(f.titulo_formulario, r'(?i)CREAS.*(PETI|Adolescente em trabalho|MSE)')
    group by p.id_usuario
),

participacoes as (
    select
        p.id_usuario,
        array_agg(
            struct(
                p.id_unidade, p.id_atividade, a.nome_atividade,
                p.data_presenca, p.hora_presenca
            ) order by p.data_presenca, p.hora_presenca, p.id_atividade
        ) as participacoes
    from {{ ref('fct_presencas_usuarios') }} as p
    inner join {{ ref('raw_atividades_grupo') }} as a on p.id_atividade = a.id_atividade
    where regexp_contains(a.nome_atividade, r'(?i)passo[ -]+a[ -]+passo')
    group by p.id_usuario
),

perfil_geral as (
    select
        coalesce(f.id_usuario, p.id_usuario) as id_usuario,
        coalesce(f.historico, []) as historico,
        coalesce(p.participacoes, []) as participacoes
    from formularios as f full outer join participacoes as p on f.id_usuario = p.id_usuario
),

base_historico as (
    select
        concat(cast(p.id_usuario as string), ':', coalesce(cast(h.id_unidade as string), '')) as id_usuario_unidade,
        array_agg(h) as historico
    from perfil_geral as p, unnest(p.historico) as h
    group by id_usuario_unidade
),

base_participacoes as (
    select
        concat(cast(p.id_usuario as string), ':', coalesce(cast(h.id_unidade as string), '')) as id_usuario_unidade,
        array_agg(h order by h.data_presenca, h.hora_presenca, h.id_atividade) as participacoes
    from perfil_geral as p, unnest(p.participacoes) as h
    group by id_usuario_unidade
),

creas_escopo as (
    select
        coalesce(h.id_usuario_unidade, p.id_usuario_unidade) as id_usuario,
        coalesce(h.historico, []) as historico,
        coalesce(p.participacoes, []) as participacoes
    from base_historico as h full join base_participacoes as p on h.id_usuario_unidade = p.id_usuario_unidade
),

registros as (
    select
        h.*,
        p.id_usuario,
        case
            when h.titulo_formulario = 'CREAS - MSE - Medidas Socioeducativas' then 'mse'
            when h.titulo_formulario = 'CREAS - MSE - RETORNO' then 'mse_retorno'
            when h.titulo_formulario = 'CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' then 'peti'
            when h.titulo_formulario = 'CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' then 'scfv'
            when h.titulo_formulario = 'CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil' then 'peti_desligamento'
        end as assunto
    from creas_escopo as p, unnest(p.historico) as h
),

ultimos as (
    select * from registros
    where assunto is not null
    qualify row_number() over (partition by id_usuario, assunto order by data_ocorrencia desc nulls last, data_evolucao desc nulls last, id_evolucao desc nulls last, id_evolucao_modulo desc) = 1
),

campos_unicos as (
    select distinct
        r.id_usuario,
        mapa.coluna,
        trim(c.valor) as valor
    from ultimos as r
    cross join unnest(r.campos) as c
    inner join
        unnest([
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '- Com que frequência vai à casa da família ou pessoa de referência?' as rotulo, 'mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '- Com quem moraria?' as rotulo, 'mse_com_quem_moraria' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '1º Bimestre  [Documentação]' as rotulo, 'mse_1o_bimestre_documentacao' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '1º Bimestre [Benefícios]' as rotulo, 'mse_1o_bimestre_beneficios' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '1º Bimestre [CadÚnico]' as rotulo, 'mse_1o_bimestre_cadunico' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '1º Bimestre [Localização]' as rotulo, 'mse_1o_bimestre_localizacao' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '1º Bimestre [Outras Ações]' as rotulo, 'mse_1o_bimestre_outras_acoes' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, '1º Bimestre [Saúde]' as rotulo, 'mse_1o_bimestre_saude' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Adolescente em reinfração?' as rotulo, 'mse_adolescente_em_reinfracao' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Adolescente tem histórico de cumprimento de MSE em meio fechado ou semiliberdade?' as rotulo, 'mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Avaliação Técnica Bimestral  | 1º Bimestre' as rotulo, 'mse_avaliacao_tecnica_bimestral_1o_bimestre' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'CRAS de Referência' as rotulo, 'mse_cras_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'CRAS de referência' as rotulo, 'mse_cras_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'CREAS de Referência' as rotulo, 'mse_creas_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'CREAS de referência' as rotulo, 'mse_creas_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Carga horária' as rotulo, 'mse_carga_horaria' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Com quem residia anteriormente?' as rotulo, 'mse_com_quem_residia_anteriormente' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Data' as rotulo, 'mse_data' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Data de inclusão no MSE' as rotulo, 'mse_data_de_inclusao_no_mse' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Desdobramento' as rotulo, 'mse_desdobramento' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Em caso de cumprimento de MSE, qual o status?' as rotulo, 'mse_em_caso_de_cumprimento_de_mse_qual_o_status' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Em caso de cumprimento de PSC, qual o local?' as rotulo, 'mse_em_caso_de_cumprimento_de_psc_qual_o_local' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Endereço (CRAS)' as rotulo, 'mse_endereco_cras' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Endereço (CREAS)' as rotulo, 'mse_endereco_creas' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Especialidade' as rotulo, 'mse_especialidade' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Gostaria de retornar para casa ou convívio anterior?' as rotulo, 'mse_gostaria_de_retornar_para_casa_ou_convivio_anterior' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Gostaria de retornar para casa ou convívio anterior?  - Com quem moraria?' as rotulo, 'mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Local' as rotulo, 'mse_local' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Mantém contato com a Família ou pessoa de referência?' as rotulo, 'mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Mantém contato com a Família ou pessoa de referência?  - Com que frequência vai à casa da família ou pessoa de referência?' as rotulo, 'mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Medida aplicada' as rotulo, 'mse_medida_aplicada' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Nome do local de cumprimento de PSC' as rotulo, 'mse_nome_do_local_de_cumprimento_de_psc' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Outras atividades' as rotulo, 'mse_outras_atividades' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Por que se afastou da família ou pessoa de referência?' as rotulo, 'mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Procedimento' as rotulo, 'mse_procedimento' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Profissional (CRAS)' as rotulo, 'mse_profissional_cras' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Profissional (CREAS)' as rotulo, 'mse_profissional_creas' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Quais atividades gostaria de participar?' as rotulo, 'mse_quais_atividades_gostaria_de_participar' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Registrar o que conseguiu cumprir e justificar o que não conseguiu cumprir no prazo estabelecido' as rotulo, 'mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Serviço de proteção social ao adolescente em cumprimento de LA ou PSC' as rotulo, 'mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Telefone (CRAS)' as rotulo, 'mse_telefone_cras' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Telefone (CREAS)' as rotulo, 'mse_telefone_creas' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Tempo de cumprimento' as rotulo, 'mse_tempo_de_cumprimento' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Tipo' as rotulo, 'mse_tipo' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Tipo de ato infracional' as rotulo, 'mse_tipo_de_ato_infracional' as coluna),
            struct('CREAS - MSE - Medidas Socioeducativas' as titulo, 'Usuário Interditado?' as rotulo, 'mse_usuario_interditado' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Adolescente em reinfração?' as rotulo, 'mse_retorno_adolescente_em_reinfracao' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Data do desligamento' as rotulo, 'mse_retorno_data_do_desligamento' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Desligamento' as rotulo, 'mse_retorno_desligamento' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Em caso de cumprimento de MSE, qual o status?' as rotulo, 'mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Em caso de cumprimento de PSC, qual o local?' as rotulo, 'mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Motivo do desligamento' as rotulo, 'mse_retorno_motivo_do_desligamento' as coluna),
            struct('CREAS - MSE - RETORNO' as titulo, 'Nome do local de cumprimento de PSC' as rotulo, 'mse_retorno_nome_do_local_de_cumprimento_de_psc' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Data da inclusão no PETI' as rotulo, 'peti_data_da_inclusao_no_peti' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Demanda Inicial' as rotulo, 'peti_demanda_inicial' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Forma de acesso (informe a origem de acesso)' as rotulo, 'peti_forma_de_acesso_informe_a_origem_de_acesso' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Marcação nos campo CadÚnico' as rotulo, 'peti_marcacao_nos_campo_cadunico' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Orientações e Encaminhamentos' as rotulo, 'peti_orientacoes_e_encaminhamentos' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Outras demandas / vulnerabilidades' as rotulo, 'peti_outras_demandas_vulnerabilidades' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Outras formas de acesso' as rotulo, 'peti_outras_formas_de_acesso' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Outros' as rotulo, 'peti_outros' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'Tipo de trabalho infantil' as rotulo, 'peti_tipo_de_trabalho_infantil' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'É acompanhado em outro CREAS?' as rotulo, 'peti_e_acompanhado_em_outro_creas' as coluna),
            struct('CREAS - Adolescente em trabalho não regulamentado e PETI - Programa de Erradicação do Trabalho Infantil' as titulo, 'É necessário realizar a Gestão de Benefícios?' as rotulo, 'peti_e_necessario_realizar_a_gestao_de_beneficios' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'Data da solicitação do acolhimento' as rotulo, 'scfv_data_da_solicitacao_do_acolhimento' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'Demanda de acolhimento institucional por decisão judicial?' as rotulo, 'scfv_demanda_de_acolhimento_institucional_por_decisao_judicial' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'Nome do CRAS responsável' as rotulo, 'scfv_nome_do_cras_responsavel' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'O usuário participa do SCFV (Serviço de Convivência e Fortalecimento de Vínculos)?' as rotulo, 'scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'Oriundo de instituição de acolhimento interditado?' as rotulo, 'scfv_oriundo_de_instituicao_de_acolhimento_interditado' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'Usuário Interditado?' as rotulo, 'scfv_usuario_interditado' as coluna),
            struct('CREAS - Participação em SCFV - Público prioritário / MSE e Trab. Infantil' as titulo, 'Órgão de origem do acolhimento' as rotulo, 'scfv_orgao_de_origem_do_acolhimento' as coluna),
            struct('CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil' as titulo, 'Data do desligamento do PETI' as rotulo, 'peti_desligamento_data_do_desligamento_do_peti' as coluna),
            struct('CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil' as titulo, 'Motivo do desligamento' as rotulo, 'peti_desligamento_motivo_do_desligamento' as coluna),
            struct('CREAS - PETI - DESLIGAMENTO - Programa de Erradicação do Trabalho Infantil' as titulo, 'Outros' as rotulo, 'peti_desligamento_outros' as coluna)
        ]) as mapa
        on
            r.titulo_formulario = mapa.titulo
            and lower(trim(c.label)) = lower(trim(mapa.rotulo))
    where nullif(trim(c.valor), '') is not null
),

respostas as (
    select
        id_usuario,
        {% for campo in campos_respostas %}
            string_agg(if(coluna = '{{ campo }}', valor, null), ', ' order by valor) as {{ campo }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from campos_unicos
    group by id_usuario
),

perfil as (
    select
        p.id_usuario,
        {% for assunto, campos in campos_por_assunto.items() %}
            r_{{ assunto }}.data_ocorrencia as {{ assunto }}_data_ocorrencia,
            r_{{ assunto }}.data_evolucao as {{ assunto }}_data_registro,
            r_{{ assunto }}.id_unidade as {{ assunto }}_id_unidade,
            u_{{ assunto }}.nome_unidade as {{ assunto }}_nome_unidade,
            r_{{ assunto }}.observacoes as {{ assunto }}_observacoes,
            {% for campo in campos %}
                respostas.{{ campo }},
            {% endfor %}
        {% endfor %}
        u_passo.nome_unidade as passo_a_passo_ultima_unidade,
        p.participacoes[safe_offset(array_length(p.participacoes) - 1)].nome_atividade as passo_a_passo_ultima_atividade,
        p.participacoes[safe_offset(array_length(p.participacoes) - 1)].data_presenca as passo_a_passo_ultima_data,
        p.participacoes[safe_offset(array_length(p.participacoes) - 1)].hora_presenca as passo_a_passo_ultima_hora
    from creas_escopo as p
    left join respostas on p.id_usuario = respostas.id_usuario
    left join ultimos as r_mse on p.id_usuario = r_mse.id_usuario and r_mse.assunto = 'mse'
    left join {{ ref('dim_unidades') }} as u_mse on r_mse.id_unidade = u_mse.id_unidade
    left join ultimos as r_mse_retorno on p.id_usuario = r_mse_retorno.id_usuario and r_mse_retorno.assunto = 'mse_retorno'
    left join {{ ref('dim_unidades') }} as u_mse_retorno on r_mse_retorno.id_unidade = u_mse_retorno.id_unidade
    left join ultimos as r_peti on p.id_usuario = r_peti.id_usuario and r_peti.assunto = 'peti'
    left join {{ ref('dim_unidades') }} as u_peti on r_peti.id_unidade = u_peti.id_unidade
    left join ultimos as r_scfv on p.id_usuario = r_scfv.id_usuario and r_scfv.assunto = 'scfv'
    left join {{ ref('dim_unidades') }} as u_scfv on r_scfv.id_unidade = u_scfv.id_unidade
    left join ultimos as r_peti_desligamento on p.id_usuario = r_peti_desligamento.id_usuario and r_peti_desligamento.assunto = 'peti_desligamento'
    left join {{ ref('dim_unidades') }} as u_peti_desligamento on r_peti_desligamento.id_unidade = u_peti_desligamento.id_unidade

    left join {{ ref('dim_unidades') }} as u_passo on p.participacoes[safe_offset(array_length(p.participacoes) - 1)].id_unidade = u_passo.id_unidade
),

perfil_por_unidade as (
    select
        split(id_usuario, ':')[offset(0)] as id_usuario,
        safe_cast(split(id_usuario, ':')[offset(1)] as int64) as id_unidade,
        (select as struct perfil.* except (id_usuario)) as creas
    from perfil
),

registros_por_usuario as (
    select
        u.id_usuario,
        array_agg((select as struct u.* except (id_usuario)) order by u.id_unidade) as registros
    from perfil_por_unidade as u
    group by u.id_usuario
)

select
    coalesce(cast(g.id_usuario as string), u.id_usuario) as id_usuario,
    if(g.id_usuario is null, null, g) as perfil_geral,
    coalesce(u.registros, []) as registros
from perfil_geral as g
full join registros_por_usuario as u on cast(g.id_usuario as string) = u.id_usuario
