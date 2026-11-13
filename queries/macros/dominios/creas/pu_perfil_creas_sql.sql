{% macro pu_perfil_creas_sql(fonte) %}
with registros as (
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
    from {{ fonte }} as p, unnest(p.historico) as h
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
    from ultimos as r, unnest(r.campos) as c
    inner join {{ ref('creas_campos') }} as mapa
        on
            r.titulo_formulario = mapa.titulo
            and lower(trim(c.label)) = lower(trim(mapa.rotulo))
    where nullif(trim(c.valor), '') is not null
),

respostas as (
    select
        id_usuario,
        string_agg(if(coluna = 'mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia', valor, null), ', ' order by valor) as mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
        string_agg(if(coluna = 'mse_com_quem_moraria', valor, null), ', ' order by valor) as mse_com_quem_moraria,
        string_agg(if(coluna = 'mse_1o_bimestre_documentacao', valor, null), ', ' order by valor) as mse_1o_bimestre_documentacao,
        string_agg(if(coluna = 'mse_1o_bimestre_beneficios', valor, null), ', ' order by valor) as mse_1o_bimestre_beneficios,
        string_agg(if(coluna = 'mse_1o_bimestre_cadunico', valor, null), ', ' order by valor) as mse_1o_bimestre_cadunico,
        string_agg(if(coluna = 'mse_1o_bimestre_localizacao', valor, null), ', ' order by valor) as mse_1o_bimestre_localizacao,
        string_agg(if(coluna = 'mse_1o_bimestre_outras_acoes', valor, null), ', ' order by valor) as mse_1o_bimestre_outras_acoes,
        string_agg(if(coluna = 'mse_1o_bimestre_saude', valor, null), ', ' order by valor) as mse_1o_bimestre_saude,
        string_agg(if(coluna = 'mse_adolescente_em_reinfracao', valor, null), ', ' order by valor) as mse_adolescente_em_reinfracao,
        string_agg(if(coluna = 'mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade', valor, null), ', ' order by valor) as mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade,
        string_agg(if(coluna = 'mse_avaliacao_tecnica_bimestral_1o_bimestre', valor, null), ', ' order by valor) as mse_avaliacao_tecnica_bimestral_1o_bimestre,
        string_agg(if(coluna = 'mse_cras_de_referencia', valor, null), ', ' order by valor) as mse_cras_de_referencia,
        string_agg(if(coluna = 'mse_creas_de_referencia', valor, null), ', ' order by valor) as mse_creas_de_referencia,
        string_agg(if(coluna = 'mse_carga_horaria', valor, null), ', ' order by valor) as mse_carga_horaria,
        string_agg(if(coluna = 'mse_com_quem_residia_anteriormente', valor, null), ', ' order by valor) as mse_com_quem_residia_anteriormente,
        string_agg(if(coluna = 'mse_data', valor, null), ', ' order by valor) as mse_data,
        string_agg(if(coluna = 'mse_data_de_inclusao_no_mse', valor, null), ', ' order by valor) as mse_data_de_inclusao_no_mse,
        string_agg(if(coluna = 'mse_desdobramento', valor, null), ', ' order by valor) as mse_desdobramento,
        string_agg(if(coluna = 'mse_em_caso_de_cumprimento_de_mse_qual_o_status', valor, null), ', ' order by valor) as mse_em_caso_de_cumprimento_de_mse_qual_o_status,
        string_agg(if(coluna = 'mse_em_caso_de_cumprimento_de_psc_qual_o_local', valor, null), ', ' order by valor) as mse_em_caso_de_cumprimento_de_psc_qual_o_local,
        string_agg(if(coluna = 'mse_endereco_cras', valor, null), ', ' order by valor) as mse_endereco_cras,
        string_agg(if(coluna = 'mse_endereco_creas', valor, null), ', ' order by valor) as mse_endereco_creas,
        string_agg(if(coluna = 'mse_especialidade', valor, null), ', ' order by valor) as mse_especialidade,
        string_agg(if(coluna = 'mse_gostaria_de_retornar_para_casa_ou_convivio_anterior', valor, null), ', ' order by valor) as mse_gostaria_de_retornar_para_casa_ou_convivio_anterior,
        string_agg(if(coluna = 'mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria', valor, null), ', ' order by valor) as mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria,
        string_agg(if(coluna = 'mse_local', valor, null), ', ' order by valor) as mse_local,
        string_agg(if(coluna = 'mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia', valor, null), ', ' order by valor) as mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia,
        string_agg(if(coluna = 'mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia', valor, null), ', ' order by valor) as mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
        string_agg(if(coluna = 'mse_medida_aplicada', valor, null), ', ' order by valor) as mse_medida_aplicada,
        string_agg(if(coluna = 'mse_nome_do_local_de_cumprimento_de_psc', valor, null), ', ' order by valor) as mse_nome_do_local_de_cumprimento_de_psc,
        string_agg(if(coluna = 'mse_outras_atividades', valor, null), ', ' order by valor) as mse_outras_atividades,
        string_agg(if(coluna = 'mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia', valor, null), ', ' order by valor) as mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia,
        string_agg(if(coluna = 'mse_procedimento', valor, null), ', ' order by valor) as mse_procedimento,
        string_agg(if(coluna = 'mse_profissional_cras', valor, null), ', ' order by valor) as mse_profissional_cras,
        string_agg(if(coluna = 'mse_profissional_creas', valor, null), ', ' order by valor) as mse_profissional_creas,
        string_agg(if(coluna = 'mse_quais_atividades_gostaria_de_participar', valor, null), ', ' order by valor) as mse_quais_atividades_gostaria_de_participar,
        string_agg(if(coluna = 'mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido', valor, null), ', ' order by valor) as mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido,
        string_agg(if(coluna = 'mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc', valor, null), ', ' order by valor) as mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc,
        string_agg(if(coluna = 'mse_telefone_cras', valor, null), ', ' order by valor) as mse_telefone_cras,
        string_agg(if(coluna = 'mse_telefone_creas', valor, null), ', ' order by valor) as mse_telefone_creas,
        string_agg(if(coluna = 'mse_tempo_de_cumprimento', valor, null), ', ' order by valor) as mse_tempo_de_cumprimento,
        string_agg(if(coluna = 'mse_tipo', valor, null), ', ' order by valor) as mse_tipo,
        string_agg(if(coluna = 'mse_tipo_de_ato_infracional', valor, null), ', ' order by valor) as mse_tipo_de_ato_infracional,
        string_agg(if(coluna = 'mse_usuario_interditado', valor, null), ', ' order by valor) as mse_usuario_interditado,
        string_agg(if(coluna = 'mse_retorno_adolescente_em_reinfracao', valor, null), ', ' order by valor) as mse_retorno_adolescente_em_reinfracao,
        string_agg(if(coluna = 'mse_retorno_data_do_desligamento', valor, null), ', ' order by valor) as mse_retorno_data_do_desligamento,
        string_agg(if(coluna = 'mse_retorno_desligamento', valor, null), ', ' order by valor) as mse_retorno_desligamento,
        string_agg(if(coluna = 'mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status', valor, null), ', ' order by valor) as mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status,
        string_agg(if(coluna = 'mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local', valor, null), ', ' order by valor) as mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local,
        string_agg(if(coluna = 'mse_retorno_motivo_do_desligamento', valor, null), ', ' order by valor) as mse_retorno_motivo_do_desligamento,
        string_agg(if(coluna = 'mse_retorno_nome_do_local_de_cumprimento_de_psc', valor, null), ', ' order by valor) as mse_retorno_nome_do_local_de_cumprimento_de_psc,
        string_agg(if(coluna = 'peti_data_da_inclusao_no_peti', valor, null), ', ' order by valor) as peti_data_da_inclusao_no_peti,
        string_agg(if(coluna = 'peti_demanda_inicial', valor, null), ', ' order by valor) as peti_demanda_inicial,
        string_agg(if(coluna = 'peti_forma_de_acesso_informe_a_origem_de_acesso', valor, null), ', ' order by valor) as peti_forma_de_acesso_informe_a_origem_de_acesso,
        string_agg(if(coluna = 'peti_marcacao_nos_campo_cadunico', valor, null), ', ' order by valor) as peti_marcacao_nos_campo_cadunico,
        string_agg(if(coluna = 'peti_orientacoes_e_encaminhamentos', valor, null), ', ' order by valor) as peti_orientacoes_e_encaminhamentos,
        string_agg(if(coluna = 'peti_outras_demandas_vulnerabilidades', valor, null), ', ' order by valor) as peti_outras_demandas_vulnerabilidades,
        string_agg(if(coluna = 'peti_outras_formas_de_acesso', valor, null), ', ' order by valor) as peti_outras_formas_de_acesso,
        string_agg(if(coluna = 'peti_outros', valor, null), ', ' order by valor) as peti_outros,
        string_agg(if(coluna = 'peti_tipo_de_trabalho_infantil', valor, null), ', ' order by valor) as peti_tipo_de_trabalho_infantil,
        string_agg(if(coluna = 'peti_e_acompanhado_em_outro_creas', valor, null), ', ' order by valor) as peti_e_acompanhado_em_outro_creas,
        string_agg(if(coluna = 'peti_e_necessario_realizar_a_gestao_de_beneficios', valor, null), ', ' order by valor) as peti_e_necessario_realizar_a_gestao_de_beneficios,
        string_agg(if(coluna = 'scfv_data_da_solicitacao_do_acolhimento', valor, null), ', ' order by valor) as scfv_data_da_solicitacao_do_acolhimento,
        string_agg(if(coluna = 'scfv_demanda_de_acolhimento_institucional_por_decisao_judicial', valor, null), ', ' order by valor) as scfv_demanda_de_acolhimento_institucional_por_decisao_judicial,
        string_agg(if(coluna = 'scfv_nome_do_cras_responsavel', valor, null), ', ' order by valor) as scfv_nome_do_cras_responsavel,
        string_agg(if(coluna = 'scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos', valor, null), ', ' order by valor) as scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos,
        string_agg(if(coluna = 'scfv_oriundo_de_instituicao_de_acolhimento_interditado', valor, null), ', ' order by valor) as scfv_oriundo_de_instituicao_de_acolhimento_interditado,
        string_agg(if(coluna = 'scfv_usuario_interditado', valor, null), ', ' order by valor) as scfv_usuario_interditado,
        string_agg(if(coluna = 'scfv_orgao_de_origem_do_acolhimento', valor, null), ', ' order by valor) as scfv_orgao_de_origem_do_acolhimento,
        string_agg(if(coluna = 'peti_desligamento_data_do_desligamento_do_peti', valor, null), ', ' order by valor) as peti_desligamento_data_do_desligamento_do_peti,
        string_agg(if(coluna = 'peti_desligamento_motivo_do_desligamento', valor, null), ', ' order by valor) as peti_desligamento_motivo_do_desligamento,
        string_agg(if(coluna = 'peti_desligamento_outros', valor, null), ', ' order by valor) as peti_desligamento_outros
    from campos_unicos
    group by id_usuario
)

select
    p.id_usuario,
    r_mse.data_ocorrencia as mse_data_ocorrencia,
    r_mse.data_evolucao as mse_data_registro,
    r_mse.id_unidade as mse_id_unidade,
    u_mse.nome_unidade as mse_nome_unidade,
    r_mse.observacoes as mse_observacoes,
    respostas.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
    respostas.mse_com_quem_moraria,
    respostas.mse_1o_bimestre_documentacao,
    respostas.mse_1o_bimestre_beneficios,
    respostas.mse_1o_bimestre_cadunico,
    respostas.mse_1o_bimestre_localizacao,
    respostas.mse_1o_bimestre_outras_acoes,
    respostas.mse_1o_bimestre_saude,
    respostas.mse_adolescente_em_reinfracao,
    respostas.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade,
    respostas.mse_avaliacao_tecnica_bimestral_1o_bimestre,
    respostas.mse_cras_de_referencia,
    respostas.mse_creas_de_referencia,
    respostas.mse_carga_horaria,
    respostas.mse_com_quem_residia_anteriormente,
    respostas.mse_data,
    respostas.mse_data_de_inclusao_no_mse,
    respostas.mse_desdobramento,
    respostas.mse_em_caso_de_cumprimento_de_mse_qual_o_status,
    respostas.mse_em_caso_de_cumprimento_de_psc_qual_o_local,
    respostas.mse_endereco_cras,
    respostas.mse_endereco_creas,
    respostas.mse_especialidade,
    respostas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior,
    respostas.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria,
    respostas.mse_local,
    respostas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia,
    respostas.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
    respostas.mse_medida_aplicada,
    respostas.mse_nome_do_local_de_cumprimento_de_psc,
    respostas.mse_outras_atividades,
    respostas.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia,
    respostas.mse_procedimento,
    respostas.mse_profissional_cras,
    respostas.mse_profissional_creas,
    respostas.mse_quais_atividades_gostaria_de_participar,
    respostas.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido,
    respostas.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc,
    respostas.mse_telefone_cras,
    respostas.mse_telefone_creas,
    respostas.mse_tempo_de_cumprimento,
    respostas.mse_tipo,
    respostas.mse_tipo_de_ato_infracional,
    respostas.mse_usuario_interditado,
    r_mse_retorno.data_ocorrencia as mse_retorno_data_ocorrencia,
    r_mse_retorno.data_evolucao as mse_retorno_data_registro,
    r_mse_retorno.id_unidade as mse_retorno_id_unidade,
    u_mse_retorno.nome_unidade as mse_retorno_nome_unidade,
    r_mse_retorno.observacoes as mse_retorno_observacoes,
    respostas.mse_retorno_adolescente_em_reinfracao,
    respostas.mse_retorno_data_do_desligamento,
    respostas.mse_retorno_desligamento,
    respostas.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status,
    respostas.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local,
    respostas.mse_retorno_motivo_do_desligamento,
    respostas.mse_retorno_nome_do_local_de_cumprimento_de_psc,
    r_peti.data_ocorrencia as peti_data_ocorrencia,
    r_peti.data_evolucao as peti_data_registro,
    r_peti.id_unidade as peti_id_unidade,
    u_peti.nome_unidade as peti_nome_unidade,
    r_peti.observacoes as peti_observacoes,
    respostas.peti_data_da_inclusao_no_peti,
    respostas.peti_demanda_inicial,
    respostas.peti_forma_de_acesso_informe_a_origem_de_acesso,
    respostas.peti_marcacao_nos_campo_cadunico,
    respostas.peti_orientacoes_e_encaminhamentos,
    respostas.peti_outras_demandas_vulnerabilidades,
    respostas.peti_outras_formas_de_acesso,
    respostas.peti_outros,
    respostas.peti_tipo_de_trabalho_infantil,
    respostas.peti_e_acompanhado_em_outro_creas,
    respostas.peti_e_necessario_realizar_a_gestao_de_beneficios,
    r_scfv.data_ocorrencia as scfv_data_ocorrencia,
    r_scfv.data_evolucao as scfv_data_registro,
    r_scfv.id_unidade as scfv_id_unidade,
    u_scfv.nome_unidade as scfv_nome_unidade,
    r_scfv.observacoes as scfv_observacoes,
    respostas.scfv_data_da_solicitacao_do_acolhimento,
    respostas.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial,
    respostas.scfv_nome_do_cras_responsavel,
    respostas.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos,
    respostas.scfv_oriundo_de_instituicao_de_acolhimento_interditado,
    respostas.scfv_usuario_interditado,
    respostas.scfv_orgao_de_origem_do_acolhimento,
    r_peti_desligamento.data_ocorrencia as peti_desligamento_data_ocorrencia,
    r_peti_desligamento.data_evolucao as peti_desligamento_data_registro,
    r_peti_desligamento.id_unidade as peti_desligamento_id_unidade,
    u_peti_desligamento.nome_unidade as peti_desligamento_nome_unidade,
    r_peti_desligamento.observacoes as peti_desligamento_observacoes,
    respostas.peti_desligamento_data_do_desligamento_do_peti,
    respostas.peti_desligamento_motivo_do_desligamento,
    respostas.peti_desligamento_outros,
    u_passo.nome_unidade as passo_a_passo_ultima_unidade,
    p.participacoes[safe_offset(array_length(p.participacoes) - 1)].nome_atividade as passo_a_passo_ultima_atividade,
    p.participacoes[safe_offset(array_length(p.participacoes) - 1)].data_presenca as passo_a_passo_ultima_data,
    p.participacoes[safe_offset(array_length(p.participacoes) - 1)].hora_presenca as passo_a_passo_ultima_hora
from {{ fonte }} as p
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

{% endmacro %}
