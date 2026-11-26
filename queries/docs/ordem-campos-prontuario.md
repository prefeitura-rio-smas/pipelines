# Ordem dos campos do Prontuário Eletrônico

Cada linha continua a representar um cidadão por unidade. As tabelas mostram a ordem das colunas de cada bloco. Nos arrays, essa é a ordem dos campos dentro de cada item. A sequência dos itens permanece a mesma. A raiz da tabela e a ordem interna de `usuario` foram preservadas.

Ordem publicada em 24 de novembro de 2026 em `rj-smas-dev.abordagem.prontuario_eletronico`. O Prefect recebeu o commit `4c60a32` e o agendamento diário segue às 10h de São Paulo. [PR #163](https://github.com/prefeitura-rio-smas/pipelines/pull/163) · [Verificação do Prefect](https://github.com/prefeitura-rio-smas/pipelines/actions/runs/45462819959)

A conferência após a troca confirmou 696.042 linhas e zero diferenças de conteúdo em relação ao snapshot, com os arrays na mesma sequência. O snapshot de recuperação é `prontuario_eletronico_backup_ordem_20261124_908c9f11`.

No Looker, a consulta segura mantém os filtros, os campos visíveis e as supressões anteriores. O resumo `centro_pop.data_ultimo_atend_tecnico_informada` da raiz do bloco fica disponível na tabela.

## Famílias

### `familias`

| Assunto | Colunas na ordem |
|---|---|
| Identificação | `id_familia` → `nome_responsavel` |
| Situação e atualização | `flag_responsavel_atual` → `flag_ativo` → `data_ultima_modificacao` |
| Coleções | `membros` → `vinculos` → `servicos` |

### `familias.membros`

| Assunto | Colunas na ordem |
|---|---|
| Identificação | `id_usuario` → `nome` |
| Parentesco | `parentesco_responsavel` |

### `familias.vinculos`

| Assunto | Colunas na ordem |
|---|---|
| Parentesco | `parentesco_responsavel` |
| Vigência e saída | `data_entrada` → `data_saida` → `motivo_saida` |

### `familias.servicos`

| Assunto | Colunas na ordem |
|---|---|
| Unidade e serviço | `nome_unidade` → `nome_servico` |
| Vigência | `data_cadastro` → `data_cancelamento` |

## Projetos sociais

### `projetos_sociais`

| Assunto | Colunas na ordem |
|---|---|
| Projeto e unidade | `id_projeto_social` → `nome_projeto_social` → `id_unidade` → `nome_unidade` |
| Situação e vigência | `indicador_ativo` → `data_cadastro` → `data_cancelamento` |

## Atendimentos

### `atendimentos`

| Assunto | Colunas na ordem |
|---|---|
| Contadores | `quantidade` → `quantidade_cras` → `quantidade_creas` → `quantidade_centro_pop` → `quantidade_outros` |
| Coleção | `registros` |

### `atendimentos.registros`

| Assunto | Colunas na ordem |
|---|---|
| Identificação e origem | `id_atendimento_modulo` → `id_familia` → `origem_modulo` |
| Unidade | `id_unidade` → `nome_unidade` → `tipo_unidade` → `cas` |
| Data e hora | `data_atendimento_informada` → `hora_atendimento` |
| Tipo e classificação | `tipo_atendimento_descricao` → `tipo_servico` → `classificacao_atendimento` → `projetos_sociais_familia` → `elegivel_indicador` → `atendimento_compartilhado` |
| Profissionais | `profissionais` |
| Cadastro e e-mails | `cadastrante` → `data_atendimento_sistema` → `email_unidade` → `email_cas` |

### `atendimentos.registros.profissionais`

| Assunto | Colunas na ordem |
|---|---|
| Identificação e referência | `id_profissional` → `nome_profissional` → `id_familia_referencia` → `id_atendimento_legado` |
| Ocupação | `descricoes_cbo` → `cbo_principal_descricao` → `profissional_cbo` |
| Classificação da participação | `classificacao_atendimento` → `nome_atendimento` |

## Evoluções

### `evolucoes`

| Assunto | Colunas na ordem |
|---|---|
| Contadores | `quantidade` → `quantidade_cras` → `quantidade_creas` → `quantidade_centro_pop` → `quantidade_outros` |
| Coleção | `registros` |

### `evolucoes.registros`

| Assunto | Colunas na ordem |
|---|---|
| Identificação e origem | `id_evolucao_modulo` → `origem_modulo` → `id_familia` |
| Unidade | `id_unidade` → `nome_unidade` |
| Datas | `data_evolucao` → `data_cancelamento` |
| Tipo, atividade e descrição | `tipo_evolucao` → `tipo_servico` → `nome_atividade` → `descricao_evolucao` |
| Profissional e formulários | `nome_profissional` → `formularios` |

### `evolucoes.registros.formularios`

| Assunto | Colunas na ordem |
|---|---|
| Identificação | `titulo_formulario` |
| Datas e proveniência | `data_ocorrencia` → `origem_data_ocorrencia` → `situacao_data_ocorrencia` |
| Observações e respostas | `observacoes` → `campos` |

## Centro POP

### `centro_pop`

| Assunto | Colunas na ordem |
|---|---|
| Unidades de atendimento | `unidades_atendimento` |
| PAI | `possui_registro_pai` → `data_ocorrencia_pai` → `origem_data_ocorrencia_pai` → `situacao_data_ocorrencia_pai` → `data_registro_pai` → `data_resposta_pai` → `data_inclusao_pai` → `data_inclusao_pai_informada` → `flag_incluido_acompanhamento_pai` → `flag_empregabilidade_imediata` → `capacidade_habilidades` → `flag_interesse_curso` → `demandas_pai` → `encaminhamentos_pai` → `observacoes_pai` |
| Atendimento social | `data_ocorrencia_atendimento_social` → `origem_data_ocorrencia_atendimento_social` → `situacao_data_ocorrencia_atendimento_social` → `data_registro_atendimento_social` → `data_resposta_atendimento_social` → `demandas_atendimento_social` → `encaminhamentos_atendimento_social` → `observacoes_atendimento_social` → `encaminhamentos` → `procedimentos` → `data_ultimo_atend_tecnico_informada` |
| Saúde | `data_ocorrencia_saude` → `origem_data_ocorrencia_saude` → `situacao_data_ocorrencia_saude` → `data_registro_saude` → `data_resposta_saude` → `uso_substancias_psicoativas` → `situacao_saude` → `local_tratamento` → `possui_problema_saude` → `realiza_acompanhamento_saude` → `observacoes_saude` |
| Situação de rua | `data_ocorrencia_situacao_rua` → `origem_data_ocorrencia_situacao_rua` → `situacao_data_ocorrencia_situacao_rua` → `data_registro_situacao_rua` → `data_resposta_situacao_rua` → `motivo_principal_permanencia_rua` → `motivo_secundario_permanencia_rua` → `possui_referencias_familiares` → `territorio_referencia_familia` → `possibilidade_reinsercao_familiar` |
| Desligamento | `data_ocorrencia_desligamento_pai` → `origem_data_ocorrencia_desligamento_pai` → `situacao_data_ocorrencia_desligamento_pai` → `data_hora_registro_desligamento_pai` → `data_hora_resposta_desligamento_pai` → `data_desligamento_pai` → `data_desligamento_pai_informada` → `motivo_desligamento_pai` → `complemento_motivo_desligamento_pai` → `observacoes_desligamento_pai` |
| Participação | `ultima_participacao_atividade` → `ultima_participacao_unidade` → `ultima_participacao_data` → `ultima_participacao_hora` |
| Coleções | `registros` → `historico_mensal` |

### `centro_pop.registros`

| Assunto | Colunas na ordem |
|---|---|
| Unidade | `id_unidade` → `nome_unidade` |
| Unidades de atendimento | `unidades_atendimento` |
| PAI | `possui_registro_pai` → `data_ocorrencia_pai` → `origem_data_ocorrencia_pai` → `situacao_data_ocorrencia_pai` → `data_registro_pai` → `data_resposta_pai` → `data_inclusao_pai` → `data_inclusao_pai_informada` → `flag_incluido_acompanhamento_pai` → `flag_empregabilidade_imediata` → `capacidade_habilidades` → `flag_interesse_curso` → `demandas_pai` → `encaminhamentos_pai` → `observacoes_pai` |
| Atendimento social | `data_ocorrencia_atendimento_social` → `origem_data_ocorrencia_atendimento_social` → `situacao_data_ocorrencia_atendimento_social` → `data_registro_atendimento_social` → `data_resposta_atendimento_social` → `demandas_atendimento_social` → `encaminhamentos_atendimento_social` → `observacoes_atendimento_social` |
| Saúde | `data_ocorrencia_saude` → `origem_data_ocorrencia_saude` → `situacao_data_ocorrencia_saude` → `data_registro_saude` → `data_resposta_saude` → `uso_substancias_psicoativas` → `situacao_saude` → `local_tratamento` → `possui_problema_saude` → `realiza_acompanhamento_saude` → `observacoes_saude` |
| Situação de rua | `data_ocorrencia_situacao_rua` → `origem_data_ocorrencia_situacao_rua` → `situacao_data_ocorrencia_situacao_rua` → `data_registro_situacao_rua` → `data_resposta_situacao_rua` → `motivo_principal_permanencia_rua` → `motivo_secundario_permanencia_rua` → `possui_referencias_familiares` → `territorio_referencia_familia` → `possibilidade_reinsercao_familiar` |
| Desligamento | `data_ocorrencia_desligamento_pai` → `origem_data_ocorrencia_desligamento_pai` → `situacao_data_ocorrencia_desligamento_pai` → `data_hora_registro_desligamento_pai` → `data_hora_resposta_desligamento_pai` → `data_desligamento_pai` → `data_desligamento_pai_informada` → `motivo_desligamento_pai` → `complemento_motivo_desligamento_pai` → `observacoes_desligamento_pai` |
| Participação | `ultima_participacao_atividade` → `ultima_participacao_unidade` → `ultima_participacao_data` → `ultima_participacao_hora` |

### `centro_pop.historico_mensal`

| Assunto | Colunas na ordem |
|---|---|
| Competência, unidade, situação e atualização | `mes_referencia` → `id_unidade` → `nome_unidade` → `situacao` → `data_atualizacao` → `versao` → `data_snapshot` → `id_fechamento` → `data_carga_fontes` |
| Contadores | `quantidade_atendimentos` → `quantidade_evolucoes` → `quantidade_participacoes` |
| Unidades de atendimento | `unidades_atendimento` |
| PAI | `possui_registro_pai` → `data_ocorrencia_pai` → `origem_data_ocorrencia_pai` → `situacao_data_ocorrencia_pai` → `data_registro_pai` → `data_resposta_pai` → `data_inclusao_pai` → `data_inclusao_pai_informada` → `flag_incluido_acompanhamento_pai` → `flag_empregabilidade_imediata` → `capacidade_habilidades` → `flag_interesse_curso` → `demandas_pai` → `encaminhamentos_pai` → `observacoes_pai` |
| Atendimento social | `data_ocorrencia_atendimento_social` → `origem_data_ocorrencia_atendimento_social` → `situacao_data_ocorrencia_atendimento_social` → `data_registro_atendimento_social` → `data_resposta_atendimento_social` → `demandas_atendimento_social` → `encaminhamentos_atendimento_social` → `observacoes_atendimento_social` → `data_ultimo_atend_tecnico_informada` |
| Saúde | `data_ocorrencia_saude` → `origem_data_ocorrencia_saude` → `situacao_data_ocorrencia_saude` → `data_registro_saude` → `data_resposta_saude` → `uso_substancias_psicoativas` → `situacao_saude` → `local_tratamento` → `possui_problema_saude` → `realiza_acompanhamento_saude` → `observacoes_saude` |
| Situação de rua | `data_ocorrencia_situacao_rua` → `origem_data_ocorrencia_situacao_rua` → `situacao_data_ocorrencia_situacao_rua` → `data_registro_situacao_rua` → `data_resposta_situacao_rua` → `motivo_principal_permanencia_rua` → `motivo_secundario_permanencia_rua` → `possui_referencias_familiares` → `territorio_referencia_familia` → `possibilidade_reinsercao_familiar` |
| Desligamento | `data_ocorrencia_desligamento_pai` → `origem_data_ocorrencia_desligamento_pai` → `situacao_data_ocorrencia_desligamento_pai` → `data_hora_registro_desligamento_pai` → `data_hora_resposta_desligamento_pai` → `data_desligamento_pai` → `data_desligamento_pai_informada` → `motivo_desligamento_pai` → `complemento_motivo_desligamento_pai` → `observacoes_desligamento_pai` |
| Participação | `ultima_participacao_atividade` → `ultima_participacao_unidade` → `ultima_participacao_data` → `ultima_participacao_hora` |
| Coleções e resumos do mês | `atendimentos` → `evolucoes` → `ocorrencias` → `procedimentos_mes` → `encaminhamentos_mes` |

### `centro_pop.historico_mensal.atendimentos`

| Assunto | Colunas na ordem |
|---|---|
| Identificação e origem | `id_atendimento` → `id_atendimento_modulo` → `origem_modulo` → `id_familia` |
| Unidade | `id_unidade` → `nome_unidade` → `tipo_unidade` → `cas` |
| Data e hora | `data_atendimento` → `hora_atendimento` |
| Tipo e classificação | `id_tipo_atendimento` → `tipo_atendimento_descricao` → `tipo_servico` → `classificacao_atendimento` → `elegivel_indicador` |
| Profissionais | `profissionais` |
| Cadastro e e-mails | `cadastrante` → `data_cadastro_atendimento` → `email_unidade` → `email_cas` |

### `centro_pop.historico_mensal.atendimentos.profissionais`

| Assunto | Colunas na ordem |
|---|---|
| Identificação e referência | `id_profissional` → `nome_profissional` → `id_familia_referencia` → `id_atendimento_legado` |
| Ocupação | `codigos_cbo` → `descricoes_cbo` → `cbo_principal_codigo` → `cbo_principal_descricao` → `profissional_cbo` |
| Classificação da participação | `classificacao_atendimento` → `nome_atendimento` |

### `centro_pop.historico_mensal.evolucoes`

| Assunto | Colunas na ordem |
|---|---|
| Identificação e origem | `id_evolucao` → `id_evolucao_modulo` → `origem_modulo` → `id_familia` → `codigo_abrangencia` → `id_atividade` |
| Unidade | `id_unidade` → `nome_unidade` |
| Datas | `data_evolucao` → `data_cancelamento` → `data_evento` |
| Tipo, descrição e atividade | `tipo_evolucao` → `tipo_servico` → `descricao_evolucao` → `nome_atividade` |
| Profissional e formulários | `id_profissional` → `nome_profissional` → `formularios` |

### `centro_pop.historico_mensal.evolucoes.formularios`

| Assunto | Colunas na ordem |
|---|---|
| Formulário, evento e responsáveis | `titulo_formulario` → `id_evolucao_modulo` → `id_evolucao` → `origem_modulo` → `id_familia` → `codigo_abrangencia` → `id_atividade` → `id_unidade` → `id_profissional` → `nome_profissional` → `nome_atividade` |
| Datas e proveniência | `data_evolucao` → `data_cancelamento` → `data_ocorrencia` → `origem_data_ocorrencia` → `situacao_data_ocorrencia` |
| Tipo, observações e respostas | `tipo_evolucao` → `observacoes_html` → `observacoes` → `campos` |

### `centro_pop.historico_mensal.evolucoes.formularios.campos`

| Assunto | Colunas na ordem |
|---|---|
| Resposta | `label` → `valor` |

### `centro_pop.historico_mensal.ocorrencias`

| Assunto | Colunas na ordem |
|---|---|
| Identificação, origem e unidade | `id_registro` → `origem` → `tipo_registro` → `abrangencia` → `id_evolucao_modulo` → `titulo_formulario` → `id_unidade` → `nome_unidade` |
| Datas e proveniência | `data_ocorrencia` → `origem_data_ocorrencia` → `situacao_data_ocorrencia` → `data_registro` → `data_resposta` → `data_evento` |
| PAI e inclusão | `data_inclusao_pai` → `data_inclusao_informada` → `empregabilidade_imediata` → `capacidade_habilidades` → `interesse_curso` |
| Demandas, encaminhamentos e acesso | `demandas` → `encaminhamentos` → `observacoes` → `resultado_acesso` → `resultado_descricao` |
| Saúde | `uso_substancias_psicoativas` → `situacao_saude` → `local_tratamento` → `possui_problema_saude` → `realiza_acompanhamento_saude` |
| Situação de rua e referências | `motivo_principal_permanencia_rua` → `motivo_secundario_permanencia_rua` → `possui_referencias_familiares` → `territorio_referencia_familia` → `possibilidade_reinsercao_familiar` |
| Desligamento | `data_desligamento` → `data_desligamento_informada` → `motivo_desligamento` → `motivo_desligamento_outros` |
| Participação e conflitos | `id_atividade` → `nome_atividade` → `data_presenca` → `hora_presenca` → `conflitos` |

### `centro_pop.historico_mensal.ocorrencias.encaminhamentos`

| Assunto | Colunas na ordem |
|---|---|
| Encaminhamento | `label` → `valor` |

## CREAS

### `creas`

| Assunto | Colunas na ordem |
|---|---|
| MSE | `mse_id_unidade` → `mse_nome_unidade` → `mse_data_ocorrencia` → `mse_data_registro` → `mse_data` → `mse_data_de_inclusao_no_mse` → `mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` → `mse_com_quem_moraria` → `mse_1o_bimestre_documentacao` → `mse_1o_bimestre_beneficios` → `mse_1o_bimestre_cadunico` → `mse_1o_bimestre_localizacao` → `mse_1o_bimestre_outras_acoes` → `mse_1o_bimestre_saude` → `mse_adolescente_em_reinfracao` → `mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` → `mse_avaliacao_tecnica_bimestral_1o_bimestre` → `mse_cras_de_referencia` → `mse_creas_de_referencia` → `mse_carga_horaria` → `mse_com_quem_residia_anteriormente` → `mse_desdobramento` → `mse_em_caso_de_cumprimento_de_mse_qual_o_status` → `mse_em_caso_de_cumprimento_de_psc_qual_o_local` → `mse_endereco_cras` → `mse_endereco_creas` → `mse_especialidade` → `mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` → `mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` → `mse_local` → `mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` → `mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` → `mse_medida_aplicada` → `mse_nome_do_local_de_cumprimento_de_psc` → `mse_outras_atividades` → `mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` → `mse_procedimento` → `mse_profissional_cras` → `mse_profissional_creas` → `mse_quais_atividades_gostaria_de_participar` → `mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` → `mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` → `mse_telefone_cras` → `mse_telefone_creas` → `mse_tempo_de_cumprimento` → `mse_tipo` → `mse_tipo_de_ato_infracional` → `mse_usuario_interditado` → `mse_observacoes` |
| Retorno MSE | `mse_retorno_id_unidade` → `mse_retorno_nome_unidade` → `mse_retorno_data_ocorrencia` → `mse_retorno_data_registro` → `mse_retorno_data_do_desligamento` → `mse_retorno_adolescente_em_reinfracao` → `mse_retorno_desligamento` → `mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` → `mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` → `mse_retorno_motivo_do_desligamento` → `mse_retorno_nome_do_local_de_cumprimento_de_psc` → `mse_retorno_observacoes` |
| PETI | `peti_id_unidade` → `peti_nome_unidade` → `peti_data_ocorrencia` → `peti_data_registro` → `peti_data_da_inclusao_no_peti` → `peti_demanda_inicial` → `peti_forma_de_acesso_informe_a_origem_de_acesso` → `peti_marcacao_nos_campo_cadunico` → `peti_orientacoes_e_encaminhamentos` → `peti_outras_demandas_vulnerabilidades` → `peti_outras_formas_de_acesso` → `peti_outros` → `peti_tipo_de_trabalho_infantil` → `peti_e_acompanhado_em_outro_creas` → `peti_e_necessario_realizar_a_gestao_de_beneficios` → `peti_observacoes` |
| Desligamento PETI | `peti_desligamento_id_unidade` → `peti_desligamento_nome_unidade` → `peti_desligamento_data_ocorrencia` → `peti_desligamento_data_registro` → `peti_desligamento_data_do_desligamento_do_peti` → `peti_desligamento_motivo_do_desligamento` → `peti_desligamento_outros` → `peti_desligamento_observacoes` |
| SCFV | `scfv_id_unidade` → `scfv_nome_unidade` → `scfv_data_ocorrencia` → `scfv_data_registro` → `scfv_data_da_solicitacao_do_acolhimento` → `scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` → `scfv_nome_do_cras_responsavel` → `scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` → `scfv_oriundo_de_instituicao_de_acolhimento_interditado` → `scfv_usuario_interditado` → `scfv_orgao_de_origem_do_acolhimento` → `scfv_observacoes` |
| Passo a Passo | `passo_a_passo_ultima_unidade` → `passo_a_passo_ultima_atividade` → `passo_a_passo_ultima_data` → `passo_a_passo_ultima_hora` |
| Coleção | `registros` |

### `creas.registros`

| Assunto | Colunas na ordem |
|---|---|
| Unidade | `id_unidade` → `nome_unidade` |
| MSE | `mse_id_unidade` → `mse_nome_unidade` → `mse_data_ocorrencia` → `mse_data_registro` → `mse_data` → `mse_data_de_inclusao_no_mse` → `mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` → `mse_com_quem_moraria` → `mse_1o_bimestre_documentacao` → `mse_1o_bimestre_beneficios` → `mse_1o_bimestre_cadunico` → `mse_1o_bimestre_localizacao` → `mse_1o_bimestre_outras_acoes` → `mse_1o_bimestre_saude` → `mse_adolescente_em_reinfracao` → `mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade` → `mse_avaliacao_tecnica_bimestral_1o_bimestre` → `mse_cras_de_referencia` → `mse_creas_de_referencia` → `mse_carga_horaria` → `mse_com_quem_residia_anteriormente` → `mse_desdobramento` → `mse_em_caso_de_cumprimento_de_mse_qual_o_status` → `mse_em_caso_de_cumprimento_de_psc_qual_o_local` → `mse_endereco_cras` → `mse_endereco_creas` → `mse_especialidade` → `mse_gostaria_de_retornar_para_casa_ou_convivio_anterior` → `mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria` → `mse_local` → `mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia` → `mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia` → `mse_medida_aplicada` → `mse_nome_do_local_de_cumprimento_de_psc` → `mse_outras_atividades` → `mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia` → `mse_procedimento` → `mse_profissional_cras` → `mse_profissional_creas` → `mse_quais_atividades_gostaria_de_participar` → `mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido` → `mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc` → `mse_telefone_cras` → `mse_telefone_creas` → `mse_tempo_de_cumprimento` → `mse_tipo` → `mse_tipo_de_ato_infracional` → `mse_usuario_interditado` → `mse_observacoes` |
| Retorno MSE | `mse_retorno_id_unidade` → `mse_retorno_nome_unidade` → `mse_retorno_data_ocorrencia` → `mse_retorno_data_registro` → `mse_retorno_data_do_desligamento` → `mse_retorno_adolescente_em_reinfracao` → `mse_retorno_desligamento` → `mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status` → `mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local` → `mse_retorno_motivo_do_desligamento` → `mse_retorno_nome_do_local_de_cumprimento_de_psc` → `mse_retorno_observacoes` |
| PETI | `peti_id_unidade` → `peti_nome_unidade` → `peti_data_ocorrencia` → `peti_data_registro` → `peti_data_da_inclusao_no_peti` → `peti_demanda_inicial` → `peti_forma_de_acesso_informe_a_origem_de_acesso` → `peti_marcacao_nos_campo_cadunico` → `peti_orientacoes_e_encaminhamentos` → `peti_outras_demandas_vulnerabilidades` → `peti_outras_formas_de_acesso` → `peti_outros` → `peti_tipo_de_trabalho_infantil` → `peti_e_acompanhado_em_outro_creas` → `peti_e_necessario_realizar_a_gestao_de_beneficios` → `peti_observacoes` |
| Desligamento PETI | `peti_desligamento_id_unidade` → `peti_desligamento_nome_unidade` → `peti_desligamento_data_ocorrencia` → `peti_desligamento_data_registro` → `peti_desligamento_data_do_desligamento_do_peti` → `peti_desligamento_motivo_do_desligamento` → `peti_desligamento_outros` → `peti_desligamento_observacoes` |
| SCFV | `scfv_id_unidade` → `scfv_nome_unidade` → `scfv_data_ocorrencia` → `scfv_data_registro` → `scfv_data_da_solicitacao_do_acolhimento` → `scfv_demanda_de_acolhimento_institucional_por_decisao_judicial` → `scfv_nome_do_cras_responsavel` → `scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos` → `scfv_oriundo_de_instituicao_de_acolhimento_interditado` → `scfv_usuario_interditado` → `scfv_orgao_de_origem_do_acolhimento` → `scfv_observacoes` |
| Passo a Passo | `passo_a_passo_ultima_unidade` → `passo_a_passo_ultima_atividade` → `passo_a_passo_ultima_data` → `passo_a_passo_ultima_hora` |

## Alta complexidade

### `alta_complexidade`

| Assunto | Colunas na ordem |
|---|---|
| Ciclo e unidade | `id_ciclo` → `id_unidade` → `nome_unidade` |
| Entrada e saída | `data_entrada` → `data_saida` |
| Motivo e duração | `motivo_saida` → `tempo_permanencia_dias` |
| Coleção | `registros` |

### `alta_complexidade.registros`

| Assunto | Colunas na ordem |
|---|---|
| Ciclo e unidade | `id_ciclo` → `id_unidade` → `nome_unidade` |
| Entrada e saída | `data_entrada` → `data_saida` |
| Motivo e duração | `motivo_saida` → `tempo_permanencia_dias` |
