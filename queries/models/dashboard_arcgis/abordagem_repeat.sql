
SELECT
  objectid,
  globalid,
  CASE
   WHEN repeat_unidade_calculo = 'CREAS MARIA LINA DE CASTRO LIMA' THEN 'Creas Maria Lina De Castro Lima'
   WHEN repeat_unidade_calculo = 'CREAS SIMONE DE BEAUVOIR' THEN 'Creas Simone De Beauvoir'
   WHEN repeat_unidade_calculo = 'CREAS ARLINDO RODRIGUES' THEN 'Creas Arlindo Rodrigues'
   WHEN repeat_unidade_calculo = 'CREAS JANETE CLAIR' THEN 'Creas Janete Clair'
   WHEN repeat_unidade_calculo = 'CREAS PROFESSORA ALDAIZA SPOSATI' THEN 'Creas Professora Aldaiza Sposati'
   WHEN repeat_unidade_calculo = 'CREAS PROFESSORA MÁRCIA LOPES' THEN 'Creas Professora Marcia Lopes'
   WHEN repeat_unidade_calculo = 'CREAS STELLA MARIS' THEN 'Creas Stella Maris'
   WHEN repeat_unidade_calculo = 'CREAS NELSON CARNEIRO' THEN 'Creas Nelson Carneiro'
   WHEN repeat_unidade_calculo = 'CREAS WANDA ENGEL ADUAN' THEN 'Creas Wanda Engel Aduan'
   WHEN repeat_unidade_calculo = 'CREAS JOÃO HÉLIO FERNANDES VIEITES' THEN 'Creas Joao Helio Fernandes Vieites'
   WHEN repeat_unidade_calculo = 'CREAS DANIELA PEREZ' THEN 'Creas Daniela Perez'
   WHEN repeat_unidade_calculo = 'CREAS PADRE GUILHERME DECAMINADA' THEN 'Creas Padre Guilherme Decaminada'
   WHEN repeat_unidade_calculo = 'CREAS ZILDA ARNS NEUMANN' THEN 'Creas Zilda Arns Neumann'
   WHEN repeat_unidade_calculo = 'CREAS JOÃO MANUEL MONTEIRO' THEN 'Creas Joao Manuel Monteiro'
    WHEN repeat_unidade_calculo = 'CENTRO POP JOSÉ SARAMAGO' THEN 'Centro Pop José Saramago'
   WHEN repeat_unidade_calculo = 'CENTRO POP BÁRBARA CALAZANS' THEN 'Centro Pop Barbara  Calazans'
   ELSE repeat_unidade_calculo
   END AS repeat_unidade_calculo_tratada,
  repeat_unidade_cas,
  repeat_nome_usuario,

  SAFE.PARSE_DATE('%d/%m/%Y', repeat_data_nascimento) AS repeat_data_nascimento,

  repeat_idade,
  repeat_faixa_etaria,
  CASE
   WHEN repeat_estado_nascimento = 'rio_de_janeiro' THEN 'Rio de Janeiro'
   WHEN repeat_estado_nascimento = 'outros_estados' THEN 'Outros Estados'
   WHEN repeat_estado_nascimento = 'outro_pais' THEN 'Outro País'
   WHEN repeat_estado_nascimento = 'ns_nr' THEN 'NS/NR'
   ELSE repeat_estado_nascimento
   END AS repeat_estado_nascimento_tratada,
  repeat_nome_mae,
  repeat_nome_pai,
  CASE 
   WHEN repeat_raca_cor_etnia = 'parda' THEN 'Parda'
   WHEN repeat_raca_cor_etnia = 'branca' THEN 'Branca'
   WHEN repeat_raca_cor_etnia = 'preta' THEN 'Preta'
   WHEN repeat_raca_cor_etnia = 'amarela' THEN 'Amarela'
   WHEN repeat_raca_cor_etnia = 'indígena' THEN 'Indígena'
   WHEN repeat_raca_cor_etnia = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
   ELSE repeat_raca_cor_etnia
   END AS repeat_raca_cor_etnia_tratada,
  CASE
   WHEN repeat_sexo = 'masculino' THEN 'Masculino'
   WHEN repeat_sexo = 'feminino' THEN 'Feminino'
   WHEN repeat_sexo = 'intersexo' THEN 'Intersexo'
   WHEN repeat_sexo = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
   ELSE repeat_sexo
   END AS repeat_sexo,
  turno_abordagem,

  DATE(data_abordagem) AS data_abordagem,
  ano_num_data_abordagem,
  dia_num_data_abordagem,
  CASE
  WHEN RIGHT(mes_abrev_data_abordagem, 1) = '.' THEN mes_abrev_data_abordagem
  ELSE CONCAT(mes_abrev_data_abordagem, '.')
  END AS mes_abrev_data_abordagem,

  ano_mes_data_abordagem,
  bairro_abord,

  CONCAT(y, ', ', x) AS coordenadas,

  CASE
   WHEN note_creas = 'CREAS Janete Clair' THEN 'Creas Janete Clair'
   WHEN note_creas = 'CREAS Maria Lina de Castro Lima' THEN 'Creas Maria Lina De Castro Lima'
   WHEN note_creas = 'CREAS Daniela Perez' THEN 'Creas Daniela Perez'
   WHEN note_creas = 'CREAS Stella Maris' THEN 'Creas Stella Maris'
   WHEN note_creas = 'CREAS Nélson Carneiro' THEN 'Creas Nelson Carneiro'
   WHEN note_creas = 'CREAS Padre Guilherme Decaminada' THEN 'Creas Padre Guilherme Decaminada'
   WHEN note_creas = 'CREAS Professora Márcia Lopes' THEN 'Creas Professora Márcia Lopes'
   WHEN note_creas = 'CREAS Professora Aldaíza Sposati' THEN 'Creas Professora Aldaíza Sposati'
   WHEN note_creas = 'CREAS João Hélio Fernandes Vieites' THEN 'Creas João Hélio Fernandes Vieites'
   WHEN note_creas = 'CREAS Wanda Engel Aduan' THEN 'Creas Wanda Engel Aduan'
   WHEN note_creas = 'CREAS Zilda Arns Neumann' THEN 'Creas Zilda Arns Neumann'
   WHEN note_creas = 'CREAS Nelson Carneiro' THEN 'Creas Nelson Carneiro'
   WHEN note_creas = 'CREAS João Manoel Monteiro' THEN 'Creas João Manoel Monteiro'
   WHEN note_creas = 'CREAS Simone de Beauvoir' THEN 'Creas Simone de Beauvoir'
   WHEN note_creas = 'CREAS Arlindo Rodrigues' THEN 'Creas Arlindo Rodrigues'
   ELSE note_creas
   END AS note_creas_tratada,
  CASE
   WHEN resp_abordagem = 'cgppsr' THEN 'CTPR'
   WHEN resp_abordagem = 'creas' THEN 'CREAS'
   WHEN resp_abordagem = 'centro_pop' THEN 'CENTRO POP'
   ELSE resp_abordagem
   END AS resp_abordagem,
  CASE resp_abordagem1
   WHEN 'ouvidoria1' THEN 'Ouvidoria CTPR'
   WHEN'ouvidoria2' THEN 'Ouvidoria CREAS'
   WHEN'ouvidoria3' THEN 'Ouvidoria Centro POP'
   WHEN'tenda_acol_direitos' THEN 'Tenda acolhe com direitos'
   WHEN'abord_itinerante' THEN 'Abordagem CTPR'
   WHEN'deman_emergencial1' THEN 'Demanda emergencial CTPR'
   WHEN'naas' THEN 'NAAS'
   WHEN'itinerante' THEN 'Abordagem CREAS'
   WHEN'deman_emergencial2' THEN 'Demanda emergencial CREAS'
   WHEN'abordagem_social' THEN 'Abordagem Centro POP'
   WHEN'demanda_emergencial' THEN 'Demanda emergencial Centro POP'
   WHEN'ncp' THEN 'NCP'
   ELSE resp_abordagem1
   END AS resp_abordagem1,
  acao_conjunta,
  CASE
   WHEN permanencia_rua = 'h24' THEN '24 horas'
   WHEN permanencia_rua = 'apenas_durante_dia' THEN 'Apenas durante o dia'
   WHEN permanencia_rua = 'durante_semana_retorna_casa_final_de_semana' THEN 'Durante a semana'
   WHEN permanencia_rua = 'frequenta_cenas_de_uso_esporadicamente' THEN 'Frequenta cenas de uso'
   WHEN permanencia_rua = 'apenas_durante_noite' THEN 'Apenas durante a noite'
   WHEN permanencia_rua = 'nao_sabe_nao_respondeu' THEN 'NS/NR'
   ELSE permanencia_rua
   END AS permanencia_rua_tratada,
  CASE
   WHEN tempo_permanencia = 'de_1_a_3_anos' THEN 'De 1 a 3 anos'
   WHEN tempo_permanencia = 'de_3_a_6_anos' THEN 'De 3 a 6 anos'
   WHEN tempo_permanencia = 'de_1_a_3_meses' THEN 'De 1 a 3 meses'
   WHEN tempo_permanencia = 'menos_3_dias' THEN 'Menos de 3 dias'
   WHEN tempo_permanencia = 'de_6_meses_a_1_ano' THEN 'De 6 meses a 1 ano'
   WHEN tempo_permanencia = 'de_6_a_10_anos' THEN 'De 6 a 10 anos'
   WHEN tempo_permanencia = 'de_3_a_6_meses' THEN 'De 3 a 6 meses'
   WHEN tempo_permanencia = 'mais_de_10_anos' THEN 'Mais de 10 anos'
   WHEN tempo_permanencia = 'de_7_a_30_dias' THEN 'De 7 a 30 anos'
   WHEN tempo_permanencia = 'de_3_a_7_dias' THEN 'De 3 a 7 dias'
   WHEN tempo_permanencia = 'ns_nr' THEN 'NS/NR'
   ELSE tempo_permanencia
   END AS tempo_permanencia_tratada,
  principal_motivo_permanencia,

  flag_conflito_familiar,
  flag_abandono_familiar,
  flag_desocupacao,
  flag_orfandade,
  flag_acesso_a_renda,
  flag_desemprego,
  flag_desemprego_dos_pais,
  flag_transtorno_psiquiatrico,
  flag_uso_drogas_ilicitas,
  flag_trabalho_infantil,
  flag_exploracao_sexual,
  flag_egresso_sistema_prisional,
  flag_alcoolismo_motivo,
  flag_violencia_conflito_comuni,
  flag_preferencia_vontade_prop,
  flag_egresso_mse,
  flag_imigrante,
  flag_migrante,
  flag_refugiado,
  flag_ns_nr_motivo,
  e_migrante,
  CASE
   WHEN migrante_terra_natal = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
   WHEN migrante_terra_natal = 'sim' THEN 'Sim'
   WHEN migrante_terra_natal = 'nao' THEN 'Não'
   ELSE migrante_terra_natal
   END AS migrante_terra_natal_tratada,
  CASE
   WHEN possui_referencia = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
    WHEN possui_referencia = 'sim' THEN 'Sim'
    WHEN possui_referencia = 'nao' THEN 'Não'
   ELSE possui_referencia
   END AS possui_referencia_tratada,
  possui_documento,
  documentacao,

  flg_documentacao_identidade,
  flg_documentacao_cnh,
  flg_documentacao_registro_nasc,
  flg_documentacao_ctps,
  flg_documentacao_cpf,
  flg_documentacao_passaporte,
  CASE
   WHEN escolaridade = 'medio_incompleto' THEN 'Médio incompleto'
   WHEN escolaridade = 'fundamental_incompleto' THEN 'Fundamental incompleto'
   WHEN escolaridade = 'medio_completo' THEN 'Médio completo'
   WHEN escolaridade = 'fundamental_completo' THEN 'Fundamental completo'
   WHEN escolaridade = 'nao_alfabetizado' THEN 'Não alfabetizado'
   WHEN escolaridade = 'nao_sabe_nao_respondeu' THEN 'Não sabe/Não respondeu'
   WHEN escolaridade = 'superior_completo' THEN 'Superior completo'
   WHEN escolaridade = 'superior_incompleto' THEN 'Superior incompleto'
   WHEN escolaridade = 'nao_escolarizado' THEN 'Não escolarizado'
   ELSE escolaridade
   END AS escolaridade_tratada,
  recebe_beneficio,
  beneficios,

  flag_bolsa_familia,
  flag_bpc,
  flag_seguro_desemprego,
  flag_aposentadoria,
  flag_pensionista,
  flag_outros_ben,
  beneficios_outros,
  CASE
   WHEN ocupacao = 'catador' THEN 'Catador'
   WHEN ocupacao = 'pedinte' THEN 'Pedinte'
   WHEN ocupacao = 'ambulante' THEN 'Ambulante'
   WHEN ocupacao = 'bicos' THEN 'Bicos'
   WHEN ocupacao = 'outros' THEN 'Outros'
   WHEN ocupacao = 'impossibilitado_para_trabalho' THEN 'Impossibilitado'
   WHEN ocupacao = 'prostituicao' THEN 'Prostituição'
   WHEN ocupacao = 'ns_nr' THEN 'NS/NR' 
   ELSE ocupacao
   END AS ocupacao_tratada,

  flag_nao_tem_interesse,
  flag_gastronomia,
  flag_beleza,
  flag_pequenos_reparos,
  flag_jardinagem,
  flag_empreendedorismo,
  flag_producao_artesanal,
  flag_inclusao_digital,
  flag_outros_curso,
  flag_curso_ns_nr,
  flag_nao_tem,

  flag_alcoolismo,
  flag_asma_bronquite,
  flag_covid_19,
  flag_cancer_tumores,
  flag_diabetes,
  flag_dengue,
  flag_depen_quimica,
  flag_epilepsia,
  flag_escabiose,
  flag_hanseniase,
  flag_hepatite,
  flag_hipertensao_doenca_cardio,
  flag_hiv_aids,
  flag_pneumonia,
  flag_sarampo,
  flag_sifilis,
  flag_transtorno_mental,
  flag_trauma_fisico,
  flag_tuberculose,
  flag_ns_nr,

  IFNULL(aceita_acolhimento, 'N/A') AS aceita_acolhimento_tratada,
  motivo_acolhimento_nao,
  outro_motivo,

  flag_motivo_renda,
  flag_motivo_regra,
  flag_motivo_moradia,
  flag_motivo_animal,
  flag_motivo_n_interesse,
  flag_motivo_outro,
  CASE
   WHEN unidade_destino = 'albergue' THEN 'Albergue'
   WHEN unidade_destino = 'central_recepcao' THEN 'Central de Recepção'
   WHEN unidade_destino = 'outros' THEN 'Outros'
   WHEN unidade_destino = 'com_terapeutica' THEN 'Comunidade Terapêutica'
   WHEN unidade_destino = 'urs' THEN 'URS'
   ELSE unidade_destino
   END AS unidade_destino_tratada,
  CASE equipamento_destino
    WHEN 'albergue_dercy_gonçalves' THEN 'Albergue Dercy Gonçalves'
    WHEN 'albergue_nise_da_silveira' THEN 'Albergue Nise da Silveira'
    WHEN 'craf_tom_jobim' THEN 'Craf Tom Jobim'
    WHEN 'assoc_maranatha_rj_madureira' THEN 'Associação Maranatha RJ Madureira'
    WHEN 'albergue_martin_luther_kingjr' THEN 'Albergue Martin Luther King Jr'
    WHEN 'assoc_maranatha_rj_lins_de_vasconcelos' THEN 'Associação Maranatha RJ Lins de Vasconcelos'
    WHEN 'inst_social_marca_de_cristo' THEN 'Instituto Social Marca de Cristo'
    WHEN 'urs_rio_acolhedor_paciencia' THEN 'Urs Rio Acolhedor Paciência'
    WHEN 'cri_pastor_carlos_portela' THEN 'Cri Pastor Carlos Portela'
    WHEN 'urs_haroldo_costa' THEN 'Urs Haroldo Costa'
    WHEN 'albergue_mais_tempo_lgbtqia' THEN 'Albergue Mais Tempo LGBTQIA'
    WHEN 'assoc_maranatha_rj_padre_miguel' THEN 'Associação Maranatha RJ Padre Miguel'
    WHEN 'assoc_maranatha_rj_vila_kennedy' THEN 'Associação Maranatha RJ Vila Kennedy'
    WHEN 'albergue_betinho' THEN 'Albergue Betinho'
    WHEN 'inst_revivendo_com_cristo' THEN 'Instituto Revivendo com Cristo'
    WHEN 'crca_ademar_ferreira_de_oliveira' THEN 'CRCA Ademar Ferreira de Oliveira'
    WHEN 'assoc_maranatha_rj_bangu' THEN 'Associação Maranatha RJ Bangu'
    WHEN 'crca_taiguara' THEN 'CRCA Taiguara'
    WHEN 'assoc_maranatha_rj_sepetiba' THEN 'Associação Maranatha RJ Sepetiba'
    WHEN 'albergue_alfonso_lavalle' THEN 'Albergue Alfonso Lavalle'
    WHEN 'assoc_maranatha_rj_vila_valqueire' THEN 'Associação Maranatha RJ Vila Valqueire'
    WHEN 'assoc_de_assistencia_social_videira' THEN 'Associação de Assistência Social Videira'
    WHEN 'projeto_alcançando_vidas' THEN 'Projeto Alcançando Vidas'
    WHEN 'camor' THEN 'CAMOR'
    WHEN 'assoc_maranatha_rj_engenho_de_dentro' THEN 'Associação Maranatha RJ Engenho de Dentro'
    WHEN 'inst_social_manasses_campo_grande2' THEN 'Instituto Social Manassés Campo Grande 2'
    WHEN 'inst_social_manasses_campo_grande1' THEN 'Instituto Social Manassés Campo Grande 1'
    WHEN 'comt_valentes_de_davi_escola_de_profetas' THEN 'COMT Valentes de Davi Escola de Profetas'
   ELSE equipamento_destino
   END AS equipamento_destino_tratada,
  CASE
   WHEN encam_creas = 'creas_maria_lina_de_castro_lima' THEN 'Creas Maria Lina De Castro Lima'
   WHEN encam_creas = 'creas_simone_de_beauvoir' THEN 'Creas Simone De Beauvoir'
   WHEN encam_creas = 'creas_arlindo_rodrigues' THEN 'Creas Arlindo Rodrigues'
   WHEN encam_creas = 'creas_janete_clair' THEN 'Creas Janete Clair'
   WHEN encam_creas = 'creas_professora_aldaiza_sposati' THEN 'Creas Professora Aldaiza Sposati'
   WHEN encam_creas = 'creas_professora_marcia_lopes' THEN 'Creas Professora Marcia Lopes'
   WHEN encam_creas = 'creas_stella_maris' THEN 'Creas Stella Maris'
   WHEN encam_creas = 'creas_nelson_carneiro' THEN 'Creas Nelson Carneiro'
   WHEN encam_creas = 'creas_wanda_engel_aduan' THEN 'Creas Wanda Engel Aduan'
   WHEN encam_creas = 'creas_joao_helio_fernandes_vieites' THEN 'Creas Joao Helio Fernandes Vieites'
   WHEN encam_creas = 'creas_daniela_perez' THEN 'Creas Daniela Perez'
   WHEN encam_creas = 'creas_padre_guilherme_decaminada' THEN 'Creas Padre Guilherme Decaminada'
   WHEN encam_creas = 'creas_zilda_arns_neumann' THEN 'Creas Zilda Arns Neumann'
   WHEN encam_creas = 'creas_joao_manuel_monteiro' THEN 'Creas Joao Manuel Monteiro'
   ELSE encam_creas
   END AS encam_creas_tratada,
  CASE
   WHEN encam_centropop = 'centro_pop_jose_saramago' THEN 'Centro Pop José Saramago'
   WHEN encam_centropop = 'centro_pop_barbara_calazans' THEN 'Centro Pop Barbara  Calazans'
   ELSE encam_centropop
   END AS encam_centropop_tratada,
  encam_cras,

  flg_sem_encaminhamento,
  flg_encam_creas,
  flg_encam_centropop,
  flg_encam_cras,
  flg_encam_conselhotutelar,
  flg_encam_encaminhamento_saude,
  flg_encam_defensoria_publica,
  flg_encam_detran,
  flg_encam_cartorio,
  flg_encam_fundacao_leaoxiii,
  flg_encam_receita_federal,
  flg_encam_delegacia,
  flg_encam_outros,
  parentrowid,
  created_user

 FROM {{ source('arcgis_raw', 'abordagem_repeat_raw') }}