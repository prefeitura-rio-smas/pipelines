{{ config(materialized = 'ephemeral') }}

SELECT
  *,

  -- Coordenadas: campo composto que não existe na raw
  CONCAT(y, ', ', x) AS coordenadas,

  -- note_creas_tratada: versão capitalizada (nome diferente da raw)
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

  -- permanencia_rua_tratada: versão legível
  CASE
   WHEN permanencia_rua = 'h24' THEN '24 horas'
   WHEN permanencia_rua = 'apenas_durante_dia' THEN 'Apenas durante o dia'
   WHEN permanencia_rua = 'durante_semana_retorna_casa_final_de_semana' THEN 'Durante a semana'
   WHEN permanencia_rua = 'frequenta_cenas_de_uso_esporadicamente' THEN 'Frequenta cenas de uso'
   WHEN permanencia_rua = 'apenas_durante_noite' THEN 'Apenas durante a noite'
   WHEN permanencia_rua = 'nao_sabe_nao_respondeu' THEN 'NS/NR'
   ELSE permanencia_rua
   END AS permanencia_rua_tratada,

  -- tempo_permanencia_tratada: versão legível
  CASE
   WHEN tempo_permanencia = 'de_1_a_3_anos' THEN 'De 1 a 3 anos'
   WHEN tempo_permanencia = 'de_3_a_6_anos' THEN 'De 3 a 6 anos'
   WHEN tempo_permanencia = 'de_1_a_3_meses' THEN 'De 1 a 3 meses'
   WHEN tempo_permanencia = 'menos_3_dias' THEN 'Menos de 3 dias'
   WHEN tempo_permanencia = 'de_6_meses_a_1_ano' THEN 'De 6 meses a 1 ano'
   WHEN tempo_permanencia = 'de_6_a_10_anos' THEN 'De 6 a 10 anos'
   WHEN tempo_permanencia = 'de_3_a_6_meses' THEN 'De 3 a 6 meses'
   WHEN tempo_permanencia = 'mais_de_10_anos' THEN 'Mais de 10 anos'
   WHEN tempo_permanencia = 'de_7_a_30_dias' THEN 'De 7 a 30 dias'
   WHEN tempo_permanencia = 'de_3_a_7_dias' THEN 'De 3 a 7 dias'
   WHEN tempo_permanencia = 'ns_nr' THEN 'NS/NR'
   ELSE tempo_permanencia
   END AS tempo_permanencia_tratada,

  -- migrante_terra_natal_tratada
  CASE
   WHEN migrante_terra_natal = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
   WHEN migrante_terra_natal = 'sim' THEN 'Sim'
   WHEN migrante_terra_natal = 'nao' THEN 'Não'
   ELSE migrante_terra_natal
   END AS migrante_terra_natal_tratada,

  -- possui_referencia_tratada
  CASE
   WHEN possui_referencia = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
   WHEN possui_referencia = 'sim' THEN 'Sim'
   WHEN possui_referencia = 'nao' THEN 'Não'
   ELSE possui_referencia
   END AS possui_referencia_tratada,

  -- escolaridade_tratada
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

  -- ocupacao_tratada
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

  -- aceita_acolhimento_tratada
  CASE
   WHEN aceita_acolhimento IS NULL THEN 'nao'
   WHEN TRIM(LOWER(aceita_acolhimento)) = 'n/a' THEN 'nao'
   ELSE aceita_acolhimento
   END AS aceita_acolhimento_tratada,

  -- unidade_destino_tratada
  CASE
   WHEN unidade_destino = 'albergue' THEN 'Albergue'
   WHEN unidade_destino = 'central_recepcao' THEN 'Central de Recepção'
   WHEN unidade_destino = 'outros' THEN 'Outros'
   WHEN unidade_destino = 'com_terapeutica' THEN 'Comunidade Terapêutica'
   WHEN unidade_destino = 'urs' THEN 'URS'
   ELSE unidade_destino
   END AS unidade_destino_tratada,

  -- equipamento_destino_tratada
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

  -- encam_creas_tratada
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

  -- encam_centropop_tratada
  CASE
   WHEN encam_centropop = 'centro_pop_jose_saramago' THEN 'Centro Pop José Saramago'
   WHEN encam_centropop = 'centro_pop_barbara_calazans' THEN 'Centro Pop Barbara  Calazans'
   ELSE encam_centropop
   END AS encam_centropop_tratada,

  -- encaminhamento_rede_tratada
  ARRAY_TO_STRING(
    ARRAY(
      SELECT
        CASE
          WHEN LOWER(TRIM(part)) = 'cras' THEN 'CRAS'
          WHEN LOWER(TRIM(part)) = 'defensoria_publica' THEN 'Defensoria Pública'
          WHEN LOWER(TRIM(part)) = 'fundacao_leaoxii' THEN 'Fundação Leão XIII'
          WHEN LOWER(TRIM(part)) = 'outros' THEN 'Outros'
          WHEN LOWER(TRIM(part)) = 'creas' THEN 'CREAS'
          WHEN LOWER(TRIM(part)) = 'conselho_tutelar' THEN 'Conselho Tutelar'
          WHEN LOWER(TRIM(part)) = 'detran' THEN 'DETRAN'
          WHEN LOWER(TRIM(part)) = 'receita_federal' THEN 'Receita Federal'
          WHEN LOWER(TRIM(part)) = 'centro_pop' THEN 'Centro POP'
          WHEN LOWER(TRIM(part)) = 'encaminhamento_de_saude' THEN 'Encaminhamento de Saúde'
          WHEN LOWER(TRIM(part)) = 'cartorio' THEN 'Cartório'
          WHEN LOWER(TRIM(part)) = 'delegacia' THEN 'Delegacia'
          WHEN LOWER(TRIM(part)) = 'nao_houve_encaminhamento' THEN 'Sem encaminhamentos'
          ELSE encaminhamento_rede
        END
      FROM UNNEST(SPLIT(encaminhamento_rede, ',')) AS part
    ),
    ', '
  ) AS encaminhamento_rede_tratada,

  -- mes_abrev_data_abordagem: garante ponto final
  CASE
  WHEN RIGHT(mes_abrev_data_abordagem, 1) = '.' THEN mes_abrev_data_abordagem
  ELSE CONCAT(mes_abrev_data_abordagem, '.')
  END AS mes_abrev_data_abordagem

FROM {{ source('arcgis_raw', 'abordagem_raw') }}
