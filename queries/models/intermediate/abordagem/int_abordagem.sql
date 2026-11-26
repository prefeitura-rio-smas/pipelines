-- Camada Intermediate: entidade de abordagem (conformada).
-- Consolida o formulário repeat, o cluster de dedup, a ficha de origem e o
-- de/para de e-mails. Traduz os rótulos de domínio e expõe as chaves de dedup
-- (a raw guarda somente os valores brutos). A mart_abordagem é apenas a
-- apresentação final. Grão: 1 linha por abordagem (globalid).
-- O ficha_objectid é nulo em toda a base (join legado pendente de investigação),
-- mantido para preservar o contrato de 154 colunas.
{{ config(materialized = 'ephemeral') }}

with repeat as (
    select * from {{ ref('raw_abordagem_repeat') }}
),

dedup as (
    select * from {{ ref('int_abordagem_repeat_dedup') }}
),

emails as (
    select * from {{ ref('raw_abordagem_filtro_emails') }}
),

ficha as (
    select objectid from {{ ref('raw_abordagem_ficha') }}
)

-- A ordem das colunas é o contrato; o REPLACE mantém as posições.
select  -- noqa: ST06
    r.*  -- noqa: AM04
    replace (
          CASE
           WHEN repeat_unidade_calculo_tratada = 'CREAS MARIA LINA DE CASTRO LIMA' THEN 'Creas Maria Lina De Castro Lima'
           WHEN repeat_unidade_calculo_tratada = 'CREAS SIMONE DE BEAUVOIR' THEN 'Creas Simone De Beauvoir'
           WHEN repeat_unidade_calculo_tratada = 'CREAS ARLINDO RODRIGUES' THEN 'Creas Arlindo Rodrigues'
           WHEN repeat_unidade_calculo_tratada = 'CREAS JANETE CLAIR' THEN 'Creas Janete Clair'
           WHEN repeat_unidade_calculo_tratada = 'CREAS PROFESSORA ALDAIZA SPOSATI' THEN 'Creas Professora Aldaiza Sposati'
           WHEN repeat_unidade_calculo_tratada = 'CREAS PROFESSORA MÁRCIA LOPES' THEN 'Creas Professora Marcia Lopes'
           WHEN repeat_unidade_calculo_tratada = 'CREAS STELLA MARIS' THEN 'Creas Stella Maris'
           WHEN repeat_unidade_calculo_tratada = 'CREAS NELSON CARNEIRO' THEN 'Creas Nelson Carneiro'
           WHEN repeat_unidade_calculo_tratada = 'CREAS WANDA ENGEL ADUAN' THEN 'Creas Wanda Engel Aduan'
           WHEN repeat_unidade_calculo_tratada = 'CREAS JOÃO HÉLIO FERNANDES VIEITES' THEN 'Creas Joao Helio Fernandes Vieites'
           WHEN repeat_unidade_calculo_tratada = 'CREAS DANIELA PEREZ' THEN 'Creas Daniela Perez'
           WHEN repeat_unidade_calculo_tratada = 'CREAS PADRE GUILHERME DECAMINADA' THEN 'Creas Padre Guilherme Decaminada'
           WHEN repeat_unidade_calculo_tratada = 'CREAS ZILDA ARNS NEUMANN' THEN 'Creas Zilda Arns Neumann'
           WHEN repeat_unidade_calculo_tratada = 'CREAS JOÃO MANUEL MONTEIRO' THEN 'Creas Joao Manuel Monteiro'
            WHEN repeat_unidade_calculo_tratada = 'CENTRO POP JOSÉ SARAMAGO' THEN 'Centro Pop José Saramago'
           WHEN repeat_unidade_calculo_tratada = 'CENTRO POP BÁRBARA CALAZANS' THEN 'Centro Pop Barbara  Calazans'
           ELSE repeat_unidade_calculo_tratada
           END AS repeat_unidade_calculo_tratada,
          CASE
           WHEN repeat_estado_nascimento_tratada = 'rio_de_janeiro' THEN 'Rio de Janeiro'
           WHEN repeat_estado_nascimento_tratada = 'outros_estados' THEN 'Outros Estados'
           WHEN repeat_estado_nascimento_tratada = 'outro_pais' THEN 'Outro País'
           WHEN repeat_estado_nascimento_tratada = 'ns_nr' THEN 'NS/NR'
           ELSE repeat_estado_nascimento_tratada
           END AS repeat_estado_nascimento_tratada,
          CASE 
           WHEN repeat_raca_cor_etnia_tratada = 'parda' THEN 'Parda'
           WHEN repeat_raca_cor_etnia_tratada = 'branca' THEN 'Branca'
           WHEN repeat_raca_cor_etnia_tratada = 'preta' THEN 'Preta'
           WHEN repeat_raca_cor_etnia_tratada = 'amarela' THEN 'Amarela'
           WHEN repeat_raca_cor_etnia_tratada = 'indígena' THEN 'Indígena'
           WHEN repeat_raca_cor_etnia_tratada = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
           ELSE repeat_raca_cor_etnia_tratada
           END AS repeat_raca_cor_etnia_tratada,
          CASE
           WHEN repeat_sexo = 'masculino' THEN 'Masculino'
           WHEN repeat_sexo = 'feminino' THEN 'Feminino'
           WHEN repeat_sexo = 'intersexo' THEN 'Intersexo'
           WHEN repeat_sexo = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
           ELSE repeat_sexo
           END AS repeat_sexo,
          CASE
          WHEN RIGHT(mes_abrev_data_abordagem, 1) = '.' THEN mes_abrev_data_abordagem
          ELSE CONCAT(mes_abrev_data_abordagem, '.')
          END AS mes_abrev_data_abordagem,
          CASE
           WHEN note_creas_tratada = 'CREAS Janete Clair' THEN 'Creas Janete Clair'
           WHEN note_creas_tratada = 'CREAS Maria Lina de Castro Lima' THEN 'Creas Maria Lina De Castro Lima'
           WHEN note_creas_tratada = 'CREAS Daniela Perez' THEN 'Creas Daniela Perez'
           WHEN note_creas_tratada = 'CREAS Stella Maris' THEN 'Creas Stella Maris'
           WHEN note_creas_tratada = 'CREAS Nélson Carneiro' THEN 'Creas Nelson Carneiro'
           WHEN note_creas_tratada = 'CREAS Padre Guilherme Decaminada' THEN 'Creas Padre Guilherme Decaminada'
           WHEN note_creas_tratada = 'CREAS Professora Márcia Lopes' THEN 'Creas Professora Márcia Lopes'
           WHEN note_creas_tratada = 'CREAS Professora Aldaíza Sposati' THEN 'Creas Professora Aldaíza Sposati'
           WHEN note_creas_tratada = 'CREAS João Hélio Fernandes Vieites' THEN 'Creas João Hélio Fernandes Vieites'
           WHEN note_creas_tratada = 'CREAS Wanda Engel Aduan' THEN 'Creas Wanda Engel Aduan'
           WHEN note_creas_tratada = 'CREAS Zilda Arns Neumann' THEN 'Creas Zilda Arns Neumann'
           WHEN note_creas_tratada = 'CREAS Nelson Carneiro' THEN 'Creas Nelson Carneiro'
           WHEN note_creas_tratada = 'CREAS João Manoel Monteiro' THEN 'Creas João Manoel Monteiro'
           WHEN note_creas_tratada = 'CREAS Simone de Beauvoir' THEN 'Creas Simone de Beauvoir'
           WHEN note_creas_tratada = 'CREAS Arlindo Rodrigues' THEN 'Creas Arlindo Rodrigues'
           ELSE note_creas_tratada
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
           WHEN 'demanda_espontanea' THEN 'Demanda espontânea'
           ELSE resp_abordagem1
           END AS resp_abordagem1,
          CASE
           WHEN permanencia_rua_tratada = 'h24' THEN '24 horas'
           WHEN permanencia_rua_tratada = 'apenas_durante_dia' THEN 'Apenas durante o dia'
           WHEN permanencia_rua_tratada = 'durante_semana_retorna_casa_final_de_semana' THEN 'Durante a semana'
           WHEN permanencia_rua_tratada = 'frequenta_cenas_de_uso_esporadicamente' THEN 'Frequenta cenas de uso'
           WHEN permanencia_rua_tratada = 'apenas_durante_noite' THEN 'Apenas durante a noite'
           WHEN permanencia_rua_tratada = 'nao_sabe_nao_respondeu' THEN 'NS/NR'
           ELSE permanencia_rua_tratada
           END AS permanencia_rua_tratada,
          CASE
           WHEN tempo_permanencia_tratada = 'de_1_a_3_anos' THEN 'De 1 a 3 anos'
           WHEN tempo_permanencia_tratada = 'de_3_a_6_anos' THEN 'De 3 a 6 anos'
           WHEN tempo_permanencia_tratada = 'de_1_a_3_meses' THEN 'De 1 a 3 meses'
           WHEN tempo_permanencia_tratada = 'menos_3_dias' THEN 'Menos de 3 dias'
           WHEN tempo_permanencia_tratada = 'de_6_meses_a_1_ano' THEN 'De 6 meses a 1 ano'
           WHEN tempo_permanencia_tratada = 'de_6_a_10_anos' THEN 'De 6 a 10 anos'
           WHEN tempo_permanencia_tratada = 'de_3_a_6_meses' THEN 'De 3 a 6 meses'
           WHEN tempo_permanencia_tratada = 'mais_de_10_anos' THEN 'Mais de 10 anos'
            WHEN tempo_permanencia_tratada = 'de_7_a_30_dias' THEN 'De 7 a 30 dias'
           WHEN tempo_permanencia_tratada = 'de_3_a_7_dias' THEN 'De 3 a 7 dias'
           WHEN tempo_permanencia_tratada = 'ns_nr' THEN 'NS/NR'
           ELSE tempo_permanencia_tratada
           END AS tempo_permanencia_tratada,
          CASE
           WHEN migrante_terra_natal_tratada = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
           WHEN migrante_terra_natal_tratada = 'sim' THEN 'Sim'
           WHEN migrante_terra_natal_tratada = 'nao' THEN 'Não'
           ELSE migrante_terra_natal_tratada
           END AS migrante_terra_natal_tratada,
          CASE
           WHEN possui_referencia_tratada = 'nao_sabe_nao_quis_responder' THEN 'NS/NR'
            WHEN possui_referencia_tratada = 'sim' THEN 'Sim'
            WHEN possui_referencia_tratada = 'nao' THEN 'Não'
           ELSE possui_referencia_tratada
           END AS possui_referencia_tratada,
          CASE
           WHEN escolaridade_tratada = 'medio_incompleto' THEN 'Médio incompleto'
           WHEN escolaridade_tratada = 'fundamental_incompleto' THEN 'Fundamental incompleto'
           WHEN escolaridade_tratada = 'medio_completo' THEN 'Médio completo'
           WHEN escolaridade_tratada = 'fundamental_completo' THEN 'Fundamental completo'
           WHEN escolaridade_tratada = 'nao_alfabetizado' THEN 'Não alfabetizado'
           WHEN escolaridade_tratada = 'nao_sabe_nao_respondeu' THEN 'Não sabe/Não respondeu'
           WHEN escolaridade_tratada = 'superior_completo' THEN 'Superior completo'
           WHEN escolaridade_tratada = 'superior_incompleto' THEN 'Superior incompleto'
           WHEN escolaridade_tratada = 'nao_escolarizado' THEN 'Não escolarizado'
           ELSE escolaridade_tratada
           END AS escolaridade_tratada,
          CASE
           WHEN ocupacao_tratada = 'catador' THEN 'Catador'
           WHEN ocupacao_tratada = 'pedinte' THEN 'Pedinte'
           WHEN ocupacao_tratada = 'ambulante' THEN 'Ambulante'
           WHEN ocupacao_tratada = 'bicos' THEN 'Bicos'
           WHEN ocupacao_tratada = 'outros' THEN 'Outros'
           WHEN ocupacao_tratada = 'impossibilitado_para_trabalho' THEN 'Impossibilitado'
           WHEN ocupacao_tratada = 'prostituicao' THEN 'Prostituição'
           WHEN ocupacao_tratada = 'ns_nr' THEN 'NS/NR' 
           ELSE ocupacao_tratada
           END AS ocupacao_tratada,
          CASE
           WHEN aceita_acolhimento_tratada IS NULL THEN 'nao'
           WHEN TRIM(LOWER(aceita_acolhimento_tratada)) = 'n/a' THEN 'nao'
           ELSE aceita_acolhimento_tratada
           END AS aceita_acolhimento_tratada,
          CASE
           WHEN unidade_destino_tratada = 'albergue' THEN 'Albergue'
           WHEN unidade_destino_tratada = 'central_recepcao' THEN 'Central de Recepção'
           WHEN unidade_destino_tratada = 'outros' THEN 'Outros'
           WHEN unidade_destino_tratada = 'com_terapeutica' THEN 'Comunidade Terapêutica'
           WHEN unidade_destino_tratada = 'urs' THEN 'URS'
                WHEN unidade_destino_tratada = 'inverno_acolhedor' THEN 'Inverno Acolhedor'
                WHEN unidade_destino_tratada = 'abrigo_frio' THEN 'Inverno Acolhedor'
           WHEN unidade_destino_tratada = 'estado' THEN 'Hotel do Estado'
           ELSE unidade_destino_tratada
           END AS unidade_destino_tratada,
            CASE
                WHEN equipamento_destino_tratada = 'adf_taquara' THEN 'ADF Taquara'
                WHEN equipamento_destino_tratada = 'albergue_alfonso_lavalle' THEN 'Albergue Alfonso Lavalle'
                WHEN equipamento_destino_tratada = 'albergue_betinho' THEN 'Albergue Betinho'
                WHEN equipamento_destino_tratada = 'albergue_dercy_gonçalves' THEN 'Albergue Dercy Gonçalves'
                WHEN equipamento_destino_tratada = 'albergue_mais_tempo_lgbtqia' THEN 'Albergue + Tempo LGBTI'
                WHEN equipamento_destino_tratada = 'albergue_martin_luther_kingjr' THEN 'Albergue Martin Luther King Jr'
                WHEN equipamento_destino_tratada = 'albergue_nise_da_silveira' THEN 'Albergue Nise da Silveira'
                WHEN equipamento_destino_tratada = 'albergue_realengo' THEN 'Albergue Realengo'
                WHEN equipamento_destino_tratada = 'assoc_de_assistencia_social_videira' THEN 'Associação de Assistência Social Videira'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_bangu' THEN 'Associação Maranathá Bangu'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_engenho_de_dentro' THEN 'Associação Maranathá Engenho de Dentro'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_lins_de_vasconcelos' THEN 'Associação Maranathá Lins de Vasconcelos'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_madureira' THEN 'Associação Maranathá Madureira'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_padre_miguel' THEN 'Associação Maranathá Padre Miguel'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_sepetiba' THEN 'Associação Maranathá Sepetiba'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_vila_kennedy' THEN 'Associação Maranathá Vila Kennedy'
                WHEN equipamento_destino_tratada = 'assoc_maranatha_rj_vila_valqueire' THEN 'Associação Maranathá Vila Valqueire'
                WHEN equipamento_destino_tratada = 'assoc_projeto_resg_vidas' THEN 'Associação Projeto Resgatando Vidas'
                WHEN equipamento_destino_tratada = 'base_abordagem_social' THEN 'Base da Abordagem Social'
                WHEN equipamento_destino_tratada = 'camor' THEN 'C.A.M.O.R'
                WHEN equipamento_destino_tratada = 'casa_apoio_fem_por_amor' THEN 'Casa de Apoio Por Amor'
                WHEN equipamento_destino_tratada = 'centr_amparo_c_amor' THEN 'C.A.M.O.R'
                WHEN equipamento_destino_tratada = 'centr_transformacao_manasses' THEN 'Centro de Transformação Manasses'
                WHEN equipamento_destino_tratada = 'comt_valentes_de_davi_escola_de_profetas' THEN 'COMT Valentes de Davi Escola de Profetas'
                WHEN equipamento_destino_tratada = 'craf_tom_jobim' THEN 'CRAF Tom Jobim'
                WHEN equipamento_destino_tratada = 'crca_ademar_ferreira_de_oliveira' THEN 'CRCA Ademar Ferreira de Oliveira'
                WHEN equipamento_destino_tratada = 'crca_taiguara' THEN 'CRCA Taiguara'
                WHEN equipamento_destino_tratada = 'cri_pastor_carlos_portela' THEN 'CRI Pastor Carlos Portela'
                WHEN equipamento_destino_tratada = 'igreja_cristo_rei' THEN 'Igreja Cristo Rei'
                WHEN equipamento_destino_tratada = 'inst_efrain' THEN 'Instituto EFRAIN'
                WHEN equipamento_destino_tratada = 'inst_revivendo_com_cristo' THEN 'Instituto Revivendo com Cristo'
                WHEN equipamento_destino_tratada = 'inst_social_manasses_campo_grande1' THEN 'Instituto Social Manasses Campo Grande 1'
                WHEN equipamento_destino_tratada = 'inst_social_manasses_campo_grande2' THEN 'Instituto Social Manasses Campo Grande 2'
                WHEN equipamento_destino_tratada = 'inst_social_marca_de_cristo' THEN 'Instituto Social Marca de Cristo'
                WHEN equipamento_destino_tratada = 'inst_social_marca_de_cristo_i' THEN 'Instituto Social Marca de Cristo I'
                WHEN equipamento_destino_tratada = 'inst_social_marca_de_cristo_ii' THEN 'Instituto Social Marca de Cristo II'
                WHEN equipamento_destino_tratada = 'norte_shopping' THEN 'Norte Shopping'
                WHEN equipamento_destino_tratada = 'projeto_alcançando_vidas' THEN 'Projeto Alcançando Vidas'
                WHEN equipamento_destino_tratada = 'rio_acolhedor_abr_frio' THEN 'Rio Acolhedor – Abrigo do Frio'
                WHEN equipamento_destino_tratada = 'urs_haroldo_costa' THEN 'URS Haroldo Costa'
                WHEN equipamento_destino_tratada = 'urs_ilha' THEN 'URS Ilha do Governador'
                WHEN equipamento_destino_tratada = 'urs_irma_dulce' THEN 'URS Irmã Dulce'
                WHEN equipamento_destino_tratada = 'urs_maria_teresa_vieira' THEN 'URS Maria Teresa Vieira'
                WHEN equipamento_destino_tratada = 'urs_maria_vieira_bazani' THEN 'URS Maria Vieira Bazani'
                WHEN equipamento_destino_tratada = 'urs_nilda_ney' THEN 'URS Nilda Ney'
                WHEN equipamento_destino_tratada = 'urs_realengo' THEN 'URS Realengo'
                WHEN equipamento_destino_tratada = 'urs_rio_acolhedor_paciencia' THEN 'URS Rio Acolhedor'
                ELSE equipamento_destino_tratada
            END AS equipamento_destino_tratada,
          CASE
           WHEN encam_creas_tratada = 'creas_maria_lina_de_castro_lima' THEN 'Creas Maria Lina De Castro Lima'
           WHEN encam_creas_tratada = 'creas_simone_de_beauvoir' THEN 'Creas Simone De Beauvoir'
           WHEN encam_creas_tratada = 'creas_arlindo_rodrigues' THEN 'Creas Arlindo Rodrigues'
           WHEN encam_creas_tratada = 'creas_janete_clair' THEN 'Creas Janete Clair'
           WHEN encam_creas_tratada = 'creas_professora_aldaiza_sposati' THEN 'Creas Professora Aldaiza Sposati'
           WHEN encam_creas_tratada = 'creas_professora_marcia_lopes' THEN 'Creas Professora Marcia Lopes'
           WHEN encam_creas_tratada = 'creas_stella_maris' THEN 'Creas Stella Maris'
           WHEN encam_creas_tratada = 'creas_nelson_carneiro' THEN 'Creas Nelson Carneiro'
           WHEN encam_creas_tratada = 'creas_wanda_engel_aduan' THEN 'Creas Wanda Engel Aduan'
           WHEN encam_creas_tratada = 'creas_joao_helio_fernandes_vieites' THEN 'Creas Joao Helio Fernandes Vieites'
           WHEN encam_creas_tratada = 'creas_daniela_perez' THEN 'Creas Daniela Perez'
           WHEN encam_creas_tratada = 'creas_padre_guilherme_decaminada' THEN 'Creas Padre Guilherme Decaminada'
           WHEN encam_creas_tratada = 'creas_zilda_arns_neumann' THEN 'Creas Zilda Arns Neumann'
           WHEN encam_creas_tratada = 'creas_joao_manuel_monteiro' THEN 'Creas Joao Manuel Monteiro'
           ELSE encam_creas_tratada
           END AS encam_creas_tratada,
          CASE
           WHEN encam_centropop_tratada = 'centro_pop_jose_saramago' THEN 'Centro Pop José Saramago'
           WHEN encam_centropop_tratada = 'centro_pop_barbara_calazans' THEN 'Centro Pop Barbara  Calazans'
           ELSE encam_centropop_tratada
           END AS encam_centropop_tratada,
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
                  ELSE encaminhamento_rede_tratada
                END
              FROM UNNEST(SPLIT(encaminhamento_rede_tratada, ',')) AS part
            ),
            ', '
          ) AS encaminhamento_rede_tratada
    ),
    c.key_cpf,
    c.key_nome_usuario,
    c.key_nome_mae,
    c.key_nome_pai,
    e.cas,
    e.e_mail,
    c.cluster_id,
    c.is_duplicado,
    f.objectid as ficha_objectid
from repeat as r
left join emails as e on r.repeat_unidade_cas = e.cas
left join dedup as c on r.globalid = c.globalid
left join ficha as f on r.parentrowid = cast(f.objectid as string)
