-- Coluna NACIONALIDADE do models relatorio_geral
{% macro map_coluna_nacionalidade (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Brasileira'
    WHEN {{ coluna }} = '2' THEN 'Brasileiro'
    WHEN {{ coluna }} = '3' THEN 'Estrangeira'
  END 
{% endmacro %}

-- Coluna COND_ESTRANGEIRO do models relatorio_geral
{% macro map_coluna_cond_estrangeiro (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Apatrida'
    WHEN {{ coluna }} = '2' THEN 'Asilado'
    WHEN {{ coluna }} = '3' THEN 'Imigrante com autorização de residência'
    WHEN {{ coluna }} = '4' THEN 'Imigrante com visto temporário'
    WHEN {{ coluna }} = '5' THEN 'Refugiado (Não legalizado)'
    WHEN {{ coluna }} = '6' THEN 'Refugiado (Em solicitação)'
    WHEN {{ coluna }} = '7' THEN 'Refugiado Legalizado'
    WHEN {{ coluna }} = '8' THEN 'Turista'
  END 
{% endmacro %}

-- Coluna RAÇA/COR do models relatorio_geral
{% macro map_coluna_raca (coluna) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Branca'
    WHEN {{ coluna }} = '02' THEN 'Preta'
    WHEN {{ coluna }} = '03' THEN 'Parda'
    WHEN {{ coluna }} = '04' THEN 'Amarelo'
    WHEN {{ coluna }} = '05' THEN 'Indígena'
  END 
{% endmacro %}

-- Coluna GENERO do models relatorio_geral
{% macro map_coluna_genero (coluna) %}
  CASE
    WHEN {{ coluna }} = '02' THEN 'Travesti'
    WHEN {{ coluna }} = '03' THEN 'Homem transgênero'
    WHEN {{ coluna }} = '04' THEN 'Intersexo'
    WHEN {{ coluna }} = '05' THEN 'Mulher cisgênero'
    WHEN {{ coluna }} = '06' THEN 'Homem cisgênero'
    WHEN {{ coluna }} = '07' THEN 'Não binário'
    WHEN {{ coluna }} = '98' THEN 'Outro'
    WHEN {{ coluna }} = '01' THEN 'Mulher transgênero'
    WHEN {{ coluna }} = '99' THEN 'Sem Informação' 
  END
{% endmacro %}

-- Coluna FILIACAO do models relatorio_geral
{% macro map_coluna_filiacao (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Solteiro'
    WHEN {{ coluna }} = '2' THEN 'Casado'
    WHEN {{ coluna }} = '3' THEN 'Viúvo'
    WHEN {{ coluna }} = '4' THEN 'Separado Judicialmente'
    WHEN {{ coluna }} = '5' THEN 'União consensual'
  END AS FILIACAO
{% endmacro %}

-- Coluna serie_escola do models relatorio_geral
{% macro map_coluna_serie_escola (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Educação Infantil'
    WHEN {{ coluna }} = '2' THEN 'Fundamental I - 1° Ano'
    WHEN {{ coluna }} = '3' THEN 'Fundamental I - 2° Ano'
    WHEN {{ coluna }} = '5' THEN 'Fundamental I - 3° Ano'
    WHEN {{ coluna }} = '6' THEN 'Fundamental I - 4° Ano'
    WHEN {{ coluna }} = '7' THEN 'Fundamental I - 5° Ano'
    WHEN {{ coluna }} = '8' THEN 'Fundamental II - 6° Ano'
    WHEN {{ coluna }} = '9' THEN 'Fundamental II - 7° Ano'
    WHEN {{ coluna }} = '10' THEN 'Fundamental II - 8° Ano'
    WHEN {{ coluna }} = '11' THEN 'Fundamental II - 9° Ano'
    WHEN {{ coluna }} = '12' THEN 'Ensino Médio - 1° Ano'
    WHEN {{ coluna }} = '13' THEN 'Ensino Médio - 2° Ano'
    WHEN {{ coluna }} = '14' THEN 'Ensino Médio - 3° Ano'
    WHEN {{ coluna }} = '15' THEN 'Ensino Superior'
    WHEN {{ coluna }} = '16' THEN 'PEJA I e II'
    WHEN {{ coluna }} = '17' THEN 'PEJA III e IV'
    WHEN {{ coluna }} = '18' THEN 'PEJA V'
  END 
{% endmacro %}

-- Coluna ESCOLARIDADE do models relatorio_geral
{% macro map_coluna_escolaridade (coluna) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Não Alfabetizado'
    WHEN {{ coluna }} = '02' THEN 'Alfabetizado'
    WHEN {{ coluna }} = '03' THEN 'Nível fundamental incompleto'
    WHEN {{ coluna }} = '04' THEN 'Nível fundamental completo'
    WHEN {{ coluna }} = '05' THEN 'Nível médio incompleto'
    WHEN {{ coluna }} = '06' THEN 'Nível médio completo'
    WHEN {{ coluna }} = '07' THEN 'Superior incompleto'
    WHEN {{ coluna }} = '08' THEN 'Superior completo'
    WHEN {{ coluna }} = '12' THEN 'Nunca estudou'
    WHEN {{ coluna }} = '13' THEN 'Nível fundamental incompleto (cursando)'
    WHEN {{ coluna }} = '14' THEN 'Nível médio incompleto (cursando)'
    WHEN {{ coluna }} = '15' THEN 'Superior incompleto (cursando)'
    WHEN {{ coluna }} = '16' THEN 'Não sabe/Não informou'
  END 
{% endmacro %}

-- Coluna TIPO_CURATELA do models relatorio_geral
{% macro map_coluna_tipo_curatela (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Família extensa'
    WHEN {{ coluna }} = '2' THEN 'Público'
    WHEN {{ coluna }} = '3' THEN 'Conhecido/amigo'
    WHEN {{ coluna }} = '4' THEN 'Conselho de contabilidade'
  END AS TIPO_CURATELA
{% endmacro %}

-- Coluna DECISAO_APOIADA do models relatorio_geral
{% macro map_coluna_decisao_apoiada (coluna) %}
  CASE
    WHEN {{ coluna }} IS NULL THEN 'N'
  END AS DECISAO_APOIADA
{% endmacro %}

-- Coluna SITUACAO_DE_RUA do models relatorio_geral
{% macro map_coluna_situacao_de_rua (coluna) %}
  CASE 
      WHEN {{ coluna }} = '5' THEN 'S'
      ELSE 'N'  
  END 
{% endmacro %}

-- Coluna SAUDE_MENTAL do models relatorio_geral
{% macro map_coluna_saude_mental(coluna ) %}
  CASE 
      WHEN {{ coluna }} = 'A' THEN 'Pessoa com aparente agravo de saúde mental'
      WHEN {{ coluna }} = 'D' THEN 'Pessoa com diagnóstico (laudo médico) de doença mental'
      ELSE {{ coluna }}  
  END 
{% endmacro %}

-- Coluna MOTIV_ACOLHIMENTO do models relatorio_geral
{% macro map_coluna_tipo_motiv_acolhimento (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Negligência'
    WHEN {{ coluna }} = '2' THEN 'Abandono'
    WHEN {{ coluna }} = '3' THEN 'Maus tratos'
    WHEN {{ coluna }} = '4' THEN 'Conflito familiar'
    WHEN {{ coluna }} = '5' THEN 'Uso abusivo de drogas'
    WHEN {{ coluna }} = '6' THEN 'Perdido da família'
    WHEN {{ coluna }} = '7' THEN 'Violência Sexual'
    WHEN {{ coluna }} = '8' THEN 'Violência Psicológica'
  END 
{% endmacro %}

-- Coluna TIPO_DEFICIENCIA do models tipo_deficiencia_unnest
{% macro map_coluna_tipo_deficiencia (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Deficiência auditiva total'
    WHEN {{ coluna }} = '2' THEN 'Deficiência visual parcial'
    WHEN {{ coluna }} = '3' THEN 'Deficiência visual total'
    WHEN {{ coluna }} = '4' THEN 'Deficiência motora'
    WHEN {{ coluna }} = '5' THEN 'Deficiência mental ou intelectual'
    WHEN {{ coluna }} = '6' THEN 'Deficiência auditiva parcial'
  END AS TIPO_DEFICIENCIA
{% endmacro %}

-- Coluna VIOLACAO_DIREITO do models violacao_direito
{% macro map_coluna_violacao_de_direito (coluna) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Violência Sexual'
    WHEN {{ coluna }} = '02' THEN 'Violência Psicológica'
    WHEN {{ coluna }} = '03' THEN 'Violência financeira ou patrimonial'
    WHEN {{ coluna }} = '04' THEN 'Negligência e abandono'
    WHEN {{ coluna }} = '05' THEN 'Violência medicamentosa'
    WHEN {{ coluna }} = '06' THEN 'Violência doméstica'
    WHEN {{ coluna }} = '07' THEN 'Violência por tráfico de seres humanos'
    WHEN {{ coluna }} = '08' THEN 'Violência por trabalho escravo'
    WHEN {{ coluna }} = '09' THEN 'Violência institucional'
    WHEN {{ coluna }} = '10' THEN 'Adoção Ilegal'
    WHEN {{ coluna }} = '11' THEN 'Afastamento do convívio familiar devido a aplicação de MSE ou medida de proteção'
    WHEN {{ coluna }} = '12' THEN 'Auto Negligência'
    WHEN {{ coluna }} = '13' THEN 'Cárcere Privado'
    WHEN {{ coluna }} = '14' THEN 'Conflitos Territoriais / Conflitos Urbanos'
    WHEN {{ coluna }} = '15' THEN 'Discriminação Étnica/Racial'
    WHEN {{ coluna }} = '16' THEN 'Discriminação Religiosa'
    WHEN {{ coluna }} = '17' THEN 'Discriminação Sexual / Gênero'
    WHEN {{ coluna }} = '18' THEN 'Outras violências sexuais'
    WHEN {{ coluna }} = '19' THEN 'Trabalho Infantil'
    WHEN {{ coluna }} = '20' THEN 'Deficiência visual parcial'
    WHEN {{ coluna }} = '21' THEN 'Deficiência visual total'
    WHEN {{ coluna }} = '22' THEN 'Deficiência motora'
    WHEN {{ coluna }} = '23' THEN 'Deficiência mental ou intelectual'
    WHEN {{ coluna }} = '24' THEN 'Deficiência auditiva parcial'
    WHEN {{ coluna }} = '25' THEN 'Deficiência mental ou intelectual'
    WHEN {{ coluna }} = '26' THEN 'Deficiência auditiva parcial'
  END
{% endmacro %}

-- Coluna BENEFICIO do models tipo_beneficio
{% macro map_coluna_beneficio(coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Aposentadoria'
    WHEN {{ coluna }} = '2' THEN 'Auxilio Doença'
    WHEN {{ coluna }} = '5' THEN 'Bolsa Família'
    WHEN {{ coluna }} = '8' THEN 'Cartão Família Carioca (CFC)'
    WHEN {{ coluna }} = '9' THEN 'BPC-Benefício de Prestação Continuada'
    WHEN {{ coluna }} = '11' THEN 'Auxilio Emergêncial'
    WHEN {{ coluna }} = '13' THEN 'Assalariado'
    WHEN {{ coluna }} = '14' THEN 'Agente Experiente'
    WHEN {{ coluna }} = '16' THEN 'Apoio Moradia'
    WHEN {{ coluna }} = '17' THEN 'Benefício comprometido em empréstimo'
    WHEN {{ coluna }} = '18' THEN 'Benefícios eventuais'
    WHEN {{ coluna }} = '19' THEN 'Pensão Alimentícia'
    WHEN {{ coluna }} = '20' THEN 'Pensão por morte'
    WHEN {{ coluna }} = '21' THEN 'Outro Benefício'
    WHEN {{ coluna }} = '22' THEN 'Outro tipo de Pensão'
    WHEN {{ coluna }} = '23' THEN 'Não sabe / Não lembra'
    WHEN {{ coluna }} = '24' THEN 'Não respondeu'
    WHEN {{ coluna }} = '26' THEN 'Auxílio moradia temporário'
    WHEN {{ coluna }} = '27' THEN 'Idoso em família'
    WHEN {{ coluna }} = '31' THEN 'Acidente de trabalho'
    WHEN {{ coluna }} = '32' THEN 'Aposentadoria por invalidez'
  END AS BENEFICIO
{% endmacro %}

-- Coluna ORIENTACAO_SEXUAL do models relatorio_geral
{% macro map_coluna_orientacao_sexual (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Lésbica'
    WHEN {{ coluna }} = '2' THEN 'Gay'
    WHEN {{ coluna }} = '3' THEN 'Bissexual'
    WHEN {{ coluna }} = '4' THEN 'Heterossexual'
    WHEN {{ coluna }} = '5' THEN 'Pansexual'
    WHEN {{ coluna }} = '6' THEN 'Não informado'
  END AS ORIENTACAO_SEXUAL
{% endmacro %}

-- Coluna MOTIVO_DESLIGAMENTO do models acolhimentos
{% macro map_coluna_motivo_desligamento (coluna) %}
  CASE
    WHEN {{ coluna }} = '2' THEN 'Deslig. por decisão da direção e/ou equipe téc. por conflito com prof. da unid. de acolhim.'
    WHEN {{ coluna }} = '3' THEN 'Deslig. por decisão da direção e/ou equipe téc. por não aceitação das regras da instituição'
    WHEN {{ coluna }} = '4' THEN 'Deslig. volunt. por conflito com outro acolhido'
    WHEN {{ coluna }} = '5' THEN 'Deslig. volunt. por conflito com prof. da unid. de acolhim.'
    WHEN {{ coluna }} = '7' THEN 'Deslig. volunt. por não aceitação das regras da instituição'
    WHEN {{ coluna }} = '8' THEN 'Deslig. volunt. por abstinência de substâncias psicoativas'
    WHEN {{ coluna }} = 'C' THEN 'Deslig. por decisão da direção e/ou equipe téc. por conflito com outro acolhido'
    WHEN {{ coluna }} = 'D' THEN 'Deslig. volunt. sem motivo identificado'
    WHEN {{ coluna }} = 'E' THEN 'Acolhido'
    WHEN {{ coluna }} = 'F' THEN 'Deslig. por reinserção comunitária'
    WHEN {{ coluna }} = 'G' THEN 'Deslig. por reinserção em família de origem ou família extensa'
    WHEN {{ coluna }} = 'H' THEN 'Deslig. por afastamento de cri/adol por medida protetiva de acolhimento'
    WHEN {{ coluna }} = 'I' THEN 'Solicitação de vaga'
    WHEN {{ coluna }} = 'J' THEN 'Reinserção através do projeto de volta a terra natal'
    WHEN {{ coluna }} = 'K' THEN 'Mudança para outro município'
    WHEN {{ coluna }} = 'L' THEN 'Transferência para clínica de apoio a saúde'
    WHEN {{ coluna }} = 'N' THEN 'Transferência para delegacia policial'
    WHEN {{ coluna }} = 'O' THEN 'Óbito'
    WHEN {{ coluna }} = 'T' THEN 'Deslig. por transferência para outra unid. de acolhim.'
    WHEN {{ coluna }} = 'X' THEN 'Fechado pela Unificação'
    WHEN {{ coluna }} = 'Y' THEN 'Demanda por serviço diurno'
    WHEN {{ coluna }} = 'Z' THEN 'Demanda por serviço noturno'

  END AS MOTIVO_DESLIGAMENTO
{% endmacro %}

-- Coluna PERFIL_ACESSO do models contas_associadas
{% macro map_coluna_perfil_acesso (coluna) %}
  CASE
    WHEN {{ coluna }} = 1 THEN 'Profissional de Nível Médio'
    WHEN {{ coluna }} = 2 THEN 'Profissional de Nível Superior '
    WHEN {{ coluna }} = 3 THEN 'Gestor local'
    WHEN {{ coluna }} = 4 THEN 'Relatório'
    WHEN {{ coluna }} = 5 THEN 'Gestor Local / Prof. Nível Superior'
    WHEN {{ coluna }} = 6 THEN 'Assessoria '
    WHEN {{ coluna }} = 8 THEN 'Acesso Personalizado (Acesso Cadastro)'
    WHEN {{ coluna }} = 98 THEN 'Gestor central '
    WHEN {{ coluna }} = 99 THEN 'MASTER'
  END AS perfil_acesso
{% endmacro %}

-- Coluna STATUS_CONTA do models contas_associadas
{% macro map_coluna_status_conta (coluna) %}
  CASE
    WHEN {{ coluna }} = '0' THEN 'Inativo'
    WHEN {{ coluna }} = '1' THEN 'Ativo'
    WHEN {{ coluna }} = '2' THEN 'Trocar  senha'
    WHEN {{ coluna }} = '3' THEN 'Bloqueado'
    WHEN {{ coluna }} = '4' THEN 'Atualizar cadastro'
  END AS status_conta
{% endmacro %}

-- Coluna UNIDADE_ATIVA do models lista_unidades
{% macro map_coluna_unidade_ativa (coluna) %}
  CASE
    WHEN {{ coluna }} = 'N' THEN 'S'
    WHEN {{ coluna }} = 'S' THEN 'N'
    ELSE 'S'
  END AS UNIDADE_ATIVA
{% endmacro %}

-- Coluna vinculo_trabalhista do model dashboard_acolherio/intermediate/int_dados_usuarios
{% macro map_coluna_vinculo_trabalhista (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Formal'
    WHEN {{ coluna }} = '2' THEN 'Informal'
    WHEN {{ coluna }} = '3' THEN 'Não sabe / Não lembra'
    WHEN {{ coluna }} = '4' THEN 'Não quis dizer'
  END 
{% endmacro %}


-- Macro para a coluna indgraudepend do model dashboard_acolherio/intermediate/int_dados_usuarios
{% macro map_grau_dependencia (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN '1-Idoso independente'
    WHEN {{ coluna }} = '2' THEN '2-Idoso com dependencia em ate 3 atividades'
    WHEN {{ coluna }} = '3' THEN '3-Idoso com dependencia em todas as atividades'
  END 
{% endmacro %}

-- Coluna estcivil do models dashboard_acolherio/intermediate/int_dados_usuarios
{% macro map_coluna_estado_civil (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Solteiro'
    WHEN {{ coluna }} = '2' THEN 'Casado'
    WHEN {{ coluna }} = '3' THEN 'Viúvo'
    WHEN {{ coluna }} = '4' THEN 'Separado Judicialmente'
    WHEN {{ coluna }} = '5' THEN 'União consensual'
  END 
{% endmacro %}