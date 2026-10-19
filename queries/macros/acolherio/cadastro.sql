-- Macros de cadastro: nomes e regras preservados.

{% macro map_coluna_nacionalidade (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Brasileira'
    WHEN {{ coluna }} = '2' THEN 'Brasileira'
    WHEN {{ coluna }} = '3' THEN 'Estrangeira'
  END
{% endmacro %}

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

{% macro map_coluna_raca (coluna) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Branca'
    WHEN {{ coluna }} = '02' THEN 'Preta'
    WHEN {{ coluna }} = '03' THEN 'Parda'
    WHEN {{ coluna }} = '04' THEN 'Amarelo'
    WHEN {{ coluna }} = '05' THEN 'Indígena'
  END
{% endmacro %}

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

{% macro map_coluna_filiacao (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Solteiro'
    WHEN {{ coluna }} = '2' THEN 'Casado'
    WHEN {{ coluna }} = '3' THEN 'Viúvo'
    WHEN {{ coluna }} = '4' THEN 'Separado Judicialmente'
    WHEN {{ coluna }} = '5' THEN 'União consensual'
  END
{% endmacro %}

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
    -- Catálogo indescolari: escolaridade.ini e escolaridade_bc26.ini.
    WHEN {{ coluna }} = '09' THEN 'Especialização'
    WHEN {{ coluna }} = '10' THEN 'Mestrado'
    WHEN {{ coluna }} = '11' THEN 'Doutorado'
    WHEN {{ coluna }} = '12' THEN 'Nunca estudou'
    WHEN {{ coluna }} = '13' THEN 'Nível fundamental incompleto (cursando)'
    WHEN {{ coluna }} = '14' THEN 'Nível médio incompleto (cursando)'
    WHEN {{ coluna }} = '15' THEN 'Superior incompleto (cursando)'
    WHEN {{ coluna }} = '16' THEN 'Não sabe/Não informou'
  END
{% endmacro %}

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

{% macro map_coluna_orientacao_sexual (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Lésbica'
    WHEN {{ coluna }} = '2' THEN 'Gay'
    WHEN {{ coluna }} = '3' THEN 'Bissexual'
    WHEN {{ coluna }} = '4' THEN 'Heterossexual'
    WHEN {{ coluna }} = '5' THEN 'Pansexual'
    WHEN {{ coluna }} = '6' THEN 'Não informado'
  END
{% endmacro %}

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

  END
{% endmacro %}

{% macro map_coluna_perfil_acesso (coluna) %}
  CASE
    WHEN {{ coluna }} = 1 THEN 'Profissional de Nível Médio'
    WHEN {{ coluna }} = 2 THEN 'Profissional de Nível Superior'
    WHEN {{ coluna }} = 3 THEN 'Gestor local'
    WHEN {{ coluna }} = 4 THEN 'Relatório'
    WHEN {{ coluna }} = 5 THEN 'Gestor Local / Prof. Nível Superior'
    WHEN {{ coluna }} = 6 THEN 'Assessoria'
    WHEN {{ coluna }} = 7 THEN 'Relatorio'
    WHEN {{ coluna }} = 8 THEN 'Acesso Personalizado'
    WHEN {{ coluna }} = 98 THEN 'Gestor central'
    WHEN {{ coluna }} = 99 THEN 'MASTER'
    ELSE 'Nível ' || CAST({{ coluna }} AS STRING)
  END
{% endmacro %}

{% macro map_coluna_status_conta (coluna) %}
  CASE
    WHEN {{ coluna }} = '0' THEN 'Inativo'
    WHEN {{ coluna }} = '1' THEN 'Ativo'
    WHEN {{ coluna }} = '2' THEN 'Trocar senha'
    WHEN {{ coluna }} = '3' THEN 'Bloqueado'
    WHEN {{ coluna }} = '4' THEN 'Atualizar cadastro'
    WHEN {{ coluna }} = '5' THEN 'Aceitar Termo de Uso'
  END AS status_conta
{% endmacro %}

{% macro map_coluna_unidade_ativa (coluna) %}
  CASE
    WHEN {{ coluna }} = 'N' THEN 'S'
    WHEN {{ coluna }} = 'S' THEN 'N'
    ELSE 'S'
  END
{% endmacro %}

{% macro map_coluna_indeixo_adulto (coluna) %}
  max(
    case
      when lower({{ coluna }}) = 'a' then 'Sim'
      else 'Não'
    end
  )
{% endmacro %}

{% macro map_coluna_indeixo_familia (coluna) %}
  max(
    case
      when lower({{ coluna }}) = 'f' then 'Sim'
      else 'Não'
    end
  )
{% endmacro %}

{% macro map_coluna_indeixo_idoso (coluna) %}
  max(
    case
      when lower({{ coluna }}) = 'i' then 'Sim'
      else 'Não'
    end
  )
{% endmacro %}

{% macro map_coluna_estado_civil (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Solteiro'
    WHEN {{ coluna }} = '2' THEN 'Casado'
    WHEN {{ coluna }} = '3' THEN 'Viúvo'
    WHEN {{ coluna }} = '4' THEN 'Separado Judicialmente'
    WHEN {{ coluna }} = '5' THEN 'União consensual'
  END
{% endmacro %}

{% macro map_flag_frequencia_escola (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}

{% macro map_flag_boolean (coluna) %}
  case
    when {{ coluna }} then 'Sim'
    when not {{ coluna }} then 'Não'
    else 'Não Informado'
  end
{% endmacro %}
