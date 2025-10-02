-- Coluna NACIONALIDADE do models relatorio_geral
{% macro map_coluna_nacionalidade (coluna ) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Brasileira'
    WHEN {{ coluna }} = '2' THEN 'Brasileiro'
    WHEN {{ coluna }} = '3' THEN 'Estrangeira'
  END AS NACIONALIDADE
{% endmacro %}

-- Coluna COND_ESTRANGEIRO do models relatorio_geral
{% macro map_coluna_cond_estrangeiro (coluna ) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Apatrida'
    WHEN {{ coluna }} = '2' THEN 'Asilado'
    WHEN {{ coluna }} = '3' THEN 'Imigrante com autorização de residência'
    WHEN {{ coluna }} = '4' THEN 'Imigrante com visto temporário'
    WHEN {{ coluna }} = '5' THEN 'Refugiado (Não legalizado)'
    WHEN {{ coluna }} = '6' THEN 'Refugiado (Em solicitação)'
    WHEN {{ coluna }} = '7' THEN 'Refugiado Legalizado'
    WHEN {{ coluna }} = '8' THEN 'Turista'
  END AS COND_ESTRANGEIRO
{% endmacro %}

-- Coluna ETNIA do models relatorio_geral
{% macro map_coluna_etnia (coluna ) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Branca'
    WHEN {{ coluna }} = '02' THEN 'Preta'
    WHEN {{ coluna }} = '03' THEN 'Parda'
    WHEN {{ coluna }} = '04' THEN 'Amarelo'
    WHEN {{ coluna }} = '05' THEN 'Indígena'
  END AS ETNIA,
{% endmacro %}

-- Coluna GENERO do models relatorio_geral
{% macro map_coluna_genero (coluna ) %}
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
  END AS GENERO
{% endmacro %}

-- Coluna FILIACAO do models relatorio_geral
{% macro map_coluna_filiacao (coluna ) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Solteiro'
    WHEN {{ coluna }} = '2' THEN 'Casado'
    WHEN {{ coluna }} = '3' THEN 'Viúvo'
    WHEN {{ coluna }} = '4' THEN 'Separado Judicialmente'
    WHEN {{ coluna }} = '5' THEN 'União consensual'
  END AS FILIACAO
{% endmacro %}

-- Coluna ESCOLARIDADE do models relatorio_geral
{% macro map_coluna_escolaridade (coluna ) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Infantil - G1'
    WHEN {{ coluna }} = '2' THEN 'Infantil - G2'
    WHEN {{ coluna }} = '3' THEN 'Infantil - G3'
    WHEN {{ coluna }} = '4' THEN 'Infantil - G4'
    WHEN {{ coluna }} = '5' THEN 'Infantil - G5'
    WHEN {{ coluna }} = '6' THEN 'Fundamental I - 1º Ano'
    WHEN {{ coluna }} = '7' THEN 'Fundamental I - 2º Ano'
    WHEN {{ coluna }} = '8' THEN 'Fundamental I - 3º Ano'
    WHEN {{ coluna }} = '9' THEN 'Fundamental I - 4º Ano'
    WHEN {{ coluna }} = '10' THEN 'Fundamental I - 5º Ano'
    WHEN {{ coluna }} = '11' THEN 'Fundamental II - 6º Ano'
    WHEN {{ coluna }} = '12' THEN 'Fundamental II - 7º Ano'
    WHEN {{ coluna }} = '13' THEN 'Fundamental II - 8º Ano'
    WHEN {{ coluna }} = '14' THEN 'Fundamental II - 9º Ano'
    WHEN {{ coluna }} = '15' THEN 'Ensino Médio - 1º Ano'
    WHEN {{ coluna }} = '16' THEN 'Ensino Médio - 2º Ano'
    WHEN {{ coluna }} = '17' THEN 'Ensino Médio - 3º Ano'
    WHEN {{ coluna }} = '18' THEN 'Ensino Superior'
  END AS ESCOLARIDADE,
{% endmacro %}

-- Coluna TIPO_CURATELA do models relatorio_geral
{% macro map_coluna_tipo_curatela (coluna ) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Família extensa'
    WHEN {{ coluna }} = '2' THEN 'Público'
    WHEN {{ coluna }} = '3' THEN 'Conhecido/amigo'
    WHEN {{ coluna }} = '4' THEN 'Conselho de contabilidade'
  END AS TIPO_CURATELA
{% endmacro %}
