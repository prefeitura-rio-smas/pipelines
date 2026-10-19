-- Macros de social: nomes e regras preservados.

{% macro map_indicador_social(coluna) %}
    case upper(trim({{ coluna }}))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        else 'Não Informado'
    end
{% endmacro %}

{% macro descricoes_sociais(itens) %}
    (select string_agg(distinct item.descricao, ', ' order by item.descricao)
     from unnest({{ itens }}) as item)
{% endmacro %}

{% macro normalizar_itens_sociais(itens, nao_mapeado) %}
    array(
        select distinct as struct
            item.codigo,
            coalesce(item.descricao, format('{{ nao_mapeado }}', item.codigo)) as descricao
        from unnest({{ itens }}) as item
        where upper(trim(item.codigo)) not in ('', 'N')
        order by codigo
    )
{% endmacro %}

{% macro map_nome_projeto_social(coluna) %}
    case {{ coluna }}
        when 1 then 'Territórios Sociais'
        when 2 then 'Pequenos Cariocas'
        when 3 then 'MCMV - Minha Casa Minha Vida'
        when 4 then 'Alimenta Rio'
        when 5 then 'Banco de Alimentos/Cozinha Comunitária'
        when 6 then 'Banco Carioca de Bolsa de Estudos'
        when 7 then 'Projetos de Empreendedorismo'
        when 8 then 'PRONASCI Juventude'
        when 9 then 'Agente Experiente'
        when 10 then 'Amparando Filhos'
        when 11 then 'Bora pra Escola'
        when 12 then 'DAM+'
        when 13 then 'De Mãos Dadas'
        when 14 then 'De Volta a Terra Natal'
        when 15 then 'Escritório Social'
        when 16 then 'Garupa'
        when 17 then 'Guarda Subsidiada'
        when 18 then 'Jovem Aprendiz'
        when 19 then 'Lares Cariocas'
        when 20 then 'Moradia com Apoio'
        when 21 then 'Novos Rumos'
        when 22 then 'Passo a Passo'
        when 23 then 'Penas e Medidas Alternativas'
        when 24 then 'PETI - Errad. Trab. Infantil'
        when 25 then 'Projetos de Qualificação Profissional'
        when 26 then 'Reinserção familiar ou comunitária'
        when 27 then 'SUAS Acolhendo Talentos'
        when 28 then 'Vem com a gente'
        when 29 then 'Rio Dignidade'
        when 30 then 'Idoso em Família'
        when 31 then 'Projovem Urbano'
        when 32 then 'Tô de Boa'
        else format('Projeto não mapeado (ID %d)', {{ coluna }})
    end
{% endmacro %}

{% macro map_coluna_tipo_curatela (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Família extensa'
    WHEN {{ coluna }} = '2' THEN 'Público'
    WHEN {{ coluna }} = '3' THEN 'Conhecido/amigo'
    WHEN {{ coluna }} = '4' THEN 'Conselho de contabilidade'
  END
{% endmacro %}

{% macro map_coluna_decisao_apoiada (coluna) %}
  -- Indica preenchimento cadastral do campo do processo.
  CASE
    WHEN nullif(trim(cast({{ coluna }} as string)), '') IS NULL THEN 'N'
    ELSE 'S'
  END
{% endmacro %}

{% macro map_coluna_situacao_de_rua (coluna) %}
  CASE
      WHEN {{ coluna }} = '5' THEN 'S'
      ELSE 'N'
  END
{% endmacro %}

{% macro map_coluna_saude_mental(coluna ) %}
  CASE
      WHEN {{ coluna }} = 'A' THEN 'Pessoa com aparente agravo de saúde mental'
      WHEN {{ coluna }} = 'D' THEN 'Pessoa com diagnóstico (laudo médico) de doença mental'
      ELSE {{ coluna }}
  END
{% endmacro %}

{% macro map_coluna_tipo_deficiencia (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Deficiência auditiva total'
    WHEN {{ coluna }} = '2' THEN 'Deficiência visual parcial'
    WHEN {{ coluna }} = '3' THEN 'Deficiência visual total'
    WHEN {{ coluna }} = '4' THEN 'Deficiência motora'
    WHEN {{ coluna }} = '5' THEN 'Deficiência mental ou intelectual'
    WHEN {{ coluna }} = '6' THEN 'Deficiência auditiva parcial'
  END
{% endmacro %}

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

{% macro map_coluna_beneficio(coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Aposentadoria'
    WHEN {{ coluna }} = '2' THEN 'Auxilio Doença'
    WHEN {{ coluna }} = '3' THEN 'Bolsa BAR [Bolsa de apoio à ressocialização]'
    WHEN {{ coluna }} = '4' THEN 'Bolsa de volta para casa [PVC nacional]'
    WHEN {{ coluna }} = '5' THEN 'Bolsa Família'
    WHEN {{ coluna }} = '6' THEN 'Bolsa-Rio Tipo I'
    WHEN {{ coluna }} = '7' THEN 'Bolsa-Rio Tipo II'
    WHEN {{ coluna }} = '8' THEN 'Cartão Família Carioca [CFC]'
    WHEN {{ coluna }} = '9' THEN 'BPC-Benefício de Prestação Continuada'
    WHEN {{ coluna }} = '10' THEN 'Pensão'
    WHEN {{ coluna }} = '11' THEN 'Auxilio Emergêncial'
    WHEN {{ coluna }} = '12' THEN 'Auxilio Brasil'
    WHEN {{ coluna }} = '13' THEN 'Assalariado'
    WHEN {{ coluna }} = '14' THEN 'Agente Experiente'
    WHEN {{ coluna }} = '15' THEN 'Aluguel Social'
    WHEN {{ coluna }} = '16' THEN 'Apoio Moradia'
    WHEN {{ coluna }} = '17' THEN 'Benefício comprometido em empréstimo'
    WHEN {{ coluna }} = '18' THEN 'Benefícios eventuais'
    WHEN {{ coluna }} = '19' THEN 'Pensão Alimentícia'
    WHEN {{ coluna }} = '20' THEN 'Pensão por morte'
    WHEN {{ coluna }} = '21' THEN 'Outro Benefício'
    WHEN {{ coluna }} = '22' THEN 'Outro tipo de Pensão'
    WHEN {{ coluna }} = '23' THEN 'Não sabe / Não lembra'
    WHEN {{ coluna }} = '24' THEN 'Não respondeu'
    WHEN {{ coluna }} = '25' THEN 'Bolsa de inclusão produtiva [Seguir em Frente]'
    WHEN {{ coluna }} = '26' THEN 'Auxílio moradia temporário'
    WHEN {{ coluna }} = '27' THEN 'Idoso em família'
    WHEN {{ coluna }} = '28' THEN 'Riocard especial'
    WHEN {{ coluna }} = '29' THEN 'Riocard estudante'
    WHEN {{ coluna }} = '30' THEN 'Riocard senior'
    WHEN {{ coluna }} = '31' THEN 'Acidente de trabalho'
    WHEN {{ coluna }} = '32' THEN 'Aposentadoria por invalidez'
    WHEN {{ coluna }} = '33' THEN 'Trabalho informal'
    WHEN {{ coluna }} = '34' THEN 'Bolsa ONG / OSS'
    WHEN {{ coluna }} = '35' THEN 'Militar da Reserva'
    WHEN {{ coluna }} = '36' THEN 'Bolsa Estudantil'
    WHEN {{ coluna }} = '37' THEN 'Autônomo'
    WHEN {{ coluna }} = '38' THEN 'Jovem Aprendiz'
    WHEN {{ coluna }} = '39' THEN 'Cartão da Primeira Infância Carioca'
    WHEN {{ coluna }} = '40' THEN 'Auxílio reclusão'
    WHEN {{ coluna }} = '41' THEN 'Auxílio natalidade'
    WHEN {{ coluna }} = '42' THEN 'Auxílio funeral'
    WHEN {{ coluna }} = '43' THEN 'Seguro desemprego'
  END
{% endmacro %}

{% macro map_coluna_vinculo_trabalhista (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Formal'
    WHEN {{ coluna }} = '2' THEN 'Informal'
    WHEN {{ coluna }} = '3' THEN 'Não sabe / Não lembra'
    WHEN {{ coluna }} = '4' THEN 'Não quis dizer'
  END
{% endmacro %}

{% macro map_grau_dependencia (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN '1-Idoso independente'
    WHEN {{ coluna }} = '2' THEN '2-Idoso com dependencia em ate 3 atividades'
    WHEN {{ coluna }} = '3' THEN '3-Idoso com dependencia em todas as atividades'
  END
{% endmacro %}

{% macro map_flag_trabalho (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}

{% macro map_flag_deficiencia (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}

{% macro map_flag_curatela (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}

{% macro map_flag_situacao_rua (coluna) %}
  case
    when safe_cast(trim(cast({{ coluna }} as string)) as int64) = 5 then 'Sim'
    -- Domínio indmoradi (moradia.ini): demais moradias conhecidas.
    when safe_cast(trim(cast({{ coluna }} as string)) as int64) in (1, 2, 3, 4, 6, 7, 8, 9) then 'Não'
    when upper(trim(cast({{ coluna }} as string))) in ('A', 'B', 'C', 'D') then 'Não'
    -- E = Desconhecido; ausência e códigos fora do domínio permanecem desconhecidos.
    else null
  end
{% endmacro %}

{% macro map_flag_cadunico (coluna) %}
  case
    when upper(trim(cast({{ coluna }} as string))) = 'S' then 'Sim'
    when upper(trim(cast({{ coluna }} as string))) = 'N' then 'Não'
    else null
  end
{% endmacro %}
