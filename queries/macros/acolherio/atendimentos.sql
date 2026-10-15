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

{% macro classificar_atendimento(profissional) %}
    case
        when {{ profissional }}.nome_atendimento_original like '%Recepção%'
            or trim(upper({{ profissional }}.nome_cadastral)) = 'ATENDIMENTO RECEPÇÃO' then 'Recepção'
        when {{ profissional }}.flag_cbo_tecnico then 'Atendimento Técnico'
        when {{ profissional }}.flag_cbo_recepcao
            and {{ profissional }}.nome_atendimento_original like '%CadÚnico%' then 'Recepção'
        else 'Outros Atendimentos'
    end
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
