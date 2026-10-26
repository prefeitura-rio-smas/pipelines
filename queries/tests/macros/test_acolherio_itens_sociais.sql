-- Teste singular das macros de itens sociais (fixtures literais em CTE; sem tabela).
-- Cada linha de "checks" é uma asserção: obtido deve ser igual a esperado.
-- O teste retorna apenas as asserções que falharam; 0 linhas = PASS.

with fixtures as (
    select
        cast(null as array<struct<codigo string, descricao string>>) as entrada_nula,
        array<struct<codigo string, descricao string>>[] as entrada_vazia,
        [
            struct('' as codigo, 'vazio' as descricao),
            struct('N' as codigo, 'N maiúsculo' as descricao),
            struct('n' as codigo, 'n minúsculo' as descricao),
            struct(' N ' as codigo, 'N com espaços' as descricao)
        ] as descartaveis,
        [struct('01' as codigo, 'Benefício A' as descricao)] as zero_a_esquerda,
        [
            struct('02' as codigo, 'B' as descricao),
            struct('01' as codigo, 'A' as descricao),
            struct('02' as codigo, 'B' as descricao)
        ] as com_duplicata_e_desordem,
        [
            struct('99' as codigo, cast(null as string) as descricao),
            struct('7' as codigo, cast(null as string) as descricao)
        ] as sem_descricao,
        [
            struct('chave_2' as chave, 'Rótulo 2' as rotulo),
            struct('chave_1' as chave, 'Rótulo 1' as rotulo),
            struct('chave_3' as chave, 'Rótulo 1' as rotulo)
        ] as estrutura_alternativa,
        [struct('99' as codigo, cast(null as string) as descricao)] as item_sem_descricao,
        [struct('A' as descricao), struct('B' as descricao)] as duas_descricoes
),

resultados as (
    select
        {{ acolherio_normalizar_itens('entrada_nula') }} as nula,
        {{ acolherio_normalizar_itens('entrada_vazia') }} as vazia,
        {{ acolherio_normalizar_itens('descartaveis') }} as descartaveis,
        {{ acolherio_normalizar_itens('zero_a_esquerda') }} as zero_a_esquerda,
        {{ acolherio_normalizar_itens('com_duplicata_e_desordem') }} as com_duplicata_e_desordem,
        {{ acolherio_normalizar_itens('sem_descricao') }} as sem_descricao,
        {{ acolherio_normalizar_itens(
            'estrutura_alternativa',
            campo_codigo='chave',
            campo_descricao='rotulo'
        ) }} as alternativa,
        {{ acolherio_descricoes_itens('estrutura_alternativa', campo='rotulo', separador=' / ') }} as descricoes_campo_custom,
        {{ acolherio_normalizar_itens(
            'item_sem_descricao', "Item d'origem \\pasta\n\r\t\b\f\a\v%s 😀"
        ) }} as formato_com_escape,
        {{ acolherio_descricoes_itens(
            'duas_descricoes', separador="'\\pasta\n\r\t\b\f\a\v😀"
        ) }} as separador_com_escape,
        {{ acolherio_normalizar_itens(
            'item_sem_descricao', 'Item %%s: %s (%%d; 100%%)'
        ) }} as formato_percentual
    from fixtures
),

descricoes as (
    select
        resultados.*,
        {{ acolherio_descricoes_itens('sem_descricao') }} as descricoes_padrao,
        {{ acolherio_descricoes_itens('alternativa', campo='descricao', separador=' | ') }} as descricoes_alternativa
    from resultados
),

checks as (
    select
        'nulo_vira_array_vazio' as caso,
        cast(array_length(nula) as string) as obtido,
        '0' as esperado
    from descricoes
    union all
    select
        'vazio_vira_array_vazio',
        cast(array_length(vazia) as string),
        '0'
    from descricoes
    union all
    select
        'descarta_vazio_e_N',
        cast(array_length(descartaveis) as string),
        '0'
    from descricoes
    union all
    select
        'codigo_01_preservado',
        (select string_agg(item.codigo, ',') from unnest(zero_a_esquerda) as item),
        '01'
    from descricoes
    union all
    select
        'distinct_e_ordem_por_codigo',
        (select string_agg(item.codigo, ',') from unnest(com_duplicata_e_desordem) as item),
        '01,02'
    from descricoes
    union all
    select
        'descricao_nula_usa_formato_s',
        cast((
            select countif(item.descricao = format('Item não mapeado (código %s)', item.codigo))
            from unnest(sem_descricao) as item
        ) as string),
        cast(array_length(sem_descricao) as string)
    from descricoes
    union all
    select
        'descricoes_distintas_ordenadas',
        descricoes_padrao,
        'Item não mapeado (código 7), Item não mapeado (código 99)'
    from descricoes
    union all
    select
        'campo_e_separador_customizados',
        descricoes_alternativa,
        'Rótulo 1 | Rótulo 2'
    from descricoes
    union all
    select
        'campo_customizado_na_entrada',
        descricoes_campo_custom,
        'Rótulo 1 / Rótulo 2'
    from descricoes
    union all
    select
        'formato_preserva_aspas_barras_controles_e_unicode' as caso,
        (select item.descricao from unnest(formato_com_escape) as item) as obtido,
        "Item d'origem \\pasta\n\r\t\b\f\a\v99 😀" as esperado
    from descricoes
    union all
    select
        'separador_preserva_aspas_barras_controles_e_unicode' as caso,
        separador_com_escape as obtido,
        "A'\\pasta\n\r\t\b\f\a\v😀B" as esperado
    from descricoes
    union all
    select
        'percentuais_escapados_sao_literais' as caso,
        (select item.descricao from unnest(formato_percentual) as item) as obtido,
        'Item %s: 99 (%d; 100%)' as esperado
    from descricoes
)

select caso, obtido, esperado
from checks
where obtido is distinct from esperado
