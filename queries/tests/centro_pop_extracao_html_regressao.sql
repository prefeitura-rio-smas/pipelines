{{ config(tags = ['daily']) }}

-- Exercita a macro real no BigQuery; o teste falha quando retorna alguma linha.
with fonte as (
    select
        1 as id_evolucao,
        concat(
            '<h3>Centro POP - Atendimento Social</h3>',
            '<p>Demanda inicial: <b>Acolhimento</b></p>',
            '<p class="campo">Território da referência familiar:\n<b>Rio <em>de</em> Janeiro</b>\n</p>',
            '<h3>OBSERVAÇÕES</h3><p>Nota <b>livre</b></p>',
            '<h3 class="titulo">Centro POP - Plano de Atendimento Individual (PAI)</h3>',
            '<p>Data do atendimento: <b>01/09/2026</b><br>Demanda inicial: <b>Saúde &amp; documentação</b></p>',
            '<h3>CONTEÚDO E DESCRIÇÃO</h3>Nota PAI'
        ) as descricao_evolucao

    union all

    select
        2 as id_evolucao,
        '<H3>Situação de Saúde</H3><P>Situação: <B>Diabetes</B></P>' as descricao_evolucao

    union all

    select
        3 as id_evolucao,
        '<h3>Situação de Saúde</h3><p>Sem campos respondidos</p>' as descricao_evolucao
),

extraido as (
    select
        id_evolucao,
        titulo_formulario,
        label,
        valor,
        trim(regexp_replace(coalesce(observacoes, ''), r'<[^>]*>', '')) as observacoes
    from {{ extrair_campos_html_evolucao('fonte', ['id_evolucao']) }}
),

esperado as (
    select
        1 as id_evolucao,
        'Centro POP - Atendimento Social' as titulo_formulario,
        'Demanda inicial' as label,
        'Acolhimento' as valor,
        'Nota livre' as observacoes
    union all
    select
        1 as id_evolucao,
        'Centro POP - Atendimento Social' as titulo_formulario,
        'Território da referência familiar' as label,
        'Rio de Janeiro' as valor,
        'Nota livre' as observacoes
    union all
    select
        1 as id_evolucao,
        'Centro POP - Plano de Atendimento Individual (PAI)' as titulo_formulario,
        'Data do atendimento' as label,
        '01/09/2026' as valor,
        'Nota PAI' as observacoes
    union all
    select
        1 as id_evolucao,
        'Centro POP - Plano de Atendimento Individual (PAI)' as titulo_formulario,
        'Demanda inicial' as label,
        'Saúde & documentação' as valor,
        'Nota PAI' as observacoes
    union all
    select
        2 as id_evolucao,
        'Situação de Saúde' as titulo_formulario,
        'Situação' as label,
        'Diabetes' as valor,
        '' as observacoes
),


diferencas as (
    (
        select * from extraido
        except distinct
        select * from esperado
    )
    union all
    (
        select * from esperado
        except distinct
        select * from extraido
    )
)

select 'campos divergentes' as falha from diferencas
union all
select 'quantidade divergente' as falha
from (select 1)
where (select count(*) from extraido) != (select count(*) from esperado)
