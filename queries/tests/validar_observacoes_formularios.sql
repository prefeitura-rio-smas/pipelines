{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- noqa: disable=AL03
-- Executa o extrator real, inclusive agrupamento de formulários da mesma evolução.
with casos as (
    select
        1 as id_evolucao,
        '<p>Usar &lt;teste&gt;</p>' as html,
        'Usar <teste>' as esperado
    union all
    select
        2,
        '<p>Primeiro</p><p>Segundo<br>Terceiro</p>',
        'Primeiro\nSegundo\nTerceiro'
    union all
    select
        3,
        '<p>A &amp; B: &#60;teste&#62; &#xE7;</p>',
        'A & B: <teste> ç'
    union all
    select
        4,
        '<p>&amp;lt;teste&amp;gt;</p>',
        '&lt;teste&gt;'
    union all
    select
        5,
        cast(null as string),
        ''
    union all
    select
        6,
        '<p></p>',
        ''
    union all
    select
        7,
        '<p>Mesmo texto</p>',
        'Mesmo texto'
    union all
    select
        7,
        '<div>Mesmo texto</div>',
        'Mesmo texto'
    union all
    select
        8,
        '<script>oculto</script><p>Visível</p><style>oculto</style>',
        'Visível'
    union all
    select
        9,
        '<p>Entidade &desconhecida;</p>',
        'Entidade &desconhecida;'
),

fonte as (
    select
        1 as id_usuario,
        id_evolucao,
        'usuario' as origem_modulo,
        cast(null as int64) as id_familia,
        1 as id_unidade,
        1 as id_profissional,
        cast(null as string) as codigo_abrangencia,
        cast(null as int64) as id_atividade,
        datetime '2026-01-01 12:00:00' as data_evolucao,
        cast(null as datetime) as data_cancelamento,
        'F' as tipo_evolucao,
        'Profissional' as nome_profissional,
        cast(null as string) as nome_atividade,
        concat('usuario:', cast(id_evolucao as string)) as id_evolucao_modulo,
        concat(
            '<h3>Centro POP - Atendimento Social</h3>Campo: <b>Resposta</b>',
            if(html is null, '', concat('<h3>OBSERVAÇÕES</h3>', html))
        ) as descricao_evolucao
    from casos
),

extraidos as (
    {{ pu_formularios_sql('fonte') }}
),

obtidos as (
    select f.*
    from extraidos as e, unnest(e.formularios) as f
),

esperado as (
    select
        id_evolucao,
        esperado,
        count(*) as quantidade
    from casos
    group by id_evolucao, esperado
),

contagens as (
    select
        id_evolucao,
        observacoes,
        count(*) as quantidade
    from obtidos
    group by id_evolucao, observacoes
)

select
    e.esperado,
    o.observacoes,
    e.quantidade as quantidade_esperada,
    o.quantidade as quantidade_obtida,
    coalesce(e.id_evolucao, o.id_evolucao) as id_evolucao
from esperado as e
full outer join contagens as o
    on e.id_evolucao = o.id_evolucao and e.esperado = o.observacoes
where e.quantidade is distinct from o.quantidade
