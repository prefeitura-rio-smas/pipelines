-- noqa: disable=AL03
{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with composicao_fixture as (
    select * from unnest([
        struct('cas' as email_cas, 'planilha' as email_planilha, 'unidade' as email_unidade, 'cas,planilha, unidade' as esperado, 'cas,unidade,planilha' as esperado_programada),
        ('cas', 'planilha', null, 'cas,planilha, ', 'cas,,planilha'),
        ('cas', null, 'unidade', 'cas,, unidade', 'cas,unidade,'),
        (null, 'planilha', 'unidade', ',planilha, unidade', ',unidade,planilha'),
        ('cas', null, null, 'cas,, ', 'cas,,'),
        (null, 'planilha', null, ',planilha, ', ',,planilha'),
        (null, null, 'unidade', ',, unidade', ',unidade,'),
        (null, null, null, ',, ', ',,'),
        ('', '', '', ',, ', ',,')
    ])
),

unidades_fixture as (
    select
        id as id_unidade,
        lpad(cast(id as string), 2, '0') as cas,
        'unidade@example.test' as email_unidade
    from unnest(generate_array(1, 10)) as id
    union all
    select
        11,
        'GE',
        cast(null as string)
),

planilha_fixture as (
    select * from unnest([
        struct(' UNIDADE ' as unidade_atendimento, ' b@example.test,\na@example.test, b@example.test ' as email),
        ('unidade', ' b@example.test,\na@example.test, b@example.test '),
        ('Nula', cast(null as string)),
        ('Vazia', '')
    ])
),

atendimentos_fixture as (
    select
        id as _id_unidade,
        'antigo' as email_cas,
        'antigo' as email_unidade,
        'antigo' as email,
        concat('evento-', cast(id as string)) as id_atendimento,
        case when id <= 10 then 'Unidade' when id = 13 then 'Nula' when id = 14 then 'Vazia' else 'Sem planilha' end as unidade_atendimento
    from unnest(generate_array(1, 14)) as id
),

obtido as (
    {{ aplicar_filtro_email('atendimentos_fixture', 'unidades_fixture', 'planilha_fixture') }}
),

esperado as (
    select
        id_atendimento,
        if(
            _id_unidade <= 10,
            concat('cas', cast(_id_unidade as string), '@prefeitura.rio,unidade@example.test, b@example.test,\na@example.test, b@example.test '),
            'gdados.smas@prefeitura.rio,,'
        ) as email
    from atendimentos_fixture
),

comparacao as (
    select
        to_json_string(struct(id_atendimento, email)) as conteudo,
        1 as delta
    from esperado
    union all
    select
        to_json_string(struct(id_atendimento, email)),
        -1
    from obtido
)

select conteudo from comparacao
group by conteudo
having sum(delta) != 0
union all
select 'composicao_tres_fontes' as conteudo
from composicao_fixture
where {{ filtro_email('email_cas', 'email_planilha', 'email_unidade') }} is distinct from esperado
union all
select 'composicao_programada' as conteudo
from composicao_fixture
where {{ filtro_email('email_cas', 'email_planilha', 'email_unidade', formato='programada') }} is distinct from esperado_programada
