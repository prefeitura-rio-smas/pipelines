-- depends_on: {{ ref('mart_planilha_unificada_centro_pop') }}

with casos as (
    select * from unnest([
        struct('Diabetes' as resposta, true as esperado),
        ('CAPS', true),
        ('S', true),
        (' NÃO ', false),
        ('nao', false),
        ('n', false),
        (null, null),
        ('', null),
        ('   ', null),
        (' UNDEFINED ', null),
        ('null', null),
        ('Não Informado', null),
        ('nao informado', null),
        ('não informada', null),
        ('nao informada', null),
        (' NÃO SABE ', null),
        ('nao sabe', null),
        ('não soube informar', null),
        ('nao soube informar', null),
        ('sem informação', null),
        ('sem informacao', null),
        ('-', null),
        ('não se aplica', null),
        ('nao se aplica', null)
    ])
),

resultados as (
    select resposta, esperado, {{ interpretar_resposta_saude('resposta') }} as obtido
    from casos
)

select * from resultados
where obtido is distinct from esperado
