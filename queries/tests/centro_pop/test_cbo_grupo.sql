-- depends_on: {{ ref('mart_planilha_unificada_centro_pop') }}

with casos as (
    select * from unnest([
        struct('cbo_secundario_tecnico' as caso, ['111100', '251605'] as codigos, true as esperado),
        ('somente_recepcao', ['411010'], false),
        ('codigo_com_espacos', [' 251605 '], true),
        ('array_vazio', array<string>[], false),
        ('array_ausente', cast(null as array<string>), false),
        ('codigo_desconhecido', array<string>[null], false)
    ])
),

resultados as (
    select caso, esperado, {{ possui_cbo_grupo('codigos', '2') }} as obtido
    from casos
)

select * from resultados
where obtido is distinct from esperado
