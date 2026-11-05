{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with fonte as (
    select * from unnest([
        struct(
            1 as id_usuario, 1 as seq_atendimento, 'u' as modulo, 10 as id_familia,
            date '2026-01-01' as data_de_atendimento, 'Unidade CRAS' as unidade_atendimento,
            'CRAS' as tipo_unidade, 'permitido@example.test;gestor@example.test' as email
        ),
        (1, 1, 'u', 10, date '2026-01-01', 'Unidade CRAS', 'CRAS', 'permitido@example.test;gestor@example.test'),
        (1, 1, 'u', 11, date '2026-01-01', 'Unidade CRAS', 'CRAS', 'permitido@example.test;gestor@example.test'),
        (1, 2, 'u', cast(null as int64), date '2026-01-01', 'Unidade CRAS', 'CRAS', 'permitido@example.test'),
        (2, 1, 'f', 20, date '2026-01-01', 'Abrigo', 'ALBERGUE', 'permitido@example.test'),
        (3, 3, 'f', 30, date '2026-01-01', cast(null as string), 'CREAS', 'permitido@example.test'),
        (3, 4, 'f', 40, date '2026-01-01', 'Negado', 'CENTRO POP', 'permitido@example.test'),
        (4, 5, 'u', 50, date '2025-12-31', 'Unidade POP', 'CENTRO POP', 'permitido@example.test'),
        (4, 6, 'u', 60, date '2026-11-03', 'Unidade CREAS', 'CREAS', 'permitido@example.test'),
        (5, 7, 'f', 70, date '2026-01-01', 'Unidade CRAS', 'CRAS', cast(null as string))
    ])
),

extraidos as (
    {{ pu_familias_atendidas_sql('fonte') }}
),

linhas as (
    select
        f.*,
        p.id_usuario
    from extraidos as p, unnest(p.familias_atendidas) as f
),

fonte_conflito as (
    select * from fonte
    where seq_atendimento = 1 and modulo = 'u' and id_familia = 10
    union all
    select * replace ('outro@example.test' as email)
    from fonte
    where seq_atendimento = 1 and modulo = 'u' and id_familia = 10
),

conflitos as (
    {{ pu_familias_atendidas_sql('fonte_conflito') }}
),

metricas as (
    select
        visualizador,
        count(distinct if(
            f.elegivel_indicador = 'S'
            and safe_cast(f.data_atendimento as date) between date '2026-01-01' and date '2026-11-02'
            and strpos(f.email, nullif(visualizador, '')) > 0,
            f.id_familia, null
        )) as familias
    from linhas as f
    cross join unnest(['permitido@example.test', 'gestor@example.test', 'sem-acesso@example.test', '']) as visualizador
    group by visualizador
),

checagens as (
    select
        'multiplicidade' as teste,
        count(*) != 8 as falhou
    from linhas
    union all
    select
        'familias_do_mesmo_evento' as teste,
        count(*) != 2 as falhou
    from linhas
    where id_atendimento_modulo = '1u'
    union all
    select
        'modulos_independentes' as teste,
        count(*) != 1 as falhou
    from linhas
    where id_atendimento_modulo = '1f' and id_familia = '20'
    union all
    select
        'inelegiveis' as teste,
        count(*) != 3 as falhou
    from linhas
    where elegivel_indicador = 'N'
    union all
    select
        'email_ausente' as teste,
        count(*) != 1 as falhou
    from linhas
    where id_familia = '70' and email = ''
    union all
    select
        'conflitos_visiveis' as teste,
        sum(array_length(familias_atendidas)) != 2 as falhou
    from conflitos
    union all
    select
        'acesso_e_periodo' as teste,
        countif(familias != if(visualizador in ('permitido@example.test', 'gestor@example.test'), 2, 0)) != 0 as falhou
    from metricas
)

select teste from checagens
where falhou
