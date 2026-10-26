{{ config(tags = ['planilha_unificada']) }}

-- Não perder registros ao retirar as chaves técnicas ou traduzir nomes.
select p.usuario.id_usuario
from {{ ref('planilha_unificada') }} as p
left join
    {{ pu_ref('int_pu_familias') }} as f
    on p.usuario.id_usuario = cast(f.id_usuario as string)
left join
    {{ pu_ref('int_pu_atendimentos') }} as a
    on p.usuario.id_usuario = cast(a.id_usuario as string)
left join
    {{ pu_ref('int_pu_formularios') }} as e
    on p.usuario.id_usuario = cast(e.id_usuario as string)
where
    p.atendimentos.quantidade is null
    or p.atendimentos.quantidade != array_length(p.atendimentos.registros)
    or exists (
        select 1 from unnest(p.atendimentos.registros) as evento
        where
            (not {{ var('pu_referencia', false) }} and upper(trim(evento.flag_cancelado)) = 'S')
            or (evento.hora_atendimento != '' and not regexp_contains(
                evento.hora_atendimento, r'^(?:[01][0-9]|2[0-3]):[0-5][0-9]$'
            ))
    )
    or array_length(p.familias) != coalesce(array_length(f.familias), 0)
    or array_length(p.atendimentos.registros) != coalesce(array_length(a.atendimentos), 0)
    or (select coalesce(sum(array_length(v.formularios)), 0) from unnest(p.evolucoes.registros) as v) != coalesce(array_length(e.formularios), 0)
    or coalesce(
        (select sum(array_length(x.vinculos)) from unnest(p.familias) as x), 0
    )
    != coalesce(
        (select sum(array_length(x.vinculos)) from unnest(f.familias) as x), 0
    )
    or coalesce(
        (select sum(array_length(x.servicos)) from unnest(p.familias) as x), 0
    )
    != coalesce(
        (select sum(array_length(x.servicos)) from unnest(f.familias) as x), 0
    )
    or coalesce(
        (
            select sum(array_length(x.profissionais))
            from unnest(p.atendimentos.registros) as x
        ),
        0
    )
    != coalesce(
        (
            select sum(array_length(x.profissionais))
            from unnest(a.atendimentos) as x
        ),
        0
    )
    or coalesce(
        (select sum(array_length(x.campos)) from unnest(p.evolucoes.registros) as v, unnest(v.formularios) as x), 0
    )
    != coalesce(
        (select sum(array_length(x.campos)) from unnest(e.formularios) as x), 0
    )
