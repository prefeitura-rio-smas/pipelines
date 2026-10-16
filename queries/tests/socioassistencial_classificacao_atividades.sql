-- Casos de negócio: recorrência sozinha não identifica grupo PAIF, nem
-- o texto PAIF sem recorrência comprova grupo regular.
with casos as (
    select
        1 as id_tipo_atividade,
        'Grupo de famílias' as nome_atividade,
        'Grupo' as nome_tipo_atividade,
        'Semanal' as recorrencia,
        true as grupo_esperado,
        true as scfv_esperado
    union all
    select
        5 as id_tipo_atividade,
        'PAIF' as nome_atividade,
        'Oficina' as nome_tipo_atividade,
        '' as recorrencia,
        false as grupo_esperado,
        false as scfv_esperado
    union all
    select
        3 as id_tipo_atividade,
        'Convivência' as nome_atividade,
        'SCFV' as nome_tipo_atividade,
        'Mensal' as recorrencia,
        false as grupo_esperado,
        true as scfv_esperado
),

classificados as (
    select
        grupo_esperado,
        scfv_esperado,
        {{ classificar_atividade_rma('casos') }}
    from casos
)

select grupo_esperado
from classificados
where flag_grupo_regular_paif is distinct from grupo_esperado or flag_scfv is distinct from scfv_esperado
