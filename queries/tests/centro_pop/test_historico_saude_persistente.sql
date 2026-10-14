with perfil_mensal as (
    select distinct
        id_usuario,
        mes_referencia,
        flag_problema_saude
    from {{ ref('mart_planilha_unificada_centro_pop') }}
    where id_usuario is not null
),

primeira_evidencia as (
    select
        id_usuario,
        min(mes_referencia) as mes_primeira_evidencia
    from perfil_mensal
    where flag_problema_saude = 'Sim'
    group by id_usuario
)

select p.*
from perfil_mensal as p
inner join primeira_evidencia as e
    on p.id_usuario = e.id_usuario and p.mes_referencia >= e.mes_primeira_evidencia
where p.flag_problema_saude != 'Sim'
