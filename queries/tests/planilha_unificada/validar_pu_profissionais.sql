{{ config(tags = ['planilha_unificada']) }}

-- A posição mantém a associação com o registro interno, mesmo quando dois
-- profissionais têm o mesmo nome e a mesma ocupação; o ID exposto também é conferido.
with esperado as (
    select
        cast(p.id_usuario as string) as id_usuario,
        a.id_atendimento_modulo,
        posicao,
        to_json_string(struct(
            coalesce(pr.nome_profissional, '') as nome_profissional,
            coalesce(pr.descricoes_cbo, []) as descricoes_cbo,
            coalesce(pr.cbo_principal_descricao, '') as cbo_principal_descricao,
            coalesce(cast(pr.id_profissional as string), '') as id_profissional
        )) as profissional
    from {{ pu_ref('int_pu_atendimentos') }} as p
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario,
        unnest(p.atendimentos) as a,
        unnest(a.profissionais) as pr with offset as posicao
),

obtido as (
    select
        p.usuario.id_usuario,
        a.id_atendimento_modulo,
        posicao,
        to_json_string((select as struct pr.* except (id_familia_referencia, id_atendimento_legado))) as profissional
    from {{ ref('planilha_unificada') }} as p,
        unnest(p.atendimentos.registros) as a,
        unnest(a.profissionais) as pr with offset as posicao
)

select
    coalesce(e.id_usuario, o.id_usuario) as id_usuario,
    coalesce(e.id_atendimento_modulo, o.id_atendimento_modulo)
        as id_atendimento_modulo
from esperado as e
full outer join obtido as o
    on
        e.id_usuario = o.id_usuario
        and e.id_atendimento_modulo = o.id_atendimento_modulo
        and e.posicao = o.posicao
where e.profissional is distinct from o.profissional
