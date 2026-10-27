{{ config(tags = ['planilha_unificada']) }}

with esperado as (
    select
        cast(e.id_usuario as string) as id_usuario,
        e.id_evolucao_modulo,
        to_json_string(struct(
            coalesce(cast(e.id_evolucao_modulo as string), '') as id_evolucao_modulo,
            coalesce(cast(e.origem_modulo as string), '') as origem_modulo,
            coalesce(cast(e.id_familia as string), '') as id_familia,
            coalesce(e.nome_unidade, '') as nome_unidade,
            coalesce(cast(e.data_evolucao as string), '') as data_evolucao,
            coalesce(cast(e.data_cancelamento as string), '') as data_cancelamento,
            coalesce(cast(e.tipo_evolucao as string), '') as tipo_evolucao,
            coalesce(cast(e.nome_profissional as string), '') as nome_profissional,
            coalesce(cast(e.nome_atividade as string), '') as nome_atividade,
            {{ texto_html_legivel('e.descricao_evolucao') }} as descricao_evolucao,
            coalesce(e.tipo_servico, '') as tipo_servico
        )) as registro
    from {{ pu_ref('int_pu_evolucoes') }} as e
    inner join {{ pu_ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
),

obtido as (
    select
        p.usuario.id_usuario,
        v.id_evolucao_modulo,
        to_json_string((select as struct v.* except (formularios))) as registro
    from {{ ref('planilha_unificada') }} as p, unnest(p.evolucoes.registros) as v
)

select coalesce(e.id_usuario, o.id_usuario) as id_usuario
from esperado as e full outer join obtido as o on e.id_usuario = o.id_usuario and e.id_evolucao_modulo = o.id_evolucao_modulo
where e.registro is distinct from o.registro
