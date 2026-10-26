{{ config(tags = ['planilha_unificada'], severity = 'warn') }}

with referencias as (
    select id_unidade_referencia as id_unidade from {{ pu_ref('dim_usuarios') }}
    union distinct
    select x.id_unidade from {{ pu_ref('dim_usuarios') }} as u, unnest(u.projetos_sociais) as x
    union distinct
    select s.id_unidade from {{ pu_ref('int_pu_familias') }} as p, unnest(p.familias) as f, unnest(f.servicos) as s
    union distinct
    select a.id_unidade from {{ pu_ref('int_pu_atendimentos') }} as p, unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    union distinct
    select e.id_unidade from {{ pu_ref('int_pu_evolucoes') }} as e
    inner join {{ pu_ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
)

select r.id_unidade
from referencias as r left join {{ pu_ref('dim_unidades') }} as u on r.id_unidade = u.id_unidade
where r.id_unidade is not null and nullif(trim(u.nome_unidade), '') is null
