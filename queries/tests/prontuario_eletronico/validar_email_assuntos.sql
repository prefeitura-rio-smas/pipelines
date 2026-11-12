{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with registros as (
    {% for assunto in ['atendimentos', 'evolucoes', 'centro_pop', 'creas', 'alta_complexidade'] %}
        select
            p.usuario.id_usuario,
            safe_cast(r.id_unidade as int64) as id_unidade,
            r.email
        from {{ ref('prontuario_eletronico') }} as p, unnest(p.{{ assunto }}.registros) as r
        union all
    {% endfor %}
    select
        p.usuario.id_usuario,
        r.id_unidade,
        r.email
    from {{ ref('prontuario_eletronico') }} as p, unnest(p.projetos_sociais) as r
    union all
    select
        p.usuario.id_usuario,
        r.id_unidade,
        r.email
    from {{ ref('prontuario_eletronico') }} as p, unnest(p.centro_pop.historico_mensal) as r
)

select r.id_usuario
from registros as r left join {{ ref('int_filtro_email_unidades') }} as e on r.id_unidade = e.id_unidade
where r.email is distinct from coalesce(e.email, '')
