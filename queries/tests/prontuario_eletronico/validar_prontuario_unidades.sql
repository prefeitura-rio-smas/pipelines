{{ config(enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Cada lista pertence à unidade da linha, inclusive quando a unidade é desconhecida.
with inconsistencias as (
    {% for lista in ['projetos_sociais', 'atendimentos.registros', 'evolucoes.registros', 'centro_pop.registros', 'centro_pop.historico_mensal', 'creas.registros', 'alta_complexidade.registros'] %}
        select
            p.usuario.id_usuario,
            p.id_unidade,
            '{{ lista }}' as origem
        from {{ ref('prontuario_eletronico') }} as p, unnest(p.{{ lista }}) as r
        where p.id_unidade != coalesce(cast(r.id_unidade as string), '')
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select * from inconsistencias
union all
select
    p.usuario.id_usuario,
    p.id_unidade,
    'contadores' as origem
from {{ ref('prontuario_eletronico') }} as p
where
    p.atendimentos.quantidade != array_length(p.atendimentos.registros)
    or p.evolucoes.quantidade != array_length(p.evolucoes.registros)
    or p.atendimentos.quantidade_linhas_bi != (
        select coalesce(sum(array_length(r.profissionais)), 0) from unnest(p.atendimentos.registros) as r
    )
    {% for assunto in ['atendimentos', 'evolucoes'] %}
        {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
            or p.{{ assunto }}.quantidade_{{ servico }} != (
                select countif(r.tipo_servico = '{{ servico }}') from unnest(p.{{ assunto }}.registros) as r
            )
        {% endfor %}
    {% endfor %}
union all
select
    p.usuario.id_usuario,
    p.id_unidade,
    'unidade_e_permissoes' as origem
from {{ ref('prontuario_eletronico') }} as p
left join {{ ref('dim_unidades') }} as u on p.id_unidade = cast(u.id_unidade as string)
left join {{ ref('int_filtro_email_unidades') }} as e on p.id_unidade = cast(e.id_unidade as string)
where
    p.nome_unidade != coalesce(u.nome_unidade, '')
    or p.email_filtro != coalesce(e.email_filtro, '')
