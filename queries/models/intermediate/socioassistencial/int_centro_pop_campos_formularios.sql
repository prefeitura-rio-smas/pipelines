-- IDs de evolução são qualificados pelo módulo para não misturar registros
-- da família e do usuário que tenham o mesmo número na origem.
with evolucoes_centro_pop as (
    select
        e.id_unidade,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        concat(e.origem_modulo, ':', cast(e.id_evolucao as string)) as id_evolucao,
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 29
        and e.data_cancelamento is null
        and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

campos_centro_pop as (
    select *
    from {{ extrair_campos_html_evolucao(
        source_relation = 'evolucoes_centro_pop',
        id_cols = ['id_paciente', 'id_unidade', 'id_evolucao', 'data_evolucao']
    ) }}
    where titulo_formulario in (
        'Centro POP - Plano de Atendimento Individual (PAI)',
        'Centro POP - Atendimento Social',
        'Centro POP - Desligamento PAI'
    )
)

select * from campos_centro_pop
