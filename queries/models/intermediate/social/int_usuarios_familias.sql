{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with membros as (
    select distinct
        m.id_paciente as id_usuario,
        m.id_familia,
        m.id_membro,
        m.id_familia_nova,
        m.id_motivo_saida,
        m.data_entrada,
        m.data_saida,
        m.parentesco_responsavel,
        ms.descricao_motivo_saida as motivo_saida
    from {{ pu_ref('raw_membros_familia') }} as m
    left join {{ pu_ref('raw_familias_motivos_saida') }} as ms on m.id_motivo_saida = ms.id_motivo_saida
    where m.id_paciente is not null and m.id_familia is not null
),

vinculos as (
    select
        id_usuario,
        id_familia,
        array_agg(struct(
            id_membro,
            id_familia_nova,
            id_motivo_saida,
            data_entrada,
            data_saida,
            parentesco_responsavel,
            motivo_saida
        ) order by data_entrada, id_membro, data_saida, id_familia_nova, id_motivo_saida, parentesco_responsavel) as vinculos
    from membros
    group by id_usuario, id_familia
),

familias as (
    select distinct * from {{ pu_ref('raw_familias') }}
),

-- Inclui responsáveis sem registro na tabela de membros.
usuarios_familias as (
    select
        id_usuario,
        id_familia
    from membros
    union distinct
    select
        id_usuario_responsavel as id_usuario,
        id_familia
    from familias
    where id_usuario_responsavel is not null and id_familia is not null
),

membros_atuais_candidatos as (
    select
        id_familia,
        id_usuario,
        parentesco_responsavel,
        data_entrada,
        id_membro,
        0 as prioridade_responsavel
    from membros
    where data_saida is null
    union all
    select
        id_familia,
        id_usuario_responsavel as id_usuario,
        'Responsável' as parentesco_responsavel,
        null as data_entrada,
        null as id_membro,
        1 as prioridade_responsavel
    from familias
    where id_usuario_responsavel is not null and id_familia is not null
),

membros_atuais_unicos as (
    select
        id_familia,
        id_usuario,
        parentesco_responsavel
    from membros_atuais_candidatos
    qualify row_number() over (
        partition by id_familia, id_usuario
        order by prioridade_responsavel desc, data_entrada desc, id_membro desc, parentesco_responsavel asc
    ) = 1
),

composicao_familiar as (
    select
        m.id_familia,
        array_agg(struct(
            m.id_usuario,
            u.nome,
            m.parentesco_responsavel
        ) order by coalesce(u.nome, ''), m.id_usuario) as membros
    from membros_atuais_unicos as m
    left join {{ pu_ref('dim_usuarios') }} as u on m.id_usuario = u.id_usuario
    group by m.id_familia
),

servicos as (
    select
        id_familia,
        servicos_historico as servicos
    from {{ pu_ref('int_servicos_agregados') }}
)

select
    uf.id_usuario,
    array_agg(struct(
        uf.id_familia,
        f.id_usuario_responsavel,
        r.nome as nome_responsavel,
        f.id_usuario_responsavel = uf.id_usuario as flag_responsavel_atual,
        f.flag_ativo,
        f.data_ultima_modificacao,
        coalesce(cf.membros, []) as membros,
        coalesce(v.vinculos, []) as vinculos,
        coalesce(s.servicos, []) as servicos
    ) order by uf.id_familia) as familias
from usuarios_familias as uf
left join familias as f on uf.id_familia = f.id_familia
left join {{ pu_ref('dim_usuarios') }} as r on f.id_usuario_responsavel = r.id_usuario
left join composicao_familiar as cf on uf.id_familia = cf.id_familia
left join vinculos as v on uf.id_usuario = v.id_usuario and uf.id_familia = v.id_familia
left join servicos as s on uf.id_familia = s.id_familia
group by uf.id_usuario
