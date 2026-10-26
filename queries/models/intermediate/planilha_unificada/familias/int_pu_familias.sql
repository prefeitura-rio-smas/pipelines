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

servicos_distintos as (
    select distinct
        s.id_familia,
        s.id_servico_assistencial,
        s.id_unidade,
        s.id_login_cadastro,
        s.data_cadastro,
        s.data_cancelamento,
        un.nome_unidade
    from {{ pu_ref('raw_familias_servicos_assistenciais') }} as s
    left join {{ pu_ref('dim_unidades') }} as un on s.id_unidade = un.id_unidade
),

servicos as (
    select
        id_familia,
        array_agg(struct(
            id_servico_assistencial,
            id_unidade,
            nome_unidade,
            id_login_cadastro,
            data_cadastro,
            data_cancelamento,
            {{ map_nome_servico_assistencial('id_servico_assistencial') }} as nome_servico
        ) order by data_cadastro, id_servico_assistencial, id_unidade, data_cancelamento, id_login_cadastro) as servicos
    from servicos_distintos
    group by id_familia
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
        coalesce(v.vinculos, []) as vinculos,
        coalesce(s.servicos, []) as servicos
    ) order by uf.id_familia) as familias
from usuarios_familias as uf
left join familias as f on uf.id_familia = f.id_familia
left join {{ pu_ref('dim_usuarios') }} as r on f.id_usuario_responsavel = r.id_usuario
left join vinculos as v on uf.id_usuario = v.id_usuario and uf.id_familia = v.id_familia
left join servicos as s on uf.id_familia = s.id_familia
group by uf.id_usuario
