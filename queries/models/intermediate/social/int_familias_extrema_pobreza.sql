-- Linkage Prontuário x CadÚnico para o item B.1 do RMA CRAS.
-- Grão: 1 linha por família do Prontuário com ao menos um membro com CPF
-- localizado no CadÚnico. renda_media_pc = menor renda média per capita
-- (identificacao_controle.valor_renda_media) entre as famílias CadÚnico
-- vinculadas aos CPFs dos membros. O corte de extrema pobreza é aplicado
-- no mart (var corte_extrema_pobreza).

with membros_cpf as (
    select distinct
        m.id_familia,
        trim(u.cpf) as cpf
    from {{ ref('raw_membros_familia') }} as m
    inner join {{ ref('dim_usuarios') }} as u
        on m.id_paciente = u.id_usuario
    where
        m.data_saida is null
        and u.cpf is not null
        and trim(u.cpf) != ''
),

familias_cad as (
    select distinct
        p.id_familia,
        d.id_familia as id_familia_cad
    from membros_cpf as p
    inner join {{ ref('raw_documento_pessoa') }} as d
        on trim(d.cpf) = p.cpf
),

renda as (
    select
        f.id_familia,
        min(c.valor_renda_media) as renda_media_pc
    from familias_cad as f
    inner join {{ ref('raw_identificacao_controle') }} as c
        on f.id_familia_cad = c.id_familia
    where c.valor_renda_media is not null
    group by 1
)

select * from renda
