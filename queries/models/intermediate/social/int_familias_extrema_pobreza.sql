-- Linkage Prontuário x CadÚnico para o item B.1 do RMA CRAS.
-- Grão: 1 linha por família do Prontuário com ao menos um membro com CPF
-- localizado no CadÚnico. Os CPFs são reduzidos a dígitos e preenchidos à
-- esquerda para preservar equivalência entre fontes que armazenam o valor como
-- texto ou número. renda_media_pc = menor renda média per capita
-- (identificacao_controle.valor_renda_media) entre as famílias CadÚnico
-- cadastradas e válidas vinculadas aos CPFs dos membros. O corte de extrema
-- pobreza é aplicado no mart (var corte_extrema_pobreza).

with membros_cpf as (
    select distinct
        m.id_familia,
        lpad(regexp_replace(trim(u.cpf), r'[^0-9]', ''), 11, '0') as cpf
    from {{ ref('raw_membros_familia') }} as m
    inner join {{ ref('dim_usuarios') }} as u
        on m.id_paciente = u.id_usuario
    where
        m.data_saida is null
        and u.cpf is not null
        and length(regexp_replace(trim(u.cpf), r'[^0-9]', '')) between 10 and 11
),

familias_cad as (
    select distinct
        p.id_familia,
        d.id_familia as id_familia_cad
    from membros_cpf as p
    inner join {{ ref('raw_documento_pessoa') }} as d
        on lpad(regexp_replace(trim(d.cpf), r'[^0-9]', ''), 11, '0') = p.cpf
    where length(regexp_replace(trim(d.cpf), r'[^0-9]', '')) between 10 and 11
),

renda as (
    select
        f.id_familia,
        min(c.valor_renda_media) as renda_media_pc
    from familias_cad as f
    inner join {{ ref('raw_identificacao_controle') }} as c
        on f.id_familia_cad = c.id_familia
    where
        c.valor_renda_media is not null
        and c.id_estado_cadastro = 3
        and c.id_cadastro_valido = 1
    group by 1
)

select * from renda
