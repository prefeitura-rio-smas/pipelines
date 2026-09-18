-- Linkage Prontuário x CadÚnico para o item B.1 do RMA CRAS.
-- Grão: 1 linha por família do Prontuário com ao menos um membro ativo
-- (membros_familia.data_saida null).
-- renda_media_pc: menor renda média per capita CadÚnico
-- (identificacao_controle.valor_renda_media) entre as famílias CadÚnico
-- cadastradas e válidas vinculadas aos CPFs dos membros. Os CPFs são reduzidos
-- a dígitos e preenchidos à esquerda para preservar equivalência entre fontes
-- que armazenam o valor como texto ou número. NULL quando a família não possui
-- vínculo CadÚnico com renda registrada.
-- renda_media_pc_prontuario: soma das rendas declaradas no próprio Prontuário
-- (renda_ativa de todos os membros ativos + renda_beneficio somente para
-- benefícios de renda permanente: Aposentadoria, BPC, Pensão por morte e
-- Aposentadoria por invalidez — códigos 1, 9, 20 e 32 do domínio
-- map_coluna_beneficio) dividida pelo número de membros ativos. Membros sem
-- renda informada são ignorados; se nenhum membro informou renda, o resultado
-- é NULL (não informado), nunca 0.
-- O corte de extrema pobreza é aplicado no mart (var corte_extrema_pobreza).

with detalhes as (
    select
        id_paciente,
        any_value(renda_ativa) as renda_ativa,
        any_value(renda_beneficio) as renda_beneficio
    from {{ ref('raw_usuarios_detalhes') }}
    group by 1
),

membros as (
    select distinct
        m.id_familia,
        m.id_paciente,
        u.cpf,
        u.beneficio,
        d.renda_ativa,
        d.renda_beneficio
    from {{ ref('raw_membros_familia') }} as m
    inner join {{ ref('dim_usuarios') }} as u
        on m.id_paciente = u.id_usuario
    left join detalhes as d
        on m.id_paciente = d.id_paciente
    where m.data_saida is null
),

membros_cpf as (
    select distinct
        m.id_familia,
        lpad(regexp_replace(trim(m.cpf), r'[^0-9]', ''), 11, '0') as cpf
    from membros as m
    where
        m.cpf is not null
        and length(regexp_replace(trim(m.cpf), r'[^0-9]', '')) between 10 and 11
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

renda_cadunico as (
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
),

membros_renda as (
    select
        m.id_familia,
        m.id_paciente,
        safe_cast(m.renda_ativa as float64) as renda_ativa,
        case
            when exists (
                select 1 from unnest(m.beneficio) as b
                where b.codigo in ('1', '9', '20', '32')
            ) then safe_cast(m.renda_beneficio as float64)
        end as renda_beneficio_permanente
    from membros as m
),

renda_prontuario as (
    select
        id_familia,
        count(distinct id_paciente) as qtd_membros_ativos,
        countif(
            renda_ativa is not null or renda_beneficio_permanente is not null
        ) as qtd_membros_informaram,
        sum(
            coalesce(renda_ativa, 0) + coalesce(renda_beneficio_permanente, 0)
        ) as renda_familiar_prontuario
    from membros_renda
    group by 1
),

final as (
    select
        p.id_familia,
        p.qtd_membros_ativos,
        p.qtd_membros_informaram,
        c.renda_media_pc,
        case
            when p.qtd_membros_informaram = 0 then null
            else p.renda_familiar_prontuario
        end as renda_familiar_prontuario,
        case
            when p.qtd_membros_informaram = 0 then null
            else p.renda_familiar_prontuario / p.qtd_membros_ativos
        end as renda_media_pc_prontuario
    from renda_prontuario as p
    left join renda_cadunico as c on p.id_familia = c.id_familia
)

select * from final
