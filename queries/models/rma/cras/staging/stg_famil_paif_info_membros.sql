-- Tabela retorna todos os membros de cada família que está em acompanhamento PAIF e a informação indivídual de cada membro da família;
-- Tabela não retorna nenhum usuário teste

with informacao_membros as (
    select 
    b.seqfamil,
    a.seqpac,
    a.nome_usuario,
    a.data_nascimento,
    a.sexo,
    a.racacor,
    from {{ ref('base_cidadao_pac') }} a
    inner join {{ ref('base_familia_membros') }} b on a.seqpac = b.seqpac
),

tratar_idade_membro as (
    select
        seqfamil,
        seqpac,
        nome_usuario,
        data_nascimento,
        extract(day from data_nascimento) as dia_nascimento,
        extract(month from data_nascimento) as mes_nascimento,
        date_diff(current_date(), data_nascimento, year) as idade,
        sexo,
        racacor
    from informacao_membros
)

select
    a.seqfamil,
    a.seqpac,
    a.nome_usuario,
    c.viol_direito,
    d.beneficio,
    e.seqvulnerab,
    a.data_nascimento,
    a.dia_nascimento,
    a.mes_nascimento,
    a.idade,
    a.sexo,
    a.racacor,
    b.data_cadastro_paif,
    b.seqlogincad
    from tratar_idade_membro a
    inner join {{ ref('base_servassist') }} b on a.seqfamil = b.seqfamil
    left join {{ ref('base_violacao_direito') }} c on a.seqpac = c.seqpac
    left join {{ ref('base_beneficios') }} d on a.seqpac = d.seqpac
    left join {{ ref('base_vulnerab') }} e on a.seqfamil = e.seqfamil
