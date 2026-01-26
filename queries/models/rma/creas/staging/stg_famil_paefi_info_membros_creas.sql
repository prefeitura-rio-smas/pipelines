-- Tabela retorna todos os membros de cada família que estão em acompanhamento PAEFI suas informações individuais;
-- Tabela não retorna nenhum usuário teste
-- Pode haver usuários repetidos por conta da violação de direito e benefícios.

with informacao_membros as (
    select 
    b.seqfamil,
    a.seqpac,
    a.nome_usuario,
    a.data_nascimento,
    a.sexo,
    a.racacor,
    from {{ ref('raw_cidadao_pac_creas') }} a
    inner join {{ ref('raw_membros_familias_creas') }} b on a.seqpac = b.seqpac
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
),

tratar_data_paefi as (
    select
        seqfamil,
        data_cadastro_paefi,
        extract(day from data_cadastro_paefi) as dia_cadastro_paefi,
        extract(month from data_cadastro_paefi) as mes_cadastro_paefi,
        extract(year from data_cadastro_paefi) as ano_cadastro_paefi,
        seqlogincad,
        seqservassist
    from {{ ref('raw_servassist_creas') }}
)

select
    a.seqfamil,
    a.seqpac,
    a.nome_usuario,
    c.viol_direito,
    d.beneficio,
    a.data_nascimento,
    a.dia_nascimento,
    a.mes_nascimento,
    a.idade,
    a.sexo,
    a.racacor,
    b.data_cadastro_paefi,
    b.dia_cadastro_paefi,
    b.mes_cadastro_paefi,
    b.ano_cadastro_paefi,
    b.seqlogincad
    from tratar_idade_membro a
    inner join tratar_data_paefi b on a.seqfamil = b.seqfamil
    left join {{ ref('raw_violacao_direito_creas') }} c on a.seqpac = c.seqpac
    left join {{ ref('raw_beneficios_creas') }} d on a.seqpac = d.seqpac
