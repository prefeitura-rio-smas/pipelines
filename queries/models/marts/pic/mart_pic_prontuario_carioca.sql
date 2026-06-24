-- Mart: Prontuário Carioca para PIC
-- Grão: 1 linha por CPF de participante PIC
-- Conecta dados do PIC (sistema Prontuário Carioca - AcolheRio) com dados de
-- violação de direito e filiação documental.
-- Fonte: projeto social PIC (seqprojsoc=2) no gh_famil_projsociais.

{{ config(
    schema="pic",
    materialized="table"
) }}

with

-- Famílias marcadas como PIC (projeto social = 2) no AcolheRio
familias_pic as (
    select seqfamil
    from {{ source('brutos_acolherio_staging', 'gh_famil_projsociais') }}
    where seqprojsoc = 2
      and indativo = 'S'
      and datcancel is null
),

-- Membros ativos dessas famílias (último vínculo por pessoa)
membros_pic as (
    select
        m.seqpac,
        m.seqfamil,
        m.parentesco_responsavel_familia as parentesco
    from {{ source('brutos_acolherio_staging', 'gh_familias_membros') }} m
    inner join familias_pic f on m.seqfamil = f.seqfamil
    where m.datsaida is null
    qualify
        row_number() over (
            partition by m.seqpac
            order by m.datentrada desc
        ) = 1
),

-- Dados pessoais dos participantes PIC
participantes_pic as (
    select
        p.seqpac,
        p.numcpfpac as cpf,
        p.dscnomepac as nome,
        mp.parentesco
    from {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }} p
    inner join membros_pic mp on p.seqpac = mp.seqpac
    where p.numcpfpac is not null
      and p.numcpfpac != ''
      and upper(trim(p.dscnomepac)) not like 'TESTE%'
),

-- Usuários do Prontuário Carioca (dimensão já consolidada com violações)
usuarios_prontuario as (
    select
        id_usuario,
        cpf,
        nome,
        flag_possui_violacao_direito,
        violacoes
    from {{ ref('dim_usuarios') }}
    where cpf is not null
),

-- Evoluções de Documentação Civil (codabapac = 24)
-- Extrai indicadores de filiação do texto HTML semi-estruturado
filiacao_documentacao as (
    select
        seqpac as id_paciente,
        -- "Filiação completa na certidão de nascimento? ... <b> Sim</b>"
        regexp_contains(
            dscevopac,
            r'Filiação completa na certidão de nascimento\?.*?<b>\s*Sim\s*</b>'
        ) as possui_filiacao_completa,
        -- "Há interesse em tomar as medidas necessárias para inclusão da filiação faltante? ... <b> Sim</b>"
        regexp_contains(
            dscevopac,
            r'Há interesse em tomar as medidas necessárias para inclusão da filiação faltante\?.*?<b>\s*Sim\s*</b>'
        ) as interesse_filiacao_completa
    from {{ source('brutos_acolherio_staging', 'gh_evolupac') }}
    where codabapac = 24
    qualify
        row_number() over (
            partition by seqpac
            order by dtevopac desc
        ) = 1
),

-- Junção final
final as (
    select
        pp.cpf,
        pp.nome,
        pp.parentesco,
        case
            when up.flag_possui_violacao_direito = 'Sim' then true
            else false
        end as indicador_violacao_direito,
        up.violacoes as violacao_direito,
        coalesce(fd.possui_filiacao_completa, false) as possui_filiacao_completa,
        coalesce(fd.interesse_filiacao_completa, false) as interesse_filiacao_completa
    from participantes_pic pp
    left join usuarios_prontuario up
        on pp.seqpac = up.id_usuario
    left join filiacao_documentacao fd
        on pp.seqpac = fd.id_paciente
)

select * from final
