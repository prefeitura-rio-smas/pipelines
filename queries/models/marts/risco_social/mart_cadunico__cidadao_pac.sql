{{ config(
    materialized='table',
    alias='cd_cidadao_pac',
    schema='prontuario_eletronico' if target.name == 'prod' else 'relatorio',
    tags=['risco_social'],
) }}

-- ============================================================
-- Espelho cd_cidadao_pac do CadÚnico
--
-- Contrato: 31 colunas, nomes e ordem herdados de
-- `pipelines/cadunico_espelho/export_cd_cidadao_pac.sql` (lido pelo app do
-- prontuário a partir do CSV em GCS). NÃO renomear, acrescentar nem
-- reordenar colunas.
--
-- Grão: 1 linha por membro de família (`id_membro_familia` único).
-- `id_familia` se repete legitimamente: a origem
-- `identificacao_primeira_pessoa` traz uma linha por membro.
--
-- Fonte: modelos `raw/cadunico/`, que já aplicam `filtro_particao_cadunico()`
-- (partição mais recente). A seleção de partição que antes vinha por query
-- parameter (uma por tabela) passa a ser resolvida pela macro no dbt.
-- ============================================================

with i as (

    select
        nis_original                        as dsccns,
        nome                                as dscnomepac,
        nome_social_pessoa                  as dscnomsoci,
        filiacao_1_nom_completo_mae_pessoa  as dscnmmae,
        filiacao_2                          as dscnmpai,
        left(pais_nascimento, 5)            as paisorigem,
        municipio_nascimento                as muninasc,
        data_nascimento                     as datnascim,

        case
            when sexo = 'Masculino' then 'M'
            when sexo = 'Feminino' then 'F'
        end as indsexo,

        case
            when id_tipo_identidade_genero = '002' then '06'
            when id_tipo_identidade_genero = '001' then '05'
            when id_tipo_identidade_genero = '003' then '04'
            else '99'
        end as indgenero,

        case
            when id_raca_cor = '1' then '01'
            when id_raca_cor = '2' then '02'
            when id_raca_cor = '3' then '04'
            when id_raca_cor = '4' then '03'
            when id_raca_cor = '5' then '05'
            else '99'
        end as racacor,

        parentesco_responsavel_familia,

        case
            when parentesco_responsavel_familia
                = 'Pessoa Responsável pela Unidade Familiar - RF'
            then true
            else false
        end as flag_rf,

        estado_cadastral,
        id_familia,
        id_membro_familia,
        data_particao

    from {{ ref('raw_identificacao_primeira_pessoa') }}

),

-- Identificação e controle do endereço familiar
ic as (

    select
        cep                                 as numcepender,
        left(localidade, 40)                as dscbairroender,
        'RJ'                                as sigufender,

        concat(
            coalesce(tipo_logradouro, ''),
            ' ',
            coalesce(titulo_logradouro, ''),
            ' ',
            coalesce(logradouro, '')
        )                                   as dslogradouro,

        left(cast(numero_logradouro as string), 7) as numend,
        complemento                         as complend,
        estado_cadastro,
        data_atualizacao,
        id_familia

    from {{ ref('raw_identificacao_controle') }}

),

-- Dados de contato familiar
c as (

    select
        nullif(
            concat(
                ltrim(contato_ddd, '0'),
                ltrim(contato_telefone, '0')
            ),
            ''
        ) as dsctelef1,

        nullif(
            concat(
                ltrim(contato_2_ddd, '0'),
                ltrim(contato_2_telefone, '0')
            ),
            ''
        ) as dsctelef2,

        id_familia

    from {{ ref('raw_contato') }}

),

-- Documentos pessoais
d as (

    select
        cpf             as numcpfpac,
        right(rg, 15)   as numrg,
        id_membro_familia

    from {{ ref('raw_documento_pessoa') }}

),

-- Dados de deficiência
pd as (

    select
        case
            when tem_deficiencia = 'Sim' then 'S'
            when tem_deficiencia = 'Não' then 'N'
        end as indpresdefi,

        id_membro_familia

    from {{ ref('raw_pessoa_deficiencia') }}

)

select
    -- Identificação da pessoa
    i.dsccns,
    i.dscnomepac,
    i.dscnomsoci,
    i.indgenero,
    i.paisorigem,
    i.muninasc,
    i.dscnmmae,
    i.dscnmpai,
    i.datnascim,
    i.indsexo,
    i.racacor,
    i.parentesco_responsavel_familia,
    i.flag_rf,

    -- Documentos pessoais
    d.numcpfpac,
    d.numrg,

    -- Contatos familiares
    c.dsctelef1,
    c.dsctelef2,

    -- Endereço familiar
    ic.numcepender,
    ic.dslogradouro,
    ic.numend,
    ic.complend,
    ic.dscbairroender,
    ic.sigufender,
    ic.estado_cadastro,
    ic.data_atualizacao,

    -- Deficiência
    pd.indpresdefi,

    -- Chaves de identificação
    i.id_familia,
    i.id_membro_familia,

    -- Indicadores fixos
    'Sim'           as flag_cadunico,
    'Rio de Janeiro' as muniend,

    -- Data da partição de identificação
    i.data_particao

from i

left join c
    on i.id_familia = c.id_familia

left join ic
    on i.id_familia = ic.id_familia

left join d
    on i.id_membro_familia = d.id_membro_familia

left join pd
    on i.id_membro_familia = pd.id_membro_familia

where i.estado_cadastral != 'Excluido'
    and ic.estado_cadastro = 'Cadastrado'
