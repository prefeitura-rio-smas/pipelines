-- ============================================================
-- Intermediate: membros de família do CadÚnico
--
-- Consolida as 5 fontes do layout usadas pelo espelho do prontuário
-- (identificação da pessoa, identificação/controle da família, contato,
-- documentação e deficiência) na granularidade de MEMBRO DE FAMÍLIA.
--
-- Responsabilidades desta camada:
--   1. os 5 joins (chaves: id_familia para família, id_membro_familia para membro);
--   2. as normalizações de texto (telefones e logradouro);
--   3. o filtro de negócio do espelho:
--        estado_cadastral <> 'Excluido'  e  estado_cadastro = 'Cadastrado'.
--
-- NÃO faz tradução de código, truncamento nem constantes do contrato — isso é
-- do mart `cd_cidadao_pac`.
--
-- Grão: 1 linha por `id_membro_familia`.
-- Materialização: table (auditável) — definida em `dbt_project.yml`.
--
-- Fontes: modelos `raw/cadunico/`, que já aplicam `filtro_particao_cadunico()`.
-- ============================================================

with i as (

    select
        id_familia,
        id_membro_familia,
        nis_original,
        nome,
        nome_social_pessoa,
        filiacao_1_nom_completo_mae_pessoa,
        filiacao_2,
        pais_nascimento,
        municipio_nascimento,
        data_nascimento,
        sexo,
        id_tipo_identidade_genero,
        id_raca_cor,
        parentesco_responsavel_familia,
        estado_cadastral,
        data_particao

    from {{ ref('raw_identificacao_primeira_pessoa') }}

),

-- Identificação e controle do endereço familiar
ic as (

    select
        id_familia,
        cep,
        localidade,
        numero_logradouro,
        complemento,
        estado_cadastro,
        data_atualizacao,

        concat(
            coalesce(tipo_logradouro, ''),
            ' ',
            coalesce(titulo_logradouro, ''),
            ' ',
            coalesce(logradouro, '')
        ) as logradouro_completo

    from {{ ref('raw_identificacao_controle') }}

),

-- Contato familiar (normalizado: zeros à esquerda removidos, vazio → NULL)
c as (

    select
        id_familia,

        nullif(
            concat(
                ltrim(contato_ddd, '0'),
                ltrim(contato_telefone, '0')
            ),
            ''
        ) as telefone_1,

        nullif(
            concat(
                ltrim(contato_2_ddd, '0'),
                ltrim(contato_2_telefone, '0')
            ),
            ''
        ) as telefone_2

    from {{ ref('raw_contato') }}

),

-- Documentos pessoais
d as (

    select
        id_membro_familia,
        cpf,
        rg

    from {{ ref('raw_documento_pessoa') }}

),

-- Deficiência
pd as (

    select
        id_membro_familia,
        tem_deficiencia

    from {{ ref('raw_pessoa_deficiencia') }}

)

select
    -- Identificação da pessoa
    i.nis_original,
    i.nome,
    i.nome_social_pessoa,
    i.filiacao_1_nom_completo_mae_pessoa,
    i.filiacao_2,
    i.pais_nascimento,
    i.municipio_nascimento,
    i.data_nascimento,
    i.sexo,
    i.id_tipo_identidade_genero,
    i.id_raca_cor,
    i.parentesco_responsavel_familia,

    -- Documentos pessoais
    d.cpf,
    d.rg,

    -- Contatos familiares (normalizados)
    c.telefone_1,
    c.telefone_2,

    -- Endereço familiar
    ic.cep,
    ic.logradouro_completo,
    ic.numero_logradouro,
    ic.complemento,
    ic.localidade,
    ic.estado_cadastro,
    ic.data_atualizacao,

    -- Deficiência
    pd.tem_deficiencia,

    -- Chaves e partição
    i.id_familia,
    i.id_membro_familia,
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

where i.estado_cadastral <> 'Excluido'
    and ic.estado_cadastro = 'Cadastrado'
