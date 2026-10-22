-- ============================================================
-- Mart: cd_cidadao_pac (Prontuário Eletrônico)
--
-- CONTRATO EXTERNO: 31 colunas, com estes nomes e esta ordem, consumidas
-- pelo app do prontuário a partir do CSV publicado em
-- `gs://rj-smas-dev/raw/cadunico/cd_cidadao_pac_<data_particao>/`.
-- Não renomear, acrescentar nem reordenar coluna.
--
-- Esta camada é SÓ contrato: tradução de código, truncamentos e constantes.
-- Zero JOIN, zero WHERE, zero agregação — o recorte vive em
-- `int_cadunico_membros_familia`.
--
-- Grão: 1 linha por membro de família (`id_membro_familia`).
-- Destino: `prontuario_eletronico` (prod) / `relatorio` (dev), via pasta.
-- ============================================================

select
    -- Identificação da pessoa
    membros.nis_original                            as dsccns,
    membros.nome                                    as dscnomepac,
    membros.nome_social_pessoa                      as dscnomsoci,

    case
        when membros.id_tipo_identidade_genero = '002' then '06'
        when membros.id_tipo_identidade_genero = '001' then '05'
        when membros.id_tipo_identidade_genero = '003' then '04'
        else '99'
    end                                         as indgenero,

    left(membros.pais_nascimento, 5)                as paisorigem,
    membros.municipio_nascimento                    as muninasc,
    membros.filiacao_1_nom_completo_mae_pessoa      as dscnmmae,
    membros.filiacao_2                              as dscnmpai,
    membros.data_nascimento                         as datnascim,

    case
        when membros.sexo = 'Masculino' then 'M'
        when membros.sexo = 'Feminino' then 'F'
    end                                         as indsexo,

    case
        when membros.id_raca_cor = '1' then '01'
        when membros.id_raca_cor = '2' then '02'
        when membros.id_raca_cor = '3' then '04'
        when membros.id_raca_cor = '4' then '03'
        when membros.id_raca_cor = '5' then '05'
        else '99'
    end                                         as racacor,

    membros.parentesco_responsavel_familia,

    case
        when membros.parentesco_responsavel_familia
            = 'Pessoa Responsável pela Unidade Familiar - RF'
        then true
        else false
    end                                         as flag_rf,

    -- Documentos pessoais
    membros.cpf                                     as numcpfpac,
    right(membros.rg, 15)                           as numrg,

    -- Contatos familiares
    membros.telefone_1                              as dsctelef1,
    membros.telefone_2                              as dsctelef2,

    -- Endereço familiar
    membros.cep                                     as numcepender,
    membros.logradouro_completo                     as dslogradouro,
    left(cast(membros.numero_logradouro as string), 7) as numend,
    membros.complemento                             as complend,
    left(membros.localidade, 40)                    as dscbairroender,
    'RJ'                                        as sigufender,
    membros.estado_cadastro,
    membros.data_atualizacao,

    -- Deficiência
    case
        when membros.tem_deficiencia = 'Sim' then 'S'
        when membros.tem_deficiencia = 'Não' then 'N'
    end                                         as indpresdefi,

    -- Chaves de identificação
    membros.id_familia,
    membros.id_membro_familia,

    -- Indicadores fixos
    'Sim'                                       as flag_cadunico,
    'Rio de Janeiro'                            as muniend,

    -- Data da partição de identificação
    membros.data_particao

from {{ ref('int_cadunico_membros_familia') }} as membros
