{{ config(
    materialized = 'table',
    schema = 'alta_complexidade'
) }}

-- Ocupação diária dos acolhimentos institucionais.
-- Grão: 1 linha por ciclo × data_referência (explosão temporal).
-- Apresentação estilizada para o BI Looker, em paridade com a query legada
-- dashboard_acolherio.alta_complexidade_dev (renomeações, CASEs, normalizações).
--
-- Arquitetura (pós-refactor):
--   fct_acolhimento_ciclos → fct_acolhimento_diaria → mart_acolhimento_diaria (esta).
-- A explosão temporal e a filtragem data_referencia <= current_date() ficam na
-- fct_acolhimento_diaria; esta mart só JOINs com dim_usuarios/dim_unidades
-- (campos cosméticos: escolaridade, bairro, eixo, e-mails, etc.) + CASEs.

with diaria as (
    select * from {{ ref('fct_acolhimento_diaria') }}
),

usuarios as (
    select * from {{ ref('dim_usuarios') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

emails_planilha as (
    select * from {{ ref('raw_sheets_filtro_email_prontuario') }}
),

ultima_atualizacao_pipeline as (
    {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }}
),

final as (
    select
        a.id_usuario    as seqpac,
        a.id_ciclo      as seqciclo,

        -- ===== USUARIO =====
        usr.nome                              as nome_usuario,
        usr.nome_social,
        usr.filiacao_mae,
        a.cpf,
        case when coalesce(a.cpf, '') = '' then 'Não' else 'Sim' end as flag_cpf,
        case
            when a.cpf is null or a.cpf = '' then 'CPF não informado'
            else 'CPF informado'
        end                                   as status_cpf,
        format(
            "%s.%s.%s-%s",
            substr(lpad(cast(a.cpf as string), 11, '0'), 1, 3),
            substr(lpad(cast(a.cpf as string), 11, '0'), 4, 3),
            substr(lpad(cast(a.cpf as string), 11, '0'), 7, 3),
            substr(lpad(cast(a.cpf as string), 11, '0'), 10, 2)
        )                                     as cpf_formatado,
        usr.data_nascimento,

        date_diff(current_date(), usr.data_nascimento, year)
            - if(extract(dayofyear from usr.data_nascimento) >
                  extract(dayofyear from current_date()), 1, 0) as idade,

        case usr.sexo
            when 'M' then 'Masculino'
            when 'F' then 'Feminino'
        end                                   as sexo,
        coalesce(usr.raca_cor, 'Não Informado')         as raca_cor,
        case
            when usr.genero is null then 'Não Informado'
            when usr.genero = 'Sem Informação' then 'Não Informado'
            else usr.genero
        end                                   as genero,
        case
            when usr.orientacao_sexual is null then 'Não Informado'
            when usr.orientacao_sexual = 'Não informado' then 'Não Informado'
            else usr.orientacao_sexual
        end                                   as orientacao_sexual,
        coalesce(usr.nacionalidade, 'Não Informado')   as nacionalidade,
        usr.condicao_estrangeira,
        usr.pais_origem                       as pais_origem_descricao,
        coalesce(usr.bairro, 'Não Informado')           as bairro,
        coalesce(usr.origem_demanda, 'Não Informado')   as origem_demanda,
        usr.motivo_acolhimento,

        case usr.flag_recebe_beneficio
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end                                   as flag_recebe_beneficio,
        usr.tipo_beneficio,

        usr.flag_trabalha                     as flag_trabalho,
        usr.vinculo_trabalhista,
        usr.profissao,

        case usr.flag_frequenta_escola
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end                                   as frequenta_escola,

        case usr.escolaridade_indice
            when 'Nunca estudou'                            then '01. Nunca estudou'
            when 'Não Alfabetizado'                         then '02. Não Alfabetizado'
            when 'Alfabetizado'                             then '03. Alfabetizado'
            when 'Nível fundamental incompleto'             then '04. Nível fundamental incompleto'
            when 'Nível fundamental incompleto (cursando)'  then '04. Nível fundamental incompleto'
            when 'Nível fundamental completo'                then '05. Nível fundamental completo'
            when 'Nível médio incompleto'                    then '06. Nível médio incompleto'
            when 'Nível médio incompleto (cursando)'         then '06. Nível médio incompleto'
            when 'Nível médio completo'                      then '07. Nível médio completo'
            when 'Superior incompleto'                       then '08. Superior incompleto'
            when 'Superior incompleto (cursando)'            then '08. Superior incompleto'
            when 'Superior completo'                         then '09. Superior completo'
            when 'Não sabe/Não informou'                     then '10. Não sabe/Não informou'
            else '10. Não sabe/Não informou'
        end                                   as escolaridade,

        case usr.flag_cadunico
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end                                   as flag_cadunico,

        case usr.flag_curatela
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end                                   as curatela,

        case
            when usr.flag_saude_mental_comprometida = 'N' then 'Não'
            when usr.flag_saude_mental_comprometida = ''  then 'Não Informado'
            else usr.flag_saude_mental_comprometida
        end                                   as diagnostico_saude_mental,

        case
            when usr.flag_saude_mental_comprometida = 'N' then 'Não'
            when usr.flag_saude_mental_comprometida = ''  then 'Não Informado'
            else 'Sim'
        end                                   as saude_mental,

        case usr.flag_deficiencia
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end                                   as deficiencia,
        usr.tipo_deficiencia,
        usr.flag_situacao_rua                 as situacao_de_rua,
        usr.flag_possui_violacao_direito       as flag_violacao_direito,
        usr.violacoes,

        -- ===== CICLO / FATO =====
        a.data_entrada,
        a.data_saida                           as data_desligamento,
        case
            when a.data_saida is null then 'Acolhido'
            else 'Desligado'
        end                                   as status_acolhimento,

        a.flag_em_acolhimento,
        a.indicador_ciclo,
        a.motivo_saida,

        -- dias_acolhimento da fct já vem com +1 (correção do motor).
        a.dias_acolhimento                    as tempo_permanencia,

        case
            when a.motivo_saida = '2' then 'Decisão da direção/equipe - Conflito com profissional da unid.'
            when a.motivo_saida = '3' then 'Decisão da direção/equipe - Regras da instituição'
            when a.motivo_saida = '4' then 'Voluntário - Conflito com outro acolhido'
            when a.motivo_saida = '5' then 'Voluntário - Conflito com profissional da unid.'
            when a.motivo_saida = '7' then 'Voluntário - Regras da instituição'
            when a.motivo_saida = '8' then 'Voluntário - Abstinência'
            when a.motivo_saida = 'C' then 'Decisão da direção/equipe - Conflito com outro acolhido'
            when a.motivo_saida = 'D' then 'Voluntário - Sem motivo identificado'
            when a.motivo_saida = 'E' then 'Acolhido'
            when a.motivo_saida = 'F' then 'Reinserção comunitária'
            when a.motivo_saida = 'G' then 'Reinserção em família'
            when a.motivo_saida = 'H' then 'Afastamento de cri/adol por medida protetiva'
            when a.motivo_saida = 'I' then 'Solicitação de vaga'
            when a.motivo_saida = 'J' then 'Reinserção PVTN'
            when a.motivo_saida = 'K' then 'Mudança para outro município'
            when a.motivo_saida = 'L' then 'Transferência para clínica de apoio a saúde'
            when a.motivo_saida = 'N' then 'Transferência para delegacia policial'
            when a.motivo_saida = 'O' then 'Óbito'
            when a.motivo_saida = 'T' then 'Transferência para outra unid.'
            when a.motivo_saida = 'X' then 'Fechado pela Unificação'
            when a.motivo_saida = 'Y' then 'Demanda por serviço diurno'
            when a.motivo_saida = 'Z' then 'Demanda por serviço noturno'
            when (a.motivo_saida is null or a.motivo_saida = '') and a.flag_em_acolhimento = 1
                then 'Não desligado'
            when (a.motivo_saida is null or a.motivo_saida = '') and a.flag_em_acolhimento = 0
                then 'Acolhido'
            else 'Acolhido'
        end                                   as motivo_desligamento,

        case
            when a.motivo_saida = 'F' then 'Deslig. por reinserção comunitária'
            when a.motivo_saida = 'G' then 'Deslig. por reinserção em família de origem ou família extensa'
            when a.motivo_saida = 'J' then 'Reinserção através do projeto de volta a terra natal'
            when (a.motivo_saida is null or a.motivo_saida = '') and a.flag_em_acolhimento = 1
                then 'Null'
            when a.flag_em_acolhimento = 0 and a.motivo_saida not in ('F', 'G', 'J')
                then 'Outros motivos'
            else 'Null'
        end                                   as motivo_reinsercao,

        extract(year from a.data_saida)       as ano_desligamento,

        case
            when a.data_entrada < date(extract(year from current_date()), 1, 1)
                 and a.data_saida is null
                then date(extract(year from current_date()), 1, 1)
            else a.data_entrada
        end                                   as data_auxiliar,

        coalesce(a.data_saida, current_date()) as data_desligamento_auxiliar,

        -- ===== UNIDADE =====
        un.id_unidade                         as sequs,
        un.nome_unidade                       as unidade,
        case
            when upper(un.nome_unidade) like '%ALBERGUE%'     then 'Albergue'
            when upper(un.nome_unidade) like '%CRAF%'         then 'Central de Recepção'
            when upper(un.nome_unidade) like '%CRAS%'         then 'CRAS'
            when upper(un.nome_unidade) like '%CREAS%'        then 'CREAS'
            when upper(un.nome_unidade) like '%CRI%'          then 'Central de Recepção'
            when upper(un.nome_unidade) like '%REPÚBLICA%'    then 'República'
            when upper(un.nome_unidade) like '%URS%'          then 'URS'
            when un.nome_unidade is null                      then 'Não Informado'
            else 'Unidade Conveniada'
        end                                   as tipo_unidade,
        un.classe,

        case
            when un.esfera = 'Municipal'       then 'Rede Pública'
            when un.esfera = 'Rede Conveniada' then 'Rede Conveniada'
            else un.esfera
        end                                   as esfera,

        un.total_vagas                        as vagas_totais,
        un.vagas_disponiveis                   as vagas_livres,
        un.vagas_bloqueadas,
        un.vagas_homens,
        un.vagas_mulheres,
        un.vagas_neutras,
        a.tipo_publico,

        case
            when un.flag_unidade_ativa then 'Sim'
            else 'Não'
        end                                   as unidade_ativa,

        un.cas                                 as cas_original,
        case un.cas
            when '01' then '01ª CAS'
            when '02' then '02ª CAS'
            when '03' then '03ª CAS'
            when '04' then '04ª CAS'
            when '05' then '05ª CAS'
            when '06' then '06ª CAS'
            when '07' then '07ª CAS'
            when '08' then '08ª CAS'
            when '09' then '09ª CAS'
            when '10' then '10ª CAS'
        end                                   as cas_nome,
        case un.cas
            when '01' then 'cas1@prefeitura.rio'
            when '02' then 'cas2@prefeitura.rio'
            when '03' then 'cas3@prefeitura.rio'
            when '04' then 'cas4@prefeitura.rio'
            when '05' then 'cas5@prefeitura.rio'
            when '06' then 'cas6@prefeitura.rio'
            when '07' then 'cas7@prefeitura.rio'
            when '08' then 'cas8@prefeitura.rio'
            when '09' then 'cas9@prefeitura.rio'
            when '10' then 'cas10@prefeitura.rio'
        end                                   as email_cas,
        un.email_unidade,

        em.email                              as email_planilha,

        case
            when un.flag_eixo_adulto = 'Não'  and un.flag_eixo_idoso = 'Não'  and un.flag_eixo_familia = 'Não' then 'Outra unidade SMAS'
            when un.flag_eixo_adulto = 'Não'  and un.flag_eixo_idoso = 'Sim'  and un.flag_eixo_familia = 'Não' then 'Eixo idoso'
            when un.flag_eixo_adulto = 'Sim'  and un.flag_eixo_idoso = 'Não'  and un.flag_eixo_familia = 'Não' then 'Eixo adulto'
            when un.flag_eixo_adulto = 'Sim'  and un.flag_eixo_idoso = 'Sim'  and un.flag_eixo_familia = 'Sim' then 'Eixo adulto, idoso e família'
            when un.flag_eixo_adulto = 'Sim'  and un.flag_eixo_idoso = 'Sim'  and un.flag_eixo_familia = 'Não' then 'Eixo adulto e idoso'
            when un.flag_eixo_adulto = 'Sim'  and un.flag_eixo_idoso = 'Não'  and un.flag_eixo_familia = 'Sim' then 'Eixo adulto e família'
            else 'Outros'
        end                                   as eixo,

        a.data_referencia

    from diaria a
    left join usuarios usr   on a.id_usuario_sk = usr.id_usuario_sk
    left join unidades un    on a.id_unidade_sk = un.id_unidade_sk
    left join emails_planilha em on lower(trim(em.unidade_atendimento)) = lower(trim(un.nome_unidade))
    cross join ultima_atualizacao_pipeline ult_atualizacao
)

select
    *,
    ult_atualizacao.ultima_atualizacao,
    extract(day from ult_atualizacao.ultima_atualizacao)   as dia_atualizacao,
    extract(month from ult_atualizacao.ultima_atualizacao) as mes_atualizacao,
    extract(year from ult_atualizacao.ultima_atualizacao)  as ano_atualizacao,
    extract(hour from ult_atualizacao.ultima_atualizacao)  as hora_atualizacao,
    extract(minute from ult_atualizacao.ultima_atualizacao) as minuto_atualizacao,
    extract(year from data_referencia)  as ano_referencia,
    extract(month from data_referencia) as mes_referencia,

    case
        when idade between 0 and 3    then 'De 0 a 3 anos - Bebê'
        when idade between 4 and 11   then 'De 04 a 11 anos - Criança'
        when idade between 12 and 17  then 'De 12 a 17 anos - Adolescente'
        when idade between 18 and 59  then 'De 18 a 59 anos - Adulto'
        when idade >= 60              then 'Mais de 60 anos - Idoso'
        else 'Não informada'
    end                                   as faixa_etaria,

    concat(
        coalesce(email_cas, ''), ',',
        coalesce(email_planilha, ''), ', ',
        coalesce(email_unidade, '')
    )                                     as email

from final