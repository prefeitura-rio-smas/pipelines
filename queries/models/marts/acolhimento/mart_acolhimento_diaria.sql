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

final as (
    select
        a.id_acolhimento_sk,
        a.id_usuario_sk,
        a.id_unidade_sk,
        a.id_usuario,
        a.id_ciclo,

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
        usr.pais_origem,
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

        -- dias_acolhimento da fct já vem com +1 (correção do motor).
        a.dias_acolhimento                    as tempo_permanencia,
        a.flag_em_acolhimento,
        a.indicador_ciclo,
        a.motivo_saida,

        -- ===== UNIDADE =====
        un.id_unidade,
        un.nome_unidade                       as unidade,
        un.nome_tipo                           as tipo_unidade,
        un.classe,

        case
            when un.esfera = 'Municipal'       then 'Rede Pública'
            when un.esfera = 'Rede Conveniada' then 'Rede Conveniada'
            else un.esfera
        end                                   as esfera,

        un.total_vagas,
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
)

select
    *,
    extract(year from data_referencia)  as ano,
    extract(month from data_referencia) as mes,

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