-- Intermediate: base do Cartão PIC (folha + enriquecimentos)
-- Grão: 1 linha por (data_particao, cod_familiar_fam).
--
-- Reúne a folha corrente (cartao_pic.beneficiarios) com: Controle CAS (CRAS,
-- doc verificada), Survey de entregas (dedup de negócio: primeira entrega
-- registrada no ArcGIS, created_date ASC) e Eventos de entrega (capacidade
-- acumulada por bairro, recorte por acao_smas: novos/retorno/ausência).
--
-- Base INCREMENTAL (insert_overwrite por data_particao): cada execução processa
-- apenas as partições da folha corrente e preserva as históricas materializadas.
-- Bootstrap (tabela inexistente): herda as partições anteriores da própria base
-- oficial (cartao_pic.status) + calcula a folha corrente.
--
-- A mart_status aplica apenas a apresentação final (cases de status/entrega).

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'data_particao', 'data_type': 'date'},
    tags=['cartao_pic_status'],
) }}

with folha_corrente as (

    -- PARTIÇÃO CORRENTE: cartao_pic.beneficiarios + schema completo
    select
        b.data_particao,
        b.cod_familiar_fam,
        b.num_cpf_responsavel,
        b.cpf_formatado,
        CAST(b.envelope as STRING) as envelope,
        b.nome_responsavel,
        CAST(NULL as STRING) as nome_social_responsavel,
        b.idade as idade_responsavel,
        b.cod_sexo_responsavel,
        b.qtd_criancas_0_a_4_anos,
        b.cas,
        b.bairro,
        b.nom_unidade_territorial_fam,
        b.endereco_fam,
        b.des_complemento_fam,
        b.desc_complemento_adic_fam as des_complemento_adic_fam,
        b.txt_referencia_local_fam,
        b.nom_centro_assist_fam,
        b.flag_folha_anterior,
        b.flag_remocao_decisao_administrativa as flag_familia_nao_encontrada,
        b.flag_mudanca_rf,
        b.flag_vr_correcao,
        b.familia.status,
        b.flag_recebe_envelope,
        b.familia.situacao,
        b.responsavel_familiar.mudou,
        b.responsavel_familiar.cod_familiar_fam_origem,
        b.responsavel_familiar.ausencia_documentacao as flag_ausencia_doc_civil,
        b.datas_participacao as cohort,
        b.cohort_entrada,
        b.qtd_folhas,
        case
            when b.idade < 18 then '0 a 17 anos'
            when b.idade < 30 then '18 a 29 anos'
            when b.idade < 40 then '30 a 39 anos'
            when b.idade < 50 then '40 a 49 anos'
            when b.idade < 60 then '50 a 59 anos'
            else '60 anos ou mais'
        end as faixa_etaria_responsavel,
        CONCAT('55', SUBSTR(b.num_tel_contato_1_fam, 1, 2), REGEXP_REPLACE(SUBSTR(b.num_tel_contato_1_fam, 3), r'^0+', '')) as num_tel_contato_1_fam,
        CONCAT('55', SUBSTR(b.num_tel_contato_2_fam, 1, 2), REGEXP_REPLACE(SUBSTR(b.num_tel_contato_2_fam, 3), r'^0+', '')) as num_tel_contato_2_fam,
        case
            when (
                b.cartao_retirado = 'sim' and b.familia.situacao = 'reconcessao'
                and b.retorno_apos_atualizacao = 'nao' and b.responsavel_familiar.ausencia_documentacao = 0
            )
            or (b.cartao_retirado = 'sim' and b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
                then 1
            else 0
        end as flag_reconcessao,
        case
            when b.flag_recebe_envelope = 1 and b.flag_mudanca_rf in (0, 1) then 'novos_beneficiarios'
            when
                b.retorno_apos_atualizacao = 'sim'
                or (b.cartao_retirado = 'nao' and b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
                then 'retorno_apos_atualizacao'
            when b.responsavel_familiar.ausencia_documentacao = 1 then 'ausencia_documentacao_obrigatoria'
            when (
                b.cartao_retirado = 'sim' and b.familia.situacao = 'reconcessao'
                and b.retorno_apos_atualizacao = 'nao' and b.responsavel_familiar.ausencia_documentacao = 0
            )
            or (b.cartao_retirado = 'sim' and b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
                then 'reconcessao'
            else 'mantidos'
        end as acao_smas,
        case
            when b.flag_recebe_envelope = 1 and b.flag_mudanca_rf in (0, 1) then 'retirar_cartao'
            when
                b.retorno_apos_atualizacao = 'sim'
                or (b.cartao_retirado = 'nao' and b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
                then 'retirar_cartao_apos_atualizacao'
            when b.responsavel_familiar.ausencia_documentacao = 1 then 'retirar_cartao_regularizacao_documento'
            when (
                b.cartao_retirado = 'sim' and b.familia.situacao = 'reconcessao'
                and b.retorno_apos_atualizacao = 'nao' and b.responsavel_familiar.ausencia_documentacao = 0
            )
            or (b.cartao_retirado = 'sim' and b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
                then 'aviso'
        end as alerta,
        case
            when b.cas = '01' then 'cas1@prefeitura.rio'
            when b.cas = '02' then 'cas2@prefeitura.rio'
            when b.cas = '03' then 'cas3@prefeitura.rio'
            when b.cas = '04' then 'cas4@prefeitura.rio'
            when b.cas = '05' then 'cas5@prefeitura.rio'
            when b.cas = '06' then 'cas6@prefeitura.rio'
            when b.cas = '07' then 'cas7@prefeitura.rio'
            when b.cas = '08' then 'cas8@prefeitura.rio'
            when b.cas = '09' then 'cas9@prefeitura.rio'
            when b.cas = '10' then 'cas10@prefeitura.rio'
        end as filtro_email_cas,
        case when b.familia.status in ('mantida', 'nova') then 'ativo' else 'inativo' end as ativo_inativo_familia,
        case when b.familia.status in ('mantida', 'nova') then NULL else b.familia.status end as motivo_inativo_familia,
        case when b.responsavel_familiar.situacao_rf in ('rf_mantido', 'rf_substituido', 'rf_nova_familia', 'rf_inedito') then 'ativo' else 'inativo' end as ativo_inativo_rf,
        case when b.responsavel_familiar.situacao_rf in ('rf_mantido', 'rf_substituido', 'rf_nova_familia', 'rf_inedito') then NULL else b.responsavel_familiar.situacao_rf end as motivo_inativo_rf
    from {{ source('cartao_pic', 'beneficiarios') }} as b
    {% if is_incremental() %}
        where b.data_particao >= (select MAX(t.data_particao) from {{ this }} as t)
    {% endif %}
),

-- Controle CAS: enriquece com CRAS, doc_verificada, justificativa e obs
controle_cas as (
    select
        c.cpf_sem_formatacao,
        c.cras,
        c.doc_verificada,
        c.categoria_justificativa,
        c.obs,
        c.data_particao,
        u.endereco_completo
    from {{ ref('raw_controle_cas') }} as c
    left join {{ ref('raw_unidade') }} as u
        on c.cras = TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(u.nome_popular, nfd), r'\pM', '')))
),

-- Survey: entregas reais (dedup de negócio: primeira entrega registrada no ArcGIS)
survey as (
    select *
    from {{ ref('raw_survey_primeira_infancia') }}
    qualify ROW_NUMBER() over (
        partition by cpf_sem_formatacao
        order by created_date asc
    ) = 1
),

-- Eventos: capacidade acumulada por bairro (quebras de bairro)
eventos_com_faixa as (
    select
        * except (bairro),
        UPPER(TRIM(bairro)) as bairro,
        CONCAT(bairro, '_', CAST(data_entrega as STRING), '_', hora_entrega) as id_evento_slot,
        COALESCE(SUM(quantidade) over (
            partition by bairro
            order by data_entrega, hora_entrega
        ), 999999) as rank_maximo_acumulado,
        COALESCE(SUM(quantidade) over (
            partition by bairro
            order by data_entrega, hora_entrega
            rows between unbounded preceding and 1 preceding
        ), 0) as cap_anterior
    from {{ ref('raw_bairros_entrega') }}
),

-- Beneficiário enriquecido: CAS + rank alfabético por bairro (distribuição nos slots)
beneficiario_enriquecido as (
    select
        s.*,
        c.cras as cras_previsto,
        c.doc_verificada,
        c.categoria_justificativa,
        c.obs,
        c.endereco_completo as endereco_cras_previsto,
        ROW_NUMBER() over (
            partition by s.bairro
            order by s.envelope asc
        ) as rank_alfabetico
    from folha_corrente as s
    left join controle_cas as c
        on
            s.num_cpf_responsavel = c.cpf_sem_formatacao
            and s.data_particao = c.data_particao
),

-- Junção final: base + eventos (recorte por acao_smas: novos/retorno/ausência) + survey
final as (
    select
        s.data_particao,
        s.cod_familiar_fam,
        s.num_cpf_responsavel,
        s.cpf_formatado,
        s.envelope,
        s.nome_responsavel,
        e.nome_social as nome_social_responsavel,
        s.idade_responsavel,
        s.faixa_etaria_responsavel,
        s.cod_sexo_responsavel,
        s.qtd_criancas_0_a_4_anos,
        s.cas,
        s.bairro,
        s.nom_unidade_territorial_fam,
        s.endereco_fam,
        s.des_complemento_fam,
        s.des_complemento_adic_fam,
        s.txt_referencia_local_fam,
        s.num_tel_contato_1_fam,
        s.num_tel_contato_2_fam,
        s.nom_centro_assist_fam,
        b.tipo_entrega as tipo_entrega_prevista,
        b.local_entrega as local_entrega_previsto,
        b.endereco_entrega as endereco_entrega_previsto,
        b.data_entrega as data_entrega_prevista,
        b.hora_entrega as hora_entrega_prevista,
        s.cras_previsto,
        s.endereco_cras_previsto,
        b.data_retirada_cras as data_retirada_cras_prevista,
        b.hora_retirada_cras as hora_retirada_cras_prevista,
        e.local_entrega as tipo_entrega,
        e.responsavel_retirada as responsavel_pela_retirada,
        e.nome_procurador,
        e.cpf_procurador,
        e.profissional as profissional_entrega,
        e.equipamento_referencia as profissional_entrega_equipamento,
        e.profissional_digit as profissional_digitacao,
        e.equipamento_referencia_digit as profissional_digitacao_equipamento,
        e.last_edited_user as survey_ultima_edicao_conta,
        s.doc_verificada,
        s.categoria_justificativa,
        s.obs,
        s.flag_folha_anterior,
        s.flag_familia_nao_encontrada,
        s.flag_mudanca_rf,
        s.flag_vr_correcao,
        s.status,
        s.flag_recebe_envelope,
        s.situacao,
        s.mudou,
        s.cod_familiar_fam_origem,
        s.flag_ausencia_doc_civil,
        s.flag_reconcessao,
        s.acao_smas,
        s.alerta,
        s.filtro_email_cas,
        s.cohort,
        s.cohort_entrada,
        s.qtd_folhas,
        s.ativo_inativo_familia,
        s.motivo_inativo_familia,
        s.ativo_inativo_rf,
        s.motivo_inativo_rf,
        DATE(e.data_entrega, 'America/Sao_Paulo') as data_entrega,
        case
            when e.local_entrega_outros is NULL or e.local_entrega_outros = '' then e.local_entrega_cras
            else e.local_entrega_outros
        end as local_entrega,
        DATE(e.created_date, 'America/Sao_Paulo') as survey_data_criacao,
        DATE(e.last_edited_date, 'America/Sao_Paulo') as survey_ultima_edicao_data,
        DATE(SAFE_CAST(e.timestamp_captura as TIMESTAMP), 'America/Sao_Paulo') as survey_data_extracao
    from beneficiario_enriquecido as s
    left join eventos_com_faixa as b
        on
            s.bairro = b.bairro
            and s.acao_smas in ('novos_beneficiarios', 'retorno_apos_atualizacao', 'ausencia_documentacao_obrigatoria')
            and s.rank_alfabetico > b.cap_anterior
            and s.rank_alfabetico <= b.rank_maximo_acumulado
    left join survey as e
        on s.num_cpf_responsavel = e.cpf_sem_formatacao
    qualify ROW_NUMBER() over (
        partition by s.data_particao, s.cod_familiar_fam
        order by b.data_entrega, b.hora_entrega
    ) = 1
)

{% if is_incremental() %}
    select * from final
{% else %}
-- Bootstrap: herda as partições históricas consolidadas na própria base oficial.
-- Qualificador st.: sem ele, `status` resolve para o alias implícito da tabela
-- (record da linha) no UNION, e não para a coluna homônima. A ordem das colunas
-- espelha o final (insert_overwrite casa por nome; UNION casa por posição).
    select
        st.data_particao,
        st.cod_familiar_fam,
        st.num_cpf_responsavel,
        st.cpf_formatado,
        st.envelope,
        st.nome_responsavel,
        st.nome_social_responsavel,
        st.idade_responsavel,
        st.faixa_etaria_responsavel,
        st.cod_sexo_responsavel,
        st.qtd_criancas_0_a_4_anos,
        st.cas,
        st.bairro,
        st.nom_unidade_territorial_fam,
        st.endereco_fam,
        st.des_complemento_fam,
        st.des_complemento_adic_fam,
        st.txt_referencia_local_fam,
        st.num_tel_contato_1_fam,
        st.num_tel_contato_2_fam,
        st.nom_centro_assist_fam,
        st.tipo_entrega_prevista,
        st.local_entrega_previsto,
        st.endereco_entrega_previsto,
        st.data_entrega_prevista,
        st.hora_entrega_prevista,
        st.cras_previsto,
        st.endereco_cras_previsto,
        st.data_retirada_cras_prevista,
        st.hora_retirada_cras_prevista,
        st.tipo_entrega,
        st.responsavel_pela_retirada,
        st.nome_procurador,
        st.cpf_procurador,
        st.profissional_entrega,
        st.profissional_entrega_equipamento,
        st.profissional_digitacao,
        st.profissional_digitacao_equipamento,
        st.survey_ultima_edicao_conta,
        st.doc_verificada,
        st.categoria_justificativa,
        st.obs,
        st.flag_folha_anterior,
        st.flag_familia_nao_encontrada,
        st.flag_mudanca_rf,
        st.flag_vr_correcao,
        st.status,
        st.flag_recebe_envelope,
        st.situacao,
        st.mudou,
        st.cod_familiar_fam_origem,
        st.flag_ausencia_doc_civil,
        st.flag_reconcessao,
        st.acao_smas,
        st.alerta,
        st.filtro_email_cas,
        st.cohort,
        st.cohort_entrada,
        st.qtd_folhas,
        st.ativo_inativo_familia,
        st.motivo_inativo_familia,
        st.ativo_inativo_rf,
        st.motivo_inativo_rf,
        st.data_entrega,
        st.local_entrega,
        st.survey_data_criacao,
        st.survey_ultima_edicao_data,
        st.survey_data_extracao
    from {{ source('cartao_pic', 'status') }} as st
    where st.data_particao < (select MAX(b.data_particao) from {{ source('cartao_pic', 'beneficiarios') }} as b)

    union all

    select * from final
{% endif %}
