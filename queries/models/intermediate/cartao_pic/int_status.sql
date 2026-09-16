-- Intermediate: status empilhada do Cartão PIC (base estática por folha)
-- Grão: 1 linha por (data_particao, cod_familiar_fam).
--
-- Base INCREMENTAL (insert_overwrite por data_particao): cada execução processa
-- apenas as partições da folha corrente (cartao_pic.beneficiarios) e preserva
-- as partições históricas já materializadas na própria tabela.
--
-- Bootstrap (tabela inexistente): herda as partições anteriores da própria
-- base oficial (cartao_pic.status) + calcula a folha corrente. Sem snapshots
-- externos e sem datas hardcoded — a próxima folha entra sozinha.
--
-- Os atributos DINÂMICOS (Survey, eventos, status de entrega) ficam na
-- mart_status (atualizada de hora em hora).

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

)

{% if is_incremental() %}
-- Incremental: partições da folha corrente ainda não refletidas na base
-- (>= última partição materializada; reprocessa a atual quando a folha é corrigida)
    select *
    from folha_corrente as fc
    where fc.data_particao >= (select MAX(t.data_particao) from {{ this }} as t)
{% else %}
-- Bootstrap: herda as partições históricas consolidadas na própria base oficial.
-- Qualificador st.: sem ele, `status` resolve para o alias implícito da tabela
-- (record da linha) no UNION, e não para a coluna homônima. A ordem das colunas
-- espelha o folha_corrente (insert_overwrite casa por nome; UNION casa por posição).
    select
        st.data_particao,
        st.cod_familiar_fam,
        st.num_cpf_responsavel,
        st.cpf_formatado,
        st.envelope,
        st.nome_responsavel,
        st.nome_social_responsavel,
        st.idade_responsavel,
        st.cod_sexo_responsavel,
        st.qtd_criancas_0_a_4_anos,
        st.cas,
        st.bairro,
        st.nom_unidade_territorial_fam,
        st.endereco_fam,
        st.des_complemento_fam,
        st.des_complemento_adic_fam,
        st.txt_referencia_local_fam,
        st.nom_centro_assist_fam,
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
        st.cohort,
        st.cohort_entrada,
        st.qtd_folhas,
        st.faixa_etaria_responsavel,
        st.num_tel_contato_1_fam,
        st.num_tel_contato_2_fam,
        st.flag_reconcessao,
        st.acao_smas,
        st.alerta,
        st.filtro_email_cas,
        st.ativo_inativo_familia,
        st.motivo_inativo_familia,
        st.ativo_inativo_rf,
        st.motivo_inativo_rf
    from {{ source('cartao_pic', 'status') }} as st
    where st.data_particao < (select MAX(b.data_particao) from {{ source('cartao_pic', 'beneficiarios') }} as b)

    union all

    select * from folha_corrente
{% endif %}
