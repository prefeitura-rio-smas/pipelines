-- Camada Raw: Eventos de entrega do Cartão PIC (planilha externa)
-- Fonte: rj-smas-dev.pic.raw_cartao_primeira_infancia_carioca_bairros_entrega (Google Sheets)
-- Capacidade por bairro/slot (QUANTIDADE), previsão de entrega e disparos WhatsApp.
with source as (
    select
        CAS as cas,
        BAIRRO as bairro,
        QUANTIDADE as quantidade,
        ETAPA as etapa,
        TIPO_ENTREGA as tipo_entrega,
        LOCAL_ENTREGA as local_entrega,
        ENDERECO_ENTREGA as endereco_entrega,
        DATA_ENTREGA as data_entrega,
        HORA_ENTREGA as hora_entrega,
        CRAS as cras,
        ENDERECO_CRAS as endereco_cras,
        DATA_RETIRADA_CRAS as data_retirada_cras,
        HORA_RETIRADA_CRAS as hora_retirada_cras,
        DATA_DISPARO_AVISO as data_disparo_aviso,
        APROVACAO_DISPARO_AVISO as aprovacao_disparo_aviso,
        DATA_DISPARO_LEMBRETE as data_disparo_lembrete,
        APROVACAO_DISPARO_LEMBRETE as aprovacao_disparo_lembrete,
        DATA_DISPARO_CRAS as data_disparo_cras,
        APROVACAO_DISPARO_CRAS as aprovacao_disparo_cras,
        DISPARO_AVISO_ENVIADO as disparo_aviso_enviado,
        DISPARO_LEMBRETE_ENVIADO as disparo_lembrete_enviado,
        EVENTO_OCORREU as evento_ocorreu,
        CARTAO_CARREGADO as cartao_carregado
    from {{ source('pic_eventos', 'raw_cartao_primeira_infancia_carioca_bairros_entrega') }}
)

select * from source