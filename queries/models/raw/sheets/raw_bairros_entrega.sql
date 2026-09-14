-- Camada Raw: Eventos de entrega do Cartão PIC (planilha externa)
-- Fonte: rj-smas-dev.pic.raw_cartao_primeira_infancia_carioca_bairros_entrega (Google Sheets)
-- Capacidade por bairro/slot (QUANTIDADE), previsão de entrega e disparos WhatsApp.
with source as (
    select
        cas,
        bairro,
        quantidade,
        etapa,
        tipo_entrega,
        local_entrega,
        endereco_entrega,
        data_entrega,
        hora_entrega,
        cras,
        endereco_cras,
        data_retirada_cras,
        hora_retirada_cras,
        data_disparo_aviso,
        aprovacao_disparo_aviso,
        data_disparo_lembrete,
        aprovacao_disparo_lembrete,
        data_disparo_cras,
        aprovacao_disparo_cras,
        disparo_aviso_enviado,
        disparo_lembrete_enviado,
        evento_ocorreu,
        cartao_carregado
    from {{ source('pic_eventos', 'raw_cartao_primeira_infancia_carioca_bairros_entrega') }}
)

select * from source
