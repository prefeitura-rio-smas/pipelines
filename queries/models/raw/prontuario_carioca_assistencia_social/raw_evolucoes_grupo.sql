-- Camada Raw: Evoluções de atividades de grupo
with source as (
    select
        seqevogrp as id_evolucao_grupo,
        seqatigrp as id_atividade,
        seqprof as id_profissional,
        dtevogrp as data_evolucao,
        dscevogrp as descricao_evolucao,
        codabagrp as codigo_abrangencia,
        indtpevogrp as tipo_evolucao,
        dtcancgrp as data_cancelamento,
        dsclistpac as lista_pacientes,
        codcboprof as cbo_profissional,
        dscrefpres as referencia_presenca
    from {{ source('prontuario_carioca_assistencia_social', 'gh_evolugrupo') }}
)
select * from source
