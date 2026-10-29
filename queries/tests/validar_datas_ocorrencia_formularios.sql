{{ config(enabled=target.name != 'prod') }}
select
    p.usuario.id_usuario,
    e.id_evolucao_modulo
from {{ ref('planilha_unificada') }} as p,
    unnest(p.evolucoes.registros) as e,
    unnest(e.formularios) as f
where
    f.origem_data_ocorrencia not in ('formulario', 'fonte')
    or f.situacao_data_ocorrencia not in ('valida', 'ausente', 'invalida', 'conflitante', 'nao_mapeado')
    or (f.origem_data_ocorrencia = 'formulario' and (f.situacao_data_ocorrencia != 'valida' or safe_cast(f.data_ocorrencia as date) is null))
    or (f.origem_data_ocorrencia = 'fonte' and safe_cast(f.data_ocorrencia as date) is distinct from safe_cast(substr(e.data_evolucao, 1, 10) as date))
