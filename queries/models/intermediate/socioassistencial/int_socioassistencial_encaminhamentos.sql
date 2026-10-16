-- Uma declaração de encaminhamento por campo preenchido do formulário.
-- Valores multisseleção permanecem íntegros: a origem não identifica cada envio
-- nem o seu resultado. Não contar linhas como encaminhamentos efetivados.
select
    'ENCAMINHAMENTO' as tipo_registro,
    concat(f.origem_registro, ':encaminhamento') as origem_registro,
    {{ dbt_utils.generate_surrogate_key(['f.id_registro_origem', 'r.label', 'r.valor']) }} as id_registro_origem,
    f.id_usuario,
    f.id_familia,
    f.id_unidade,
    f.data_evento,
    f.competencia,
    f.data_referencia,
    struct(
        f.formulario.id_evolucao_modulo,
        f.formulario.origem_modulo,
        f.formulario.tipo_evolucao,
        f.formulario.codigo_abrangencia,
        f.formulario.titulo_formulario,
        f.formulario.data_cancelamento,
        r.label as categoria,
        r.valor as destinos_informados,
        cast(null as string) as resultado,
        false as flag_evento_individualizado
    ) as encaminhamento
from {{ ref('int_socioassistencial_formularios') }} as f,
    unnest(f.formulario.respostas) as r
where
    starts_with(r.label, 'Encaminhamentos')
    and nullif(nullif(trim(r.valor), 'undefined'), '') is not null
-- Observações diferentes não criam um segundo envio com mesmo campo/valor.
qualify row_number() over (
    partition by f.id_registro_origem, r.label, r.valor
    order by r.observacoes
) = 1
