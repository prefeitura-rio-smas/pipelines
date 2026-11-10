{{ config(enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
select
    a.id_usuario,
    array_agg(struct(
        a.id_ciclo, a.id_unidade, u.nome_unidade,
        a.data_entrada, a.data_saida, a.motivo_saida,
        date_diff(coalesce(a.data_saida, current_date('America/Sao_Paulo')), a.data_entrada, day)
            as tempo_permanencia_dias
    ) order by a.data_entrada, a.id_ciclo) as historico
from {{ pu_ref('raw_usuarios_acolhimentos') }} as a
left join {{ pu_ref('dim_unidades') }} as u on a.id_unidade = u.id_unidade
group by a.id_usuario
