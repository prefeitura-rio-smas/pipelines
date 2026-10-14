{% macro situacao_saude_ate_mes(usuarios_mes, respostas_saude) %}
    select
        e.id_usuario_unidade_mes,
        r.uso_substancias,
        r.possui_acompanhamento_saude,
        -- A janela considera o histórico completo antes de escolher a última resposta.
        -- Qualquer positivo permanece válido; todos desconhecidos resultam em NULL.
        logical_or(r.possui_problema_saude) over (
            partition by e.id_usuario_unidade_mes
        ) as possui_historico_problema_saude
    from {{ usuarios_mes }} as e
    inner join {{ respostas_saude }} as r
        on
            e.id_usuario = r.id_paciente
            and {{ registro_ate_evento('r.data_evolucao', 'e.data_referencia') }}
    qualify row_number() over (
        partition by e.id_usuario_unidade_mes
        order by r.data_evolucao desc, r.id_evolucao desc
    ) = 1
{% endmacro %}
