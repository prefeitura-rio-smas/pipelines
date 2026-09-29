{#
Seleciona um único registro relacionado por evento, considerando somente registros
disponíveis até a data do evento.

Parâmetros:
- relacao_evento / relacao_registros: nomes de relações SQL ou CTEs.
- pares_chave: lista de pares {evento: coluna, registro: coluna} usados no join.
- colunas_saida: colunas da relação de registros que serão retornadas.
- id_evento / data_evento / data_registro / id_registro: colunas configuráveis;
  id_registro desempata registros com a mesma data.

A macro é genérica para relações de evento e histórico. As chamadas informam as
chaves e colunas do domínio; a macro não fixa fonte ou regra de negócio.
#}
{% macro ultimo_registro_ate_evento(
    relacao_evento,
    relacao_registros,
    pares_chave,
    colunas_saida,
    id_evento,
    data_evento,
    data_registro,
    id_registro
) %}
(
    select
        e.{{ id_evento }},
        {% for coluna in colunas_saida %}
        r.{{ coluna }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ relacao_evento }} as e
    inner join {{ relacao_registros }} as r
        on
            {% for par in pares_chave %}
            e.{{ par['evento'] }} = r.{{ par['registro'] }}
            {% if not loop.last %}and{% endif %}
            {% endfor %}
            and safe_cast(r.{{ data_registro }} as date) <= e.{{ data_evento }}
    qualify row_number() over (
        partition by e.{{ id_evento }}
        order by r.{{ data_registro }} desc, r.{{ id_registro }} desc
    ) = 1
)
{% endmacro %}
