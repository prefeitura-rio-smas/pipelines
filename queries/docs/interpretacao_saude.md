# Como as respostas de saúde são interpretadas

A regra fica diretamente em [mart_planilha_unificada_centro_pop.sql](../models/marts/centro_pop/mart_planilha_unificada_centro_pop.sql), nos campos de situação de saúde e local de tratamento.

## Regra

O modelo remove espaços nas pontas e converte o texto para minúsculas antes de comparar.

| Resposta | Interpretação |
| --- | --- |
| `NULL`, vazio, `undefined`, `null` ou `-` | Desconhecido |
| Não informado, não informada, não sabe, não soube informar, sem informação ou não se aplica | Desconhecido |
| `N`, `nao` ou `não` | Negativa explícita |
| Qualquer outro texto, como Diabetes ou Clínica da Família | Evidência positiva |

Os marcadores de desconhecido da tabela também são reconhecidos sem acentos. Por exemplo, ` Não Informado ` e `nao informado` são tratados como desconhecidos.

## 1. A pessoa tem ou teve problema de saúde?

Em `situacao_saude_historico_mes`, um `CASE` transforma cada resposta em `TRUE`, `FALSE` ou `NULL`. O modelo considera os formulários conhecidos até o fechamento do mês e agrega as respostas com `LOGICAL_OR`:

```sql
logical_or(case
    when r.situacao_saude is null or lower(trim(r.situacao_saude)) in (
        '', 'undefined', 'null', 'não informado', 'nao informado',
        'não informada', 'nao informada', 'não sabe', 'nao sabe',
        'não soube informar', 'nao soube informar',
        'sem informação', 'sem informacao', '-', 'não se aplica', 'nao se aplica'
    ) then null
    when lower(trim(r.situacao_saude)) in ('n', 'nao', 'não') then false
    else true
end) as possui_historico_problema_saude
```

`LOGICAL_OR` ignora valores `NULL`. Uma resposta positiva torna o resultado positivo; se houver somente negativas e valores desconhecidos, o resultado é negativo. Se todos forem desconhecidos, o resultado é `NULL`.

| Histórico até o fechamento | Resultado da agregação |
| --- | --- |
| Diabetes → Não | `TRUE` |
| Não → Não informado | `FALSE` |
| Não informado → Não sabe | `NULL` |

Isso permite responder “tem ou teve”: uma negativa posterior não apaga uma evidência positiva anterior. A coluna final `flag_problema_saude` também considera o indicador cadastral de saúde mental; essa regra adicional fica no modelo.

## 2. A pessoa faz acompanhamento de saúde?

A coluna `flag_acompanhamento_saude` usa o local de tratamento do último formulário conhecido até o fechamento do mês. Seu `CASE` aplica a mesma lista de marcadores:

| Local de tratamento | Resultado |
| --- | --- |
| Clínica da Família | Sim |
| Não | Não |
| Não sabe, vazio ou `NULL` | Não Informado |

## Limite da interpretação

A regra considera positivo todo texto que não esteja nas listas de respostas desconhecidas e negativas. Atualmente, “sem problemas” e “não faz tratamento” também seriam positivos. O SQL não interpreta frases: sua aplicação depende das respostas admitidas pelo formulário.

Se um novo marcador precisar ser reconhecido, os dois `CASE` devem ser atualizados conforme os valores reais da fonte e o significado de cada campo.
