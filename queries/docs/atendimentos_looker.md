# Atendimentos no Looker

O modelo `mart_atendimentos` materializa a tabela `atendimentos`: em produção,
`rj-smas.marts.atendimentos`; no alvo dev, `rj-smas-dev.relatorio.atendimentos`.
Não substitui a tabela legada `dev_atendimentos`.

## Grão e cobertura

Uma linha representa um atendimento do módulo família ou usuário, associado
ao profissional principal ou a um profissional compartilhado. O profissional
principal ausente também tem uma linha. A lista de compartilhados é normalizada:
códigos inválidos, zero, repetições e repetição do principal não geram novas linhas.

A base inclui cancelados, registros cujos nomes contêm `TESTE`, atendimentos sem
cadastros associados e usuários com vínculos familiares encerrados. A ausência
de nome, data ou profissional não elimina o atendimento. O mart não aplica filtros
pelos ranks de deduplicação.

Use `ID_ATENDIMENTO_SK` como identificador único da linha. Ele inclui o módulo
e admite profissional ausente. `ID_ATENDIMENTO` é mantido no formato legado
atendimento-profissional; pode ser nulo e não diferencia os módulos.
Para contar atendimentos sem multiplicar profissionais compartilhados, use
`COUNT(DISTINCT ID_ATENDIMENTO_MODULO)`.

## Filtros no Looker

| Campo | Uso |
| --- | --- |
| `FLAG_CANCELADO` | Excluir `S` para considerar apenas não cancelados; decidir explicitamente como tratar ausência. |
| `FLAG_USUARIO_TESTE`, `FLAG_UNIDADE_TESTE`, `FLAG_OPERADOR_TESTE`, `FLAG_PROFISSIONAL_TESTE` | Excluir registros de teste quando necessário. Identificação por nome, sem distinguir caixa. |
| `FLAG_*_CADASTRADO` | Identificar ausência de correspondência cadastral sem perder os IDs da origem. |
| `DATA_SAIDA_VINCULO_FAMILIAR` | Identificar vínculo resumido encerrado no módulo usuário. |
| `DATA_SAIDA_ATENDIMENTO` | Saída do próprio atendimento, diferente da saída familiar. |
| `atendimento_unico_rank` | Selecionar `1` quando a análise desejar deduplicar pelo agrupamento legado. Todas as posições ficam disponíveis. |
| `cbo_unico_rank` | Rank por atendimento do módulo × profissional; não é aplicado como filtro no dbt. |

Os filtros anteriores não representam uma configuração obrigatória do dashboard.
Cada análise escolhe a população adequada. Os ranks são calculados sobre a base
ampla; filtrá-los depois de outros atributos não recalcula sua numeração.

## Informações adicionais

As primeiras 39 colunas preservam os nomes e a ordem legados; o modelo acrescenta
IDs da origem, indicadores de cobertura e teste, datas de saída e atributos completos.
O grão mais amplo altera os totais e permite `ID_ATENDIMENTO` e data ausentes.

`PROFISSIONAL_CBOS` contém todos os códigos e descrições em um array de structs.
`PROFISSIONAL_CBO` e `PROFISSIONAL_CBO_ORIGINAL` continuam como resumos do primeiro
CBO traduzível ordenado por código. `PROFISSIONAL_NOME_CADASTRAL` preserva o nome
anterior à substituição pelo operador em profissionais genéricos.

`VINCULOS_FAMILIARES` preserva todos os vínculos do usuário. O `ID_FAMILIA` resumido
usa a família do atendimento familiar ou um vínculo selecionado no módulo usuário:
prioriza aberto e, entre encerrados, a saída mais recente. Não reconstrói a família
na data do atendimento.

`PROJETOS_SOCIAIS` continua resumindo os projetos ativos e não cancelados da família
selecionada. `PROJETOS_SOCIAIS_HISTORICO` inclui também vínculos inativos e cancelados,
com as datas e indicadores da origem. Esses dados são um snapshot cadastral.

Os arrays preservam relações de um para muitos sem multiplicar o grão principal.
Ao usar `UNNEST` para analisar seus elementos, cuidado para não somar atendimentos
uma vez por CBO, vínculo ou projeto.

`EMAIL_PLANILHA` agrega todos os e-mails distintos da unidade em ordem alfabética.
`EMAIL` combina CAS, unidade e planilha; componentes ausentes não anulam os demais.
Os nomes de unidade ainda são associados por igualdade com a planilha.

## Proteção da cobertura

Os testes verificam unicidade e preenchimento de `ID_ATENDIMENTO_SK`, domínio do
módulo e igualdade de multiplicidade por chave entre a origem e o mart.
O enriquecimento deve preservar todas as linhas de `int_atendimentos_completos`.
O fato `fct_atendimentos` usa essa mesma base, mantendo suas colunas e filtros
anteriores para os consumidores existentes.
