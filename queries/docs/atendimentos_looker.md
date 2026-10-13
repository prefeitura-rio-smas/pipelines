# Atendimentos no Looker

O modelo `mart_atendimentos` materializa a tabela `atendimentos`: em produção,
`rj-smas.marts.atendimentos`; no alvo dev, `rj-smas-dev.relatorio.atendimentos`.
Não substitui a tabela legada `dev_atendimentos`.

## Grão e cobertura

Uma linha representa um atendimento do módulo família ou usuário, identificado
por `ID_ATENDIMENTO_MODULO`. O mart agrega os profissionais principais e
compartilhados em `PROFISSIONAIS`, sem multiplicar os atendimentos.

A base inclui cancelados, registros cujos nomes contêm `TESTE`, atendimentos sem
cadastros associados e usuários com vínculos familiares encerrados. A ausência
de nome, data ou profissional não elimina o atendimento. O mart não aplica filtros
pelos ranks de deduplicação.

`ID_ATENDIMENTO` é igual a `ID_ATENDIMENTO_MODULO`. `ID_ATENDIMENTO_SK` é o hash
desse identificador, independente dos profissionais. `COUNT(*)` conta atendimentos.
A mudança substitui o formato anterior atendimento-profissional de `ID_ATENDIMENTO`.

Os campos individuais `PROFISSIONAL_ID`, `PROFISSIONAL`, `PROFISSIONAL_CBO`,
`PROFISSIONAL_CBOS` e indicadores correspondentes representam o profissional
principal da origem. Se ele estiver ausente, permanecem ausentes, mesmo quando
houver compartilhados; não se escolhe um substituto arbitrário.

`PROFISSIONAIS` preserva todas as posições profissionais em structs com ID,
nome, CBOs, classificação, indicação de principal e flags cadastrais/de teste.
A posição do principal ausente é preservada com ID NULL. `QUANTIDADE_PROFISSIONAIS`
conta somente profissionais com ID preenchido. Códigos compartilhados inválidos,
zero, repetidos na lista ou iguais ao principal não geram posições adicionais.

`TIPO_ATENDIMENTO` consolida todos os profissionais: Técnico quando algum for
Técnico; senão Recepção quando algum for Recepção; senão Outros Atendimentos.
`NOME_ATENDIMENTO` mantém o resumo pelas regras do principal; a classificação e
os nomes individuais estão disponíveis dentro de `PROFISSIONAIS`.

## Filtros no Looker

| Campo | Uso |
| --- | --- |
| `FLAG_CANCELADO` | Excluir `S` para considerar apenas não cancelados; decidir explicitamente como tratar ausência. |
| `FLAG_USUARIO_TESTE`, `FLAG_UNIDADE_TESTE`, `FLAG_OPERADOR_TESTE`, `FLAG_PROFISSIONAL_TESTE` | Excluir registros de teste quando necessário. Identificação por nome, sem distinguir caixa. |
| `FLAG_*_CADASTRADO` | Identificar ausência de correspondência cadastral sem perder os IDs da origem. |
| `FLAG_ALGUM_PROFISSIONAL_TESTE` | Identificar teste em qualquer profissional, incluindo compartilhados. |
| `DATA_SAIDA_VINCULO_FAMILIAR` | Identificar vínculo resumido encerrado no módulo usuário. |
| `DATA_SAIDA_ATENDIMENTO` | Saída do próprio atendimento, diferente da saída familiar. |
| `atendimento_unico_rank` | Selecionar `1` para deduplicar atendimentos por usuário, unidade, data, hora e nome original, independente dos profissionais. Todas as posições ficam disponíveis. |
| `cbo_unico_rank` | Sempre 1 no grão atendimento; mantido por compatibilidade de nomes. |

Os filtros anteriores não representam uma configuração obrigatória do dashboard.
Cada análise escolhe a população adequada. Os ranks são calculados sobre a base
ampla; filtrá-los depois de outros atributos não recalcula sua numeração.

## Informações adicionais

As primeiras 39 colunas preservam os nomes e a ordem legados; o modelo tem 63 colunas e acrescenta
IDs da origem, indicadores de cobertura e teste, datas de saída e atributos completos.
O grão atendimento altera os totais e o significado dos IDs e ranks. A data pode permanecer ausente.

`PROFISSIONAL_CBOS` contém os códigos e descrições do principal em um array de structs.
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
módulo, unicidade do identificador natural e cobertura dos atendimentos e profissionais
da origem.
O enriquecimento deve preservar todos os atendimentos de `int_atendimentos_completos`,
com todas as suas posições profissionais dentro de `PROFISSIONAIS`.
O fato `fct_atendimentos` usa essa mesma base, mantendo suas colunas e filtros
anteriores para os consumidores existentes.
