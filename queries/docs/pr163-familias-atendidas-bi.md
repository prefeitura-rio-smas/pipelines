# Famílias atendidas na supertabela

Este bloco agora é compatibilidade temporária. Para novos cartões e migração,
usar o [campo direto no atendimento](pr163-familia-no-atendimento.md).

A fonte continua `rj-smas-dev.dashboard_acolherio.planilha_unificada`.
O novo bloco `atendimentos.familias_atendidas` reproduz as associações da
programada `dashboard_acolherio.dev_atendimentos`, sem depender de o profissional
existir na dimensão/fato novo. Não usar a lista cadastral `familias` para este indicador.

## Contrato

Um item por usuário, atendimento com módulo e família da referência. Repetições
idênticas por profissional são deduplicadas; famílias distintas no mesmo evento
são preservadas. Contextos conflitantes para a mesma chave falham no teste de
unicidade, sem seleção arbitrária. Família nula não cria uma associação.

Cada item contém os campos STRING `id_atendimento_modulo`, `id_familia`,
`data_atendimento` (AAAA-MM-DD), `nome_unidade`, `tipo_unidade`, `email` e
`elegivel_indicador` (S/N). Ausência de texto é representada por ''.

Elegível = tipo de unidade CRAS, CREAS ou CENTRO POP na referência e unidade
não nula/diferente de Negado. A lista guarda também as associações inelegíveis,
permitindo inspecionar a referência completa. O indicador exige filtrar S.
Os eventos de `atendimentos.registros`, as famílias cadastrais, evoluções,
formulários e suas quantidades permanecem com os contratos anteriores.

O e-mail é a composição da referência, não a identidade do construtor da tabela.
Acesso do visualizador e período continuam aplicados no BI, sobre os campos do
mesmo item do bloco novo. Não filtrar apenas por usuário que possua algum evento
permitido, pois isso liberaria também seus demais eventos.

## Mapeamento no Looker Studio

Na fonte existente, atualizar os campos para descobrir o array novo. No cartão:

| Configuração | Campo/regra |
|---|---|
| Métrica | Contagem distinta de `atendimentos.familias_atendidas.id_familia` |
| Data do período | `atendimentos.familias_atendidas.data_atendimento`, convertida em Data AAAA-MM-DD |
| Elegibilidade | `atendimentos.familias_atendidas.elegivel_indicador = S` |
| Acesso | Mesma configuração de identidade do relatório antigo, usando `atendimentos.familias_atendidas.email` |
| Campos para controles de unidade | `nome_unidade` e `tipo_unidade` do mesmo bloco |

A regra legada de acesso é `STRPOS(email, email_do_visualizador) > 0`, com e-mail
do visualizador obrigatório/não vazio e unidade diferente de Negado. O e-mail do
visualizador deve vir do mecanismo de identidade já configurado no relatório
antigo; não deve ser uma constante nem um parâmetro livre editável pelo leitor.
Usuário sem permissão ou sem identidade não deve receber linhas.

Todos os campos do indicador, controles e filtros devem referenciar esse array.
Combinar a família de `familias` com data/e-mail de `atendimentos.registros`
reintroduz o cruzamento entre históricos distintos que causou a divergência.
A edição do Looker Studio é uma etapa manual: não há ferramenta de edição do BI
conectada nesta sessão. Não é necessária uma fonte SQL separada para este cartão.

## Validação de 03/11/2026

Referência e tabela anterior congeladas em
`rj-smas-dev.ci_pr163_familias_20261103_200023`.
A candidata foi construída por adição do novo bloco à foto publicada, evitando
recalcular outros assuntos sobre fontes mais recentes.

Para 01/01/2026 a 02/11/2026 inclusive, na identidade do cartão antigo:

| Recorte | Programada congelada | Novo bloco |
|---|---:|---:|
| Todas as famílias | 259499 | 259499 |
| Unidades elegíveis | 258913 | 258913 |
| Acesso por e-mail, sem restrição de unidade | 255163 | 255163 |
| Indicador com ambos os filtros | 254585 | 254585 |

Comparação integral das associações, incluindo multiplicidade: zero diferenças.
Unicidade por usuário/evento/família: aprovada. A comparação integral dos campos
anteriores da supertabela retornou zero diferenças para 595064 usuários.
A diferença residual do caminho por profissional foi eliminada ao preservar a
associação diretamente da referência, independentemente do profissional do fato.

Testes executados no BigQuery: `validar_familias_atendidas_bi`,
`pu_familias_atendidas_unicas` e `pu_familias_atendidas_paridade` (3 aprovados).
Os casos sintéticos cobrem repetição por profissional, múltiplas famílias,
sequências iguais em módulos distintos, ausência de família/e-mail/unidade,
unidade inelegível, Negado, limites do período, dois visualizadores autorizados,
visualizador desconhecido/vazio e preservação de conflitos para diagnóstico.
E-mail ausente ou não autorizado também retornou zero linhas na candidata real.
O modelo completo compilou e o SQLFluff não encontrou violações nos SQL alterados.

Para repetir com referência congelada, usar target ci, schema_id do dataset de
validação, pu_referencia=true e pu_base_schema apontando para o mesmo dataset,
que deve conter `atendimentos_programada`. Sem essas variáveis, o modelo usa a
referência programada normal. A validação deve sempre comparar fontes do mesmo
instante; a programada possui escolhas por ROW_NUMBER sem desempate por família.

Publicação: criar snapshot da tabela de destino, copiar a candidata aprovada,
remover a expiração herdada do dataset de validação e comparar a cópia integral.
Se a validação falhar, restaurar o snapshot. O agendamento permanece pausado.
