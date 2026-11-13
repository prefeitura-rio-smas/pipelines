## Histórico mensal Centro POP

`centro_pop.historico_mensal` preserva uma linha por cidadão na planilha, com entradas por competência/unidade desde novembro de 2026. As fontes auxiliares BI e suas consultas foram retiradas do pacote.

A prévia contém todo cidadão com histórico na unidade, inclusive com zero eventos no mês. Atendimentos, evoluções e participações são contados separadamente; formulários não criam atendimentos adicionais. O perfil inclui ocorrências até o período, usando apenas o vínculo explícito da unidade. Datas ausentes não recebem mês de ocorrência artificial. O estado cadastral da origem é observado na captura, sem promessa de reconstrução de alterações antigas.

Fechar manualmente somente após o fim da competência. No mesmo target/schema de publicação, executar `dbt build --select tag:centro_pop_mensal --vars '{centro_pop_mes: "2026-11-01"}'` e conferir os testes. Em seguida, `dbt run-operation fechar_centro_pop_mes --args '{mes: "2026-11-01", id_fechamento: "centro-pop-202611-v1"}'`. Repetir o mesmo identificador é idempotente. Para corrigir, usar outro identificador e `nova_versao: true`; versões anteriores ficam preservadas. Concorrência é controlada por uma transação com atualização de uma linha compartilhada; caso haja conflito, repetir a operação com o mesmo identificador.

Após fechar, reconstruir a prévia corrente e a planilha com `dbt build --select tag:centro_pop_mensal prontuario_eletronico` sem a variável de competência. As reconstruções diárias não removem snapshots. A lista mostra a última versão publicada de cada competência; auditoria das anteriores na tabela `centro_pop_snapshots`. Usar o target de staging; não executar full refresh manual da tabela de snapshots.
