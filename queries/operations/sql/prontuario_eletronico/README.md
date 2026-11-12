## Histórico mensal Centro POP

`centro_pop.historico_mensal` preserva uma linha por cidadão na planilha, com entradas por competência/unidade desde novembro de 2026. As fontes auxiliares BI e suas consultas foram retiradas do pacote.

A prévia contém todo cidadão com histórico na unidade, inclusive com zero eventos no mês. Atendimentos, evoluções e participações são contados separadamente; formulários não criam atendimentos adicionais. O perfil inclui ocorrências até o período, usando apenas o vínculo explícito da unidade. Datas ausentes não recebem mês de ocorrência artificial. O estado cadastral da origem é observado na captura, sem promessa de reconstrução de alterações antigas.

Fechar manualmente somente após o fim da competência. No mesmo target/schema de publicação, executar `dbt build --select tag:centro_pop_mensal --vars '{centro_pop_mes: "2026-11-01"}'` e conferir os testes. Em seguida, `dbt run-operation fechar_centro_pop_mes --args '{mes: "2026-11-01", id_fechamento: "centro-pop-202611-v1"}'`. Repetir o mesmo identificador é idempotente. Para corrigir, usar outro identificador e `nova_versao: true`; versões anteriores ficam preservadas. Concorrência é controlada por uma transação com atualização de uma linha compartilhada; caso haja conflito, repetir a operação com o mesmo identificador.

Após fechar, reconstruir a prévia corrente e a planilha com `dbt build --select tag:centro_pop_mensal prontuario_eletronico` sem a variável de competência. As reconstruções diárias não removem snapshots. A lista mostra a última versão publicada de cada competência; auditoria das anteriores na tabela `centro_pop_snapshots`. Usar o target de staging; não executar full refresh manual da tabela de snapshots.

## Filtro transversal por assunto

As consultas `looker_assunto_*.sql` leem somente `abordagem.planilha_unificada`. Habilitar o parâmetro nativo `DS_USER_EMAIL` e o consentimento do visualizador no conector BigQuery do Looker Studio. O e-mail não deve ser um parâmetro editável. Cada consulta compara o endereço completo com os destinatários do próprio registro. E-mail vazio, nulo ou desconhecido retorna zero linhas.

Usuario e famílias são dados comuns do cidadão autorizado naquele assunto. As consultas não expandem as listas universais: os campos de família e listas cadastrais são apresentados como resumos textuais. Não somar `familias_quantidade` nem tratar a lista `familias_ids` como uma chave individual de família. Para cidadãos, contar `id_usuario` distinto. Para eventos, respeitar a chave do assunto; profissionais e formulários são texto para preservar uma linha por evento. Perfis Centro POP/CREAS têm uma linha por cidadão/unidade, ciclos por cidadão/ciclo/unidade, projetos por cidadão/projeto/unidade e histórico mensal por cidadão/unidade/competência.

As permissões provêm de CAS, unidade e planilha da unidade de origem. Os modelos de apoio são ephemeral na intermediate, sem tabelas prontuario_bi. O campo email da raiz e os resumos escalares antigos são mantidos para compatibilidade; não são usados como autorização pelas consultas. A conexão direta à tabela original não executa automaticamente estes filtros. Migrar as fontes dos gráficos para as consultas por assunto antes de considerar o isolamento ativo no relatório.

`centro_pop.registros` e `creas.registros` selecionam os perfis dentro da unidade antes de consolidar respostas. `alta_complexidade.registros` contém os ciclos. Os e-mails do histórico mensal são metadados recalculados na publicação, sem alterar o conteúdo fechado dos snapshots. Uma mudança de permissões passa a valer nas consultas após atualizar a tabela consolidada.
