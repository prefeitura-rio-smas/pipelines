# Filtro de e-mail do Prontuário Eletrônico

O filtro limita os registros exibidos no Looker Studio às unidades que o visualizador está autorizado a consultar. A regra é aplicada na consulta ao BigQuery antes de os dados chegarem aos gráficos.

## De onde vêm as permissões

Cada unidade tem uma lista de destinatários formada pelo e-mail da CAS, pelo e-mail cadastrado da unidade e pelos endereços adicionais da planilha `filtro_email_dev`. O pipeline padroniza os endereços, elimina duplicatas e grava a lista no campo `email_filtro`.

Inclusões e revogações feitas na planilha passam a valer quando a tabela `planilha_unificada` é atualizada.

## Como o filtro funciona

1. O Looker Studio envia o e-mail do visualizador à consulta personalizada pelo parâmetro `@DS_USER_EMAIL`.
2. A consulta verifica se o endereço completo está na lista de destinatários de cada registro. Registros sem unidade identificada ou sem destinatário correspondente ficam fora do resultado.
3. A consulta retorna os registros autorizados e recalcula os totais de atendimentos e evoluções com base no que o visualizador pode ver.

## Exemplo no Centro POP

Uma pessoa recebeu dois atendimentos na Unidade A e três na Unidade B.

| Acesso do visualizador | Resultado no relatório |
|---|---|
| Somente Unidade A | Dois atendimentos da Unidade A |
| Unidades A e B | Cinco atendimentos das duas unidades |
| Nenhuma das duas unidades | Nenhum atendimento dessa pessoa |

A regra também se aplica aos perfis por unidade e às entradas do histórico mensal do Centro POP.

## Situação da validação

A regra de isolamento foi testada no BigQuery em uma versão anterior da consulta. A validação no relatório publicado está pendente. Para concluí-la, é preciso confirmar que os gráficos protegidos usam a consulta personalizada com `DS_USER_EMAIL` habilitado e repetir os cenários com contas autenticadas.

O acesso direto à tabela segue as permissões do BigQuery.

[Documentação técnica e evidências de validação](prontuario_consumo_seguro.md)
