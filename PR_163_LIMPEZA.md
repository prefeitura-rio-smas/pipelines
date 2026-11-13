# PR 163 — Limpeza do prontuário eletrônico

## Objetivo

Reduzir código auxiliar do PR e simplificar as dependências das marts de `prontuario_eletronico`, preservando os nomes das colunas publicadas no BigQuery.

Implementação registrada no commit `85b701c`, enviado para a branch `staging/planilha-unificada-usuarios` do PR 163.

## O que foi feito

- Removida a macro `pu_cpop_historico_publico`, sem chamadas encontradas no repositório.
- Removidos os 18 testes SQL adicionados pelo PR em `queries/tests/prontuario_eletronico`.
- Removidas as macros de testes correspondentes e suas referências nos YAMLs. As macros de transformação e os testes preexistentes foram preservados.
- Removida a operação `fechar_centro_pop_mes`, conforme decisão explícita de retirar também testes e operação auxiliares.
- Atualizada a descrição do modelo de snapshots para retirar a referência à operação removida.
- Removidas as macros `pu_ref`, `pu_programada` e `pu_filtro_email`. Suas chamadas foram substituídas pelos `ref()` e `source()` do caminho padrão.

O commit alterou 49 arquivos, com 73 linhas adicionadas e 1.626 removidas.

## O que foi preservado

O rastreamento estático encontrou dependências diretas ou indiretas das quatro marts para todos os modelos SQL adicionados pelo PR. Por isso, nenhum desses modelos foi excluído.

Foram preservados os modelos mensais, os snapshots e a leitura do histórico fechado. Não foram executadas exclusões ou escritas no BigQuery.

Não foram renomeadas colunas nem alteradas as projeções públicas. A alteração não teve como objetivo modificar tipos, campos aninhados, aliases físicos, datasets, materializações, granularidade ou regras de negócio.

## Mudanças operacionais

- O repositório deixa de oferecer a operação de criação de novos fechamentos mensais do Centro POP; a leitura do histórico existente permanece.
- As validações específicas removidas deixam de executar no dbt. Essa remoção foi autorizada e não significa que seus controles deixaram de ser relevantes.
- Os redirecionamentos por `pu_fontes`, `pu_referencia` e `pu_base_schema` deixam de existir. A resolução das fontes passa a usar o caminho padrão do projeto.

## Validação realizada

- `dbt parse` com target `ci` e `schema_id: cleanup163`: concluído com sucesso, com avisos de depreciação.
- `git diff --check`: concluído sem erros.
- Inspeção estática das dependências e das referências afetadas.

Não foram executados compile, comparação de SQL compilado, consultas de equivalência ou comparação de schemas no BigQuery. O parse não comprova equivalência funcional dos dados. Os checks remotos do commit de limpeza não foram verificados nesta entrega.

## Descrição do PR

A descrição remota do PR não foi alterada. Ela ainda precisa ser ajustada para retirar referências às consultas autenticadas do Looker removidas anteriormente e às validações/operações retiradas nesta limpeza. As consultas `looker_assunto_*.sql` e o parâmetro `@DS_USER_EMAIL` não foram encontrados no repositório durante a revisão.

## Retirada do enriquecimento do CadÚnico

- Removido o modelo `int_usuarios_documentos_cadunico` e seu join na mart principal.
- RG e número da certidão passam a usar os campos do cadastro do prontuário.
- Datas de atualização/limite e as quatro flags de ambiguidade permanecem como STRING vazia, preservando nomes, tipos e posição.
- Preservado `int_cadunico_membros_familia`, consumido por `cd_cidadao_pac`.
- A troca de origem pode alterar os valores dos documentos; não implica equivalência dos dados.

Validação desta alteração: parse, lint SQL e `git diff --check` passaram. A tentativa de compile foi bloqueada pela necessidade de reautenticação das credenciais locais do Google Cloud. Não houve comparação de schemas ou dados no BigQuery.

## Correção dos checks de lint e security

No commit `b36d81b`, foram corrigidas três violações de layout SQL e atualizada a dependência `opentelemetry-api` de `1.45.1` para `1.45.2`. O lint dos arquivos afetados e a auditoria local de dependências passaram.
