# A supertabela do Prontuário Eletrônico

A supertabela é uma **One Big Table (OBT)** que centraliza os dados do Prontuário em um único ponto de consulta. Ela reúne identificação, famílias, projetos sociais, atendimentos, evoluções, Centro POP, CREAS e acolhimentos de alta complexidade. O consumidor consulta esses assuntos na tabela `rj-smas-dev.abordagem.prontuario_eletronico`, sem precisar refazer as junções entre os domínios em cada relatório.

## Uma linha por cidadão e unidade

O grão da OBT é **cidadão por unidade**, identificado por `id_usuario_unidade`. Um cidadão com vínculos em mais de uma unidade aparece em mais de uma linha. Atributos cadastrais podem se repetir entre essas linhas. Os eventos e perfis com vínculo de unidade são reunidos no escopo correspondente.

Cada assunto ocupa um bloco. Uma `STRUCT` agrupa campos de um mesmo assunto, enquanto um `ARRAY` reúne os registros que podem se repetir, como atendimentos e evoluções. Assim, vários eventos podem caber em uma linha da OBT, cada qual com sua própria data e seus atributos. A contagem de cidadãos usa `id_usuario`. A contagem de atendimentos ou participações usa o grão definido para a coleção consultada.

## Como as dimensões alimentam a OBT

A arquitetura mantém a preparação dos dados nos modelos de cada domínio. As dimensões fornecem entidades de referência, como usuários, unidades e profissionais. Os demais modelos de domínio organizam vínculos, históricos e eventos antes da consolidação. A OBT combina esses resultados e aplica o contrato de publicação, incluindo a chave da linha, os blocos aninhados e o escopo por unidade. [Arquitetura dbt](../ARCHITECTURE.md) · [Modelo do Prontuário](../models/marts/prontuario_eletronico/prontuario_eletronico.sql)

## Proposta de atualização assíncrona

A proposta é permitir que dimensões e modelos de domínio sejam atualizados conforme suas próprias fontes e cadências. A publicação da OBT acontece em uma etapa posterior, que combina as versões concluídas de cada entrada.

1. **Atualizar as entradas.** Cada domínio processa sua carga e disponibiliza o resultado após suas verificações. Cadastro, unidades e eventos podem concluir em momentos diferentes.
2. **Montar a candidata.** A execução da OBT identifica as versões de entrada que vai usar, registra suas referências de carga e monta uma candidata isolada. Se faltar uma entrada exigida, a publicação aguarda.
3. **Validar e publicar.** A candidata passa por verificações de esquema, chave, população e preservação de eventos. Após a aprovação, a troca é atômica, com snapshot para recuperação.

Por exemplo, uma nova carga da dimensão de usuários pode terminar pela manhã e uma carga de atendimentos, à tarde. Na próxima publicação, a OBT combina as versões concluídas conforme a regra de corte. A hora de publicação indica quando o conjunto ficou disponível. A referência de cada entrada indica a atualidade dos dados daquele domínio.

## O que já existe e o que a proposta acrescenta

O daily atual já captura as entradas previstas para a execução, reconstrói dependências em um dataset isolado, roda os controles do Prontuário e publica a candidata com snapshot. A proposta amplia esse desenho ao desacoplar as cadências de atualização das dimensões e modelos de domínio da etapa de publicação da OBT. A regra de corte e o registro das versões por entrada são partes da proposta. [Rotina atual de publicação](../validation/publicar_prontuario_dev.md)

## Documentação do contrato

- [Ordem dos blocos e campos publicados](ordem-campos-prontuario.md)
- [Campos cadastrais da STRUCT usuario](ordem-struct-usuario.md)
- [Rastreio por tabela, coluna e regra de cálculo](rastreio-origem-colunas-prontuario.md)
