# Convenção do prontuário eletrônico

O produto usa o nome `prontuario_eletronico` no repositório: modelo principal e pastas da mart, macros e testes. Intermediários permanecem organizados por domínio.

Nomes físicos do BigQuery são preservados por aliases explícitos:
- `prontuario_eletronico` → `planilha_unificada`.
- `centro_pop` → `dim_centro_pop`.
- `creas` → `dim_creas`.
- `alta_complexidade` → `dim_alta_complexidade`.

Reorganizações de arquivos não devem alterar datasets, nomes físicos, materializações ou regras. Alterar nomes físicos exige pedido explícito do usuário. Os três modelos de escopo continuam sendo dimensões; manter as tags existentes para compatibilidade.

A mart prontuario_eletronico contém somente prontuario_eletronico, centro_pop, creas e alta_complexidade, com nomes simples e YAMLs correspondentes. Apoios mensais ficam em intermediate/centro_pop com prefixo int_, preservando aliases e configurações. Não recriar modelos prontuario_bi. Consultas autenticadas por assunto leem somente a tabela original e filtram o email do registro; usuario e familias são universais no contexto de cidadãos autorizados. Os perfis por unidade são apoios ephemeral na intermediate. Os três escopos da mart ficam nas subpastas homônimas.
