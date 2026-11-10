# Convenção do prontuário eletrônico

O produto usa o nome `prontuario_eletronico` no repositório: modelo principal e pastas da mart, macros e testes. Intermediários permanecem organizados por domínio.

Nomes físicos do BigQuery são preservados por aliases explícitos:
- `prontuario_eletronico` → `planilha_unificada`.
- `centro_pop` → `dim_centro_pop`.
- `creas` → `dim_creas`.
- `alta_complexidade` → `dim_alta_complexidade`.

Reorganizações de arquivos não devem alterar datasets, nomes físicos, materializações ou regras. Alterar nomes físicos exige pedido explícito do usuário. Os três modelos de escopo continuam sendo dimensões; manter as tags existentes para compatibilidade.
