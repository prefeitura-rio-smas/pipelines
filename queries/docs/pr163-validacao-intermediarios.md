# Validação dos intermediários — PR 163

Comparação em 29/10/2026 entre `00a50d6` e `a10c1c2`, no dataset isolado
`rj-smas-dev.ci_pr163_a10c1c2_compare`. As saídas refatoradas foram preservadas
com prefixo `new_`; a versão anterior foi reconstruída sobre as mesmas fontes.
Nenhuma tabela publicada foi substituída.

| Métrica | Versão anterior, mesmas fontes | Refatoração |
|---|---:|---:|
| Usuários | 592938 | 592938 |
| Atendimentos | 1017861 | 1017861 |
| Evoluções | 663544 | 663544 |
| Vínculos usuário/família | 563573 | 563573 |
| Linhas mensais | 1652 | 1652 |

Comparação do conteúdo completo por usuário: zero adições, remoções ou diferenças.
Comparação do multiconjunto de linhas completas da mart mensal: zero diferenças.
Construção refatorada: sete tabelas e 29 testes aprovados.

A publicação anterior tinha 584741 usuários, 1017864 atendimentos, 658176 evoluções,
555518 vínculos familiares e 1652 linhas mensais. Essas diferenças não são efeito
da refatoração comparada: ambas as versões produzem a mesma saída nas fontes
congeladas para esta validação. A publicação está defasada em relação a essas fontes.

O teste de cobertura inicialmente encontrou 2937 evoluções ausentes no intermediário
copiado. Após reconstrução de `int_evolucoes` no dataset isolado, passou.
Os testes de preservação da planilha verificam identidade e multiplicidade dos eventos,
e conteúdo dos formulários; a comparação integral acima complementa essa cobertura.

Na integração com main, o histórico de serviços passou a reutilizar o contrato
`servicos_historico` já existente na main, com nomes provenientes do seed de domínios.
Essa integração recebe validação própria após a resolução dos conflitos.

## Resultado após integração com main

Cinco testes unitários de vigência e dois testes de dados dos serviços passaram.
Planilha e mart mensal reconstruídas: duas tabelas e 11 testes aprovados.
Todos os totais acima permaneceram iguais. A mart mensal continuou integralmente
idêntica. A planilha mudou para 2889 usuários exclusivamente no campo
`nome_servico`: desconsiderando esse campo, zero diferenças nas linhas completas.
Essa alteração incorpora a taxonomia vigente da main, preservando seu seed e seu
contrato de histórico. Não houve mudança de população nem de quantidade de eventos.
