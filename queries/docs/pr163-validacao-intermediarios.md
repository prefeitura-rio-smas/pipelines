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

## Arquitetura final: dimensões e supertabela

Os dois modelos de mart Centro POP e o intermediário mensal foram removidos.
O fluxo permanece intermediários → dimensões → supertabela. O Looker deverá
consultar `planilha_unificada`, aplicar filtros por unidade, período e tipo de
atendimento e calcular agregações, evitando multiplicar eventos ao expandir arrays.
A configuração do Looker será migrada em etapa própria, fora deste repositório.
As tabelas físicas antigas foram preservadas temporariamente; não serão mais
reconstruídas pelo dbt. As validações mensais acima são evidências históricas,
não contratos da arquitetura final. Cobertura das evoluções permanece obrigatória.

Validação final após remover as marts: três tabelas e 17 testes aprovados.
Comparação integral antes/depois: 592938 linhas da supertabela e 1528 linhas da
dimensão Centro POP, com zero adições, remoções ou alterações de conteúdo.

## Datas da ocorrência

Formulários passam a expor `data_ocorrencia`, `origem_data_ocorrencia` e
`situacao_data_ocorrencia`. As datas originais continuam disponíveis.
O seed `formularios_datas_ocorrencia` define títulos e campos elegíveis; não há
inferência por texto livre, nascimento ou agendamento. Datas preenchidas inválidas
ou conflitantes levam à data da fonte. Na locação, os dois marcos mapeados devem
concordar; não se escolhe arbitrariamente entre assinatura e entrada.
O perfil Centro POP ordena pela ocorrência, registro original, resposta e ID.
Uma evolução pode conter formulários com ocorrências distintas.

Validação das datas: três tabelas e 18 testes aprovados, mais quatro testes de
parser/mapeamento. As contagens permaneceram em 592938 usuários, 1017861
atendimentos, 663544 evoluções e 398571 formulários. 9212 formulários usam uma
data informada válida; 5614 têm ocorrência anterior à data da fonte e 2874
passam a ter mês de ocorrência diferente. Foram sinalizados 50 valores inválidos
com alternativa da fonte; nenhum conflito de datas apareceu nesta base.
A seleção do perfil mudou para três dos 1528 usuários Centro POP, sem mudança
de população. Eventos e conteúdo original dos formulários passaram nos testes
de preservação. A tabela publicada não foi substituída.
