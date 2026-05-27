# Encaminhamentos CRAS/CREAS — RMA

> Dados extraídos de `rj-smas-dev.relatorio.encaminhamentos_unidade`  
> Data de geração: 30/04/2026  
> Fonte: Prontuário Carioca (Acolherio) via dbt — arquitetura Medallion

---

## Resumo por Unidade

| Unidade | Encaminhamentos para CRAS | Encaminhamentos para CREAS |
|---------|:------------------------:|:--------------------------:|
| CRAS ACARI | — | 10 |
| CRAS HELENICE NUNES JACINTO | — | 1 |
| CRAS JOSÉ CARLOS CAMPOS | — | 2 |
| CRAS MACHADO DE ASSIS | — | 4 |
| CRAS MARCELO CARDOSO TOMÉ | — | 3 |
| CRAS PROF. DARCY RIBEIRO | — | 10 |
| CRAS PROF. ISMÊNIA DE LIMA MARTINS | — | 16 |
| CRAS ROSANI CUNHA | — | 6 |
| CRAS SOBRAL PINTO | — | 9 |
| CRAS VILA MORETTI | — | 8 |
| CREAS MARIA LINA DE CASTRO LIMA | 25 | — |
| CREAS STELLA MARIS | 2 | — |

---

## Notas Técnicas

- **Metodologia**: Contagem de usuários distintos (`count distinct id_usuario_sk`) por unidade de origem, filtrando pelo campo de encaminhamento nas evoluções.
- **Fonte**: `fct_evolucoes` (UNION ALL de `raw_evolucoes_administrativas`, `raw_evolucoes_familias`, `raw_evolucoes_usuarios`).
- **Regex aplicado**:
  - Para CRAS: `Encaminhamentos - SMAS:` ou `Encaminhamentos - Atividades SMAS:`
  - Para CREAS: `Encaminhamentos Órgãos:`
- **Exclusões aplicadas**:
  - Usuários com nome contendo `%TESTES%` foram removidos.
  - Apenas as 12 unidades listadas acima foram consideradas.
- **Diferença de nomenclatura**: O modelo normaliza nomes para fazer o match. Exemplo:
  - Lista RMA: `CRAS HELENICE NUNES JACINTO` → no banco: `CRAS PROFESSORA HELENICE NUNES JACINTHO`
  - Lista RMA: `CRAS SOBRAL PINTO` → no banco: `CRAS DOUTOR SOBRAL PINTO`

---

## Total de Famílias em Acompanhamento PAIF

> Famílias ativas no serviço PAIF (`seqservassist = 1` e `datcancel IS NULL`).  
> Vinculação com unidade feita pelo operador que realizou o cadastro.

| Unidade | Total de Famílias PAIF |
|---------|:----------------------:|
| CRAS ACARI | 82 |
| CRAS PROF. DARCY RIBEIRO | 44 |
| CRAS HELENICE NUNES JACINTO | 3 |
| CRAS MACHADO DE ASSIS | 1 |
| CRAS ROSANI CUNHA | 1 |
| CRAS VILA MORETTI | 1 |
| CREAS MARIA LINA DE CASTRO LIMA | 1 |
| CRAS JOSÉ CARLOS CAMPOS | — |
| CRAS MARCELO CARDOSO TOMÉ | — |
| CRAS PROF. ISMÊNIA DE LIMA MARTINS | — |
| CRAS SOBRAL PINTO | — |
| CREAS STELLA MARIS | — |

**Total geral de famílias PAIF ativas no sistema:** 376

---

## Total de Famílias em Acompanhamento PAEFI

> Famílias ativas no serviço PAEFI (`seqservassist = 6` e `datcancel IS NULL`).

| Unidade | Total de Famílias PAEFI |
|---------|:----------------------:|
| CREAS MARIA LINA DE CASTRO LIMA | 18 |
| CREAS STELLA MARIS | 10 |
| CRAS ACARI | 1 |
| Demais unidades da lista | — |

---

## 3 Vulnerabilidades Mais Registradas

> Dados extraídos de `gh_famil_vulnerab` (vulnerabilidades ativas, `datcancel IS NULL`).  
> **Filtro aplicado:** Apenas as 12 unidades do piloto.

| Rank | Vulnerabilidade | Total de Famílias |
|------|-----------------|:-----------------:|
| 1º | Sem acesso à renda | 208 |
| 2º | Desemprego | 153 |
| 3º | Acesso precário/nulo a cond. habitacionais e sanitárias | 66 |

---

## 3 Violações de Direito Mais Registradas

> Dados extraídos de `dim_usuarios` (campo `violacoes` — struct array).  
> **Filtro aplicado:** Usuários com atendimento nas 12 unidades do piloto.

| Rank | Violação de Direito | Total Usuários |
|------|---------------------|:--------------:|
| 1º | Negligência e abandono | 54 |
| 2º | Outras Violações de Direitos | 49 |
| 3º | Auto Negligência | 20 |

---

## Total de Usuários Atendidos em Situação de Rua

> Dados extraídos de `dim_usuarios` (flag `flag_situacao_rua = 'Sim'`).  
> **Filtro aplicado:** Usuários com atendimento nas 12 unidades do piloto.

| Indicador | Total |
|-----------|:-----:|
| Usuários em situação de rua | 794 |

---

## Total de Famílias em Medidas Socioeducativas (MSE)

> Dados extraídos de `gh_famil_servassist` (serviços ativos, `datcancel IS NULL`).  
> **Filtro aplicado:** MSE = IDs 8 (MSE/LA) e 9 (MSE/PSC), nas 12 unidades do piloto.

| Unidade | Total de Famílias MSE |
|---------|:---------------------:|
| CREAS STELLA MARIS | 54 |
| CREAS MARIA LINA DE CASTRO LIMA | 12 |
| Demais unidades da lista | — |

---

## Observação

Modelo criado sob demanda ad-hoc na nova arquitetura Medallion (`marts/rma_cras/encaminhamentos_unidade`).  
Os dados refletem o histórico completo disponível na camada raw.
