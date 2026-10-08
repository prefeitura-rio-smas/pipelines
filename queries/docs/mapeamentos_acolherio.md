# Mapeamentos cadastrais do Acolherio

## Situação de rua

`map_flag_situacao_rua` interpreta o campo `indmoradi`:

| Código | Resultado |
| --- | --- |
| 5 | Sim |
| 1–4, 6–9, A–D | Não |
| E (Desconhecido), ausência ou código fora do domínio | NULL |

Normaliza espaços, caixa dos códigos alfabéticos e representações inteiras
dos códigos numéricos. Por exemplo, `05` indica Sim e ` a ` indica Não.
Os códigos A–D também representam tipos de moradia; letras não são
automaticamente respostas inválidas. Números fora da lista conhecida
também não constituem uma resposta negativa.

## Decisão apoiada

`map_coluna_decisao_apoiada` indica o preenchimento cadastral do campo
`numero_processo_decisao_apoiada`, originado de `dsctomdecproces`:

- NULL, string vazia ou somente espaços: N.
- Valor preenchido: S.

O indicador registra a presença de conteúdo no cadastro. A validade do
número do processo e a confirmação formal da decisão exigem conferência
do documento correspondente. O valor cadastral original continua disponível.

## Escolaridade

`map_coluna_escolaridade` interpreta `indescolari`, incluindo:

- 09: Especialização.
- 10: Mestrado.
- 11: Doutorado.

O campo de série escolar é `indserie` e tem outro domínio. Não foi
encontrada a descrição do código 4 de série no catálogo consultado;
esse código continua sem tradução, com o valor bruto preservado na raw.

## Referência do catálogo

Os códigos de moradia e os códigos de escolaridade acima foram conferidos
em `rj-smas-dev.governance.dominios_acolherio`. As fontes registradas são
`moradia.ini`, `escolaridade.ini` e `escolaridade_bc26.ini`.

As entradas consultadas têm `confidence=inferred` e `last_verified` vazio.
A confirmação formal das regras do sistema continua dependendo da origem.
