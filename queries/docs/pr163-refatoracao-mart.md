# Refatoração PR 163 — estado parcial

Baseline: e8df8e74dff5878aa8b97fd9539c9ae40cfae057; base: 04129cb57a9544b7ffb6e271bd7e502365b4b50d.

## Implementado

Somente prontuario_eletronico.sql foi modificado: JOIN de permissões incorporado às agregações de perfis por unidade Centro POP e CREAS; CTE trivial unidades substituída pela mesma referência direta. Nenhum intermediate foi enriquecido ou criado.

| Métrica | Antes | Depois |
|---|---:|---:|
| Linhas mart principal | 634 | 614 |
| CTEs mart principal | 16 | 13 |
| Linhas quatro marts | 700 | 680 |
| Modelos mart | 4 | 4 |
| SQL adicionado/removido PR completo | +4530 / -677 | +4510 / -677 |

## Reutilização preservada

- Documentos: intermediate/cadunico/int_usuarios_documentos_cadunico.sql.
- Endereço: intermediate/core/int_usuarios_enderecos.sql.
- Famílias: intermediate/social/int_usuarios_familias.sql.
- Atendimentos: intermediate/atendimentos/int_atendimentos_por_usuario.sql.
- Evoluções: intermediate/core/int_evolucoes_por_usuario.sql (evento, não usuário).
- Formulários: intermediate/formularios/int_formularios_por_usuario.sql.
- Perfis por unidade: intermediate/centro_pop/int_perfil_centro_pop_por_unidade.sql e intermediate/creas/int_perfil_creas_por_unidade.sql.
- Permissões: intermediate/seguranca/int_filtro_email_unidades.sql.

As fusões preservam os campos projetados, ordem dos campos e arrays, GROUP BY e LEFT JOIN originais. Isso é evidência estática, não comprovação de equivalência sobre dados.

## Validação

- dbt Core 1.12.5 parse: passou; avisos preexistentes de propriedades YAML obsoletas e configuração sem recursos.
- git diff --check: passou.
- dbt compile --select prontuario_eletronico: passou (exit code 0); log em /tmp/pr163-compile-result. Compilação não valida equivalência dos dados.
- Build isolado, comparação recursiva de schema, população, valores, cardinalidades e regressões: pendentes.
- Nomes, tipos, granularidade e estruturas: intenção preservada por inspeção estática; confirmação dinâmica pendente.
- Configurações de produção e intermediates: sem alterações.

## Trabalho restante

Remover a projeção intermediária base e reconstruções redundantes de arrays, fundir alta_unidades/alta_registros, executar baseline e candidato com entradas congeladas e destinos isolados, comparar saídas e executar testes dos consumidores. Não considerar esta entrega concluída ou apta a publicação sem essas verificações.
