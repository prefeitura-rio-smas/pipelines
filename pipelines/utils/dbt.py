"""Helpers compartilhados para execução de dbt (resumo e mensagens de erro)."""
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import NodeStatus

STATUS_FALHA = (NodeStatus.Fail, NodeStatus.Error, NodeStatus.RuntimeErr)


def run_dbt(args: list[str]) -> dbtRunnerResult:
    """Executa o dbt via dbtRunner e retorna o resultado."""
    return dbtRunner().invoke(args)


def nodes_falhados(result: dbtRunnerResult) -> list:
    """RunResults com status de falha (fail/error/runtime error)."""
    if result.exception or not result.result:
        return []
    return [r for r in result.result.results if r.status in STATUS_FALHA]


def resumo_markdown(result: dbtRunnerResult, comando: str = "build") -> str:
    """Markdown com contagem por status e detalhes dos nós não-sucedidos."""
    results = (result.result.results if result.result else None) or []
    contagem = {}
    for r in results:
        contagem[r.status.value] = contagem.get(r.status.value, 0) + 1
    md = f"# dbt {comando} — Resumo do run\n\n"
    for s in NodeStatus:
        md += f"- {s.value}: {contagem.get(s.value, 0)}\n"
    falhas = [r for r in results if r.status in STATUS_FALHA]
    if falhas:
        md += "\n## Nós não-sucedidos ❌\n\n"
        for r in falhas:
            node = getattr(r, "node", None)
            nome = getattr(node, "name", None) or r.unique_id
            md += f"**{nome}** [{r.status.value}]\n\n"
            md += f"> {r.message or 'sem mensagem'}\n\n"
            md += f"Path: `{getattr(node, 'original_file_path', '')}`\n\n"
    return md


def mensagem_falha(result: dbtRunnerResult) -> str:
    """Mensagem de erro legível com os nós que falharam."""
    if result.exception:
        return f"dbt build falhou (exception): {result.exception}"
    falhas = nodes_falhados(result)
    linhas = [
        f"• {getattr(r.node, 'name', None) or r.unique_id} [{r.status.value}]: {r.message}"
        for r in falhas
    ]
    return f"dbt build falhou — {len(falhas)} nó(s) com erro:\n" + "\n".join(linhas)
