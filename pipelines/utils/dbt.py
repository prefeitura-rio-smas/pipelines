"""Helpers compartilhados para execução de dbt (resumo e mensagens de erro)."""
from dbt.cli.main import dbtRunnerResult
from dbt.contracts.results import NodeStatus, RunResult, SourceFreshnessResult

STATUS_FALHA = (NodeStatus.Fail, NodeStatus.Error, NodeStatus.RuntimeErr)


class RunResultSummarizer:
    """Resumo de um RunResult (modelo/teste) por status."""

    def summarize(self, result):
        if result.status == "error":
            return self.error(result)
        if result.status == "fail":
            return self.fail(result)
        if result.status == "warn":
            return self.warn(result)
        return None

    @staticmethod
    def error(result):
        return f"`{result.node.name}`\n  {result.message.replace('__', '_')} \n"

    @staticmethod
    def fail(result):
        relation = getattr(result.node, "relation_name", None)
        if relation:
            return (
                f"`{result.node.name}`\n   {result.message}: "
                f"``` select * from {relation.replace('`', '')}``` \n"
            )
        return f"`{result.node.name}`\n   {result.message} \n"

    @staticmethod
    def warn(result):
        relation = getattr(result.node, "relation_name", None)
        if relation:
            return (
                f"`{result.node.name}`\n   {result.message}: "
                f"``` select * from {relation.replace('`', '')}``` \n"
            )
        return f"`{result.node.name}`\n   {result.message} \n"


class FreshnessResultSummarizer:
    """Resumo de um SourceFreshnessResult por status."""

    def summarize(self, result):
        if result.status == "error":
            return self.error(result)
        if result.status == "fail":
            return self.fail(result)
        if result.status == "warn":
            return self.warn(result)
        return None

    @staticmethod
    def error(result):
        freshness = result.node.freshness
        error_criteria = f">={freshness.error_after.count} {freshness.error_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({error_criteria})"

    @staticmethod
    def fail(result):
        return f"{result.node.relation_name.replace('`', '')}"

    @staticmethod
    def warn(result):
        freshness = result.node.freshness
        warn_criteria = f">={freshness.warn_after.count} {freshness.warn_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({warn_criteria})"


class Summarizer:
    """Roteia o resultado para o summarizer adequado (RunResult ou Freshness)."""

    def __call__(self, result):
        if isinstance(result, RunResult):
            return RunResultSummarizer().summarize(result)
        if isinstance(result, SourceFreshnessResult):
            return FreshnessResultSummarizer().summarize(result)
        return None


def nodes_falhados(result: dbtRunnerResult) -> list:
    """RunResults com status de falha (fail/error/runtime error)."""
    if result.exception or not result.result:
        return []
    return [r for r in result.result.results if r.status in STATUS_FALHA]


def mensagem_falha(result: dbtRunnerResult) -> str:
    """Mensagem de erro legível com os nós que falharam (formato do report)."""
    if result.exception:
        return f"dbt build falhou (exception): {result.exception}"
    falhas = nodes_falhados(result)
    summarizer = Summarizer()
    linhas = []
    for r in falhas:
        if r.status == NodeStatus.Fail:
            linhas.append(f"- 🛑 FAIL: {summarizer(r)}")
        else:
            linhas.append(f"- ❌ ERROR: {summarizer(r)}")
    return f"dbt build falhou — {len(falhas)} nó(s) com erro:\n" + "\n".join(linhas)
