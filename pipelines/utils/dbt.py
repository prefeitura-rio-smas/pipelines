"""Helpers compartilhados para execução de dbt (resumo e mensagens de erro)."""

import contextlib
from datetime import date
import io
import json

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import NodeStatus, RunResult, SourceFreshnessResult

STATUS_FALHA = (NodeStatus.Fail, NodeStatus.Error, NodeStatus.RuntimeErr)

# Inline que executa a macro existente e devolve a partição do CadÚnico.
# A macro é resolvida pelo próprio dbt — nenhum SQL de negócio no Python.
PARTICAO_CADUNICO_INLINE_SQL = (
    "select '{{ obter_particao_cadunico() }}' as data_particao"
)


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
                f"`{result.node.name}`\n   {result.message}: "  # noqa: S608 (mensagem de erro, nao executa query)
                f"``` select * from {relation.replace('`', '')}``` \n"
            )
        return f"`{result.node.name}`\n   {result.message} \n"

    @staticmethod
    def warn(result):
        relation = getattr(result.node, "relation_name", None)
        if relation:
            return (
                f"`{result.node.name}`\n   {result.message}: "  # noqa: S608 (mensagem de erro, nao executa query)
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
        error_criteria = (
            f">={freshness.error_after.count} {freshness.error_after.period}"
        )
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


def obter_particao_cadunico(target: str) -> date:
    """Executa a macro `obter_particao_cadunico()` e devolve a data da partição.

    Usa `dbt show --inline`, então quem resolve a macro é o dbt (nenhum SQL de
    negócio vive no Python). O `dbtRunner` não expõe as linhas do `show` no
    `result`; com `--quiet --output json` o stdout contém apenas o JSON do
    resultado, capturado aqui.
    """
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = dbtRunner().invoke(
            [
                "show",
                "--inline",
                PARTICAO_CADUNICO_INLINE_SQL,
                "--target",
                target,
                "--project-dir",
                "queries",
                "--profiles-dir",
                "queries",
                "--output",
                "json",
                "--quiet",
            ]
        )
    if not result.success:
        raise RuntimeError(f"dbt show falhou: {mensagem_falha(result)}")
    try:
        payload = json.loads(buffer.getvalue())
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"dbt show nao devolveu JSON (stdout={buffer.getvalue()!r})"
        ) from error
    rows = payload.get("show") or []
    if not rows:
        raise RuntimeError(
            "dbt show nao devolveu linhas para obter_particao_cadunico()"
        )
    valor = rows[0].get("data_particao")
    if not valor:
        raise RuntimeError("obter_particao_cadunico() devolveu vazio")
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError as error:
        raise RuntimeError(f"particao invalida: {valor!r}") from error
