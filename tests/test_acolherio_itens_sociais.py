"""Validação dos formatos das macros, sem conexão com BigQuery.

O teste singular dbt verifica os valores e o escape no BigQuery. Estes casos
cobrem formatos válidos e erros que devem ocorrer antes de enviar o SQL.
"""

from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, StrictUndefined
import pytest


def _raise_compiler_error(message):
    raise ValueError(message)


@pytest.fixture(scope="module")
def macros():
    source = (
        Path(__file__).resolve().parents[1]
        / "queries/macros/dominios/acolherio/sociais/itens.sql"
    ).read_text(encoding="utf-8")
    # Templates SQL não usam o escape HTML do Jinja.
    environment = Environment(undefined=StrictUndefined, autoescape=False)  # noqa: S701
    return environment.from_string(source).make_module(
        {
            "exceptions": SimpleNamespace(raise_compiler_error=_raise_compiler_error),
        }
    )


@pytest.mark.parametrize(
    "formato",
    [
        "%s",
        "Item %%s: %s",
        "Item %%d: %s",
        "100%% — %s",
        "%%%s",
        "%s%%",
        "Item d'origem \\pasta\n%s 😀",
    ],
)
def test_formato_valido_compila(macros, formato):
    macros.acolherio_normalizar_itens("itens", formato)


@pytest.mark.parametrize(
    "formato",
    ["", "sem placeholder", "%%s", "%s %s", "%d", "%s %d", "%s %x", "%s %", "%s %t"],
)
def test_formato_invalido_falha_na_compilacao(macros, formato):
    with pytest.raises(ValueError, match="exatamente um placeholder '%s'"):
        macros.acolherio_normalizar_itens("itens", formato)
