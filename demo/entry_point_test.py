"""`python demo.py` refuses to start with a list of what to install.

Every dependency the demo needs fails *late* and somewhere other than where the cause is. A missing
`fastapi` is an ImportError inside a child process nobody is watching. A missing `websockets` is the
worst of them: uvicorn serves the page perfectly and answers 404 to every upgrade, so the browser
shows a muxws handshake error and every part of the diagnosis points away from the package that is
not installed. That happened to the first person to run this demo.

Declaring `websockets` in the `[demo]` extra is the primary fix and is asserted by
`muxws/packaging_test.py::test_the_demo_extra_can_actually_serve_a_websocket`. This is the second
half: a declared dependency is not an installed one, and the reader who installed the wrong extra, or
`muxws` from PyPI rather than the repository, gets told which rather than debugging a handshake.
"""

from __future__ import annotations

import importlib.util

from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def entry_point() -> Any:
    """`demo.py` loaded by path.

    By path because `demo.py` and the `demo/` package share a name and Python resolves the package -
    so this file cannot be reached by an ordinary import at all, which is exactly why nothing tested
    it until now.
    """
    spec = importlib.util.spec_from_file_location("demo_entry_point", ROOT / "demo.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_a_complete_environment_reports_nothing_missing(entry_point: Any):
    """The environment running this test has the demo extra, so the check must be silent in it.

    Without this the two tests below would pass equally well against a check that always complains.
    """
    assert entry_point.missing_dependencies() == []


def test_a_missing_websocket_implementation_is_named(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """The one an import check of the demo's own imports could never have found.

    Nothing in this repository imports `websockets` on the demo path - it is uvicorn's, at runtime,
    and invisible to anything that walks our imports. So the check has to know to look for it.
    """
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **k: None if name in {"websockets", "wsproto"} else real(name, *a, **k),
    )

    problems = entry_point.missing_dependencies()
    assert len(problems) == 1, problems
    assert "websockets" in problems[0]
    # The message has to say what the reader will *see*, or they will go looking at the codec: a 404
    # to an upgrade reaches the browser as a muxws handshake failure and names no missing package.
    assert "404" in problems[0], "the message does not describe the symptom the reader will meet"


def test_either_websocket_implementation_satisfies_the_check(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """`wsproto` is uvicorn's other implementation and is equally sufficient.

    Asserted so the check cannot quietly narrow to the one package this repository happens to install.
    """
    real = importlib.util.find_spec

    def only_wsproto(name: str, *args: Any, **kwargs: Any) -> Any:
        # `wsproto` is not installed here either, so it is faked rather than made a dev dependency
        # for one assertion.
        if name == "wsproto":
            return object()
        if name == "websockets":
            return None
        return real(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", only_wsproto)
    assert entry_point.missing_dependencies() == []


def test_a_missing_direct_import_is_named_with_the_reason(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **k: None if name == "fastapi" else real(name, *a, **k),
    )

    problems = entry_point.missing_dependencies()
    assert [problem.split(" - ")[0].strip() for problem in problems] == ["fastapi"]
    assert "FastAPI" in problems[0], "a bare package name leaves the reader to guess what it is for"
