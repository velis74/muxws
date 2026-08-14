"""`python demo.py` picks a backend and refuses to start with a list of what to install.

Every dependency the demo needs fails *late* and somewhere other than where the cause is. A missing
`fastapi` is an ImportError inside a child process nobody is watching. A missing `websockets` is the
worst of them: uvicorn serves the page perfectly and answers 404 to every upgrade, so the browser
shows a muxws handshake error and every part of the diagnosis points away from the package that is
not installed. That happened to the first person to run this demo.

Declaring `websockets` in the `[demo]` extra is the primary fix and is asserted by
`muxws/packaging_test.py::test_the_demo_extra_can_actually_serve_a_websocket`. This is the second
half: a declared dependency is not an installed one, and the reader who installed the wrong extra, or
`muxws` from PyPI rather than the repository, gets told which rather than debugging a handshake.

Since there are two backends there is a second way for that check to be wrong, and it is the friendly
direction: demanding what the *other* backend needs. `python demo.py node` imports no fastapi, no
uvicorn and no `websockets` at all, so a check that still asked for them would stop a run that was
about to work and send the reader to install three packages nothing would load. Half the tests below
exist to pin that, and the rest pin the command line that chooses between the two.
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


@pytest.fixture(autouse=True)
def node_modules_present(entry_point: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts from an installed `node_modules`, whether the machine has one or not.

    The Python CI job installs the `dev` extra and never runs `npm install`, so reading the real
    directory would make half of this file depend on which job it ran in. The tests about a
    *missing* package replace this with their own stub.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: True)


@pytest.mark.parametrize("backend", ["python", "node", None])
def test_a_complete_environment_reports_nothing_missing(entry_point: Any, backend: str | None):
    """With both backends' dependencies present, the check is silent.

    Without this the tests below would pass equally well against a check that always complains. The
    `None` case is the no-argument call the older tests make, pinning that the default parameter is
    the Python backend rather than "check everything".
    """
    problems = entry_point.missing_dependencies() if backend is None else entry_point.missing_dependencies(backend)
    assert problems == []


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


def test_the_node_backend_is_not_asked_for_the_python_backends_packages(
    entry_point: Any, monkeypatch: pytest.MonkeyPatch
):
    """An environment with no fastapi, no uvicorn, no starlette and no `websockets` runs `node` fine.

    This is the whole reason the check takes a backend. `demo/backend_node/` builds its own
    `WebSocketServer` over `ws` and imports nothing from Python at all, so every line the Python check
    would print is a package that run would never load - and a refusal to start over one of them is a
    working demo stopped by its own launcher.
    """
    absent = {"fastapi", "uvicorn", "starlette", "muxws", "websockets", "wsproto"}
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **k: None if name in absent else real(name, *a, **k),
    )

    assert entry_point.missing_dependencies("node") == []
    # And the same environment on the other backend is the check still working, rather than a check
    # that has quietly stopped looking at anything.
    assert len(entry_point.missing_dependencies("python")) == 5


def test_the_websocket_implementation_check_does_not_fire_for_the_node_backend(
    entry_point: Any, monkeypatch: pytest.MonkeyPatch
):
    """It is uvicorn's gap, not the protocol's, and it is the one most likely to be over-applied.

    `websockets` exists in this demo for exactly one reason: uvicorn has no WebSocket implementation.
    Node's server has its own, so a reader who chose that backend and does not have `websockets` is
    not looking at the 404-to-every-upgrade failure this check was written for.
    """
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **k: None if name in {"websockets", "wsproto"} else real(name, *a, **k),
    )

    assert entry_point.missing_dependencies("node") == []
    assert len(entry_point.missing_dependencies("python")) == 1


def test_the_node_backends_own_packages_are_named(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """`tsx` and `ws` fail the way `fastapi` does: inside a child process, under a frontend that ran.

    Faked rather than uninstalled, because `node_package_installed` is a directory probe and the
    repository running these tests has both.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda name: name not in {"tsx", "ws"})

    problems = entry_point.missing_dependencies("node")
    assert [problem.split(" - ")[0].strip() for problem in problems] == ["tsx", "ws"]
    # A bare package name leaves the reader to guess; `tsx` in particular is not a name they will
    # have met, because nothing in this repository imports it - `package.json` invokes it.
    assert "build step" in problems[0]
    assert entry_point.missing_dependencies("python") == [], "these are not the Python backend's"


def test_the_frontend_is_demanded_by_both_backends(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """There is one frontend and it is the thing being demonstrated, so neither backend is exempt."""
    monkeypatch.setattr(entry_point, "node_package_installed", lambda name: name != "vue")

    for backend in ("python", "node"):
        problems = entry_point.missing_dependencies(backend)
        assert any("npm install" in problem for problem in problems), backend


def test_the_default_backend_is_python(entry_point: Any):
    """The backend that has always been here, so `python demo.py` keeps meaning what it meant."""
    assert entry_point.parse_arguments([]).backend == "python"


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        ([], "python"),
        (["python"], "python"),
        (["node"], "node"),
        # The spelling `demo/backend_node/main.ts` tells the reader to type, kept working on purpose.
        (["--backend", "node"], "node"),
        (["--backend", "python"], "python"),
        # Both, agreeing, is not a mistake worth refusing.
        (["node", "--backend", "node"], "node"),
    ],
)
def test_the_backend_argument_accepts_what_it_should(entry_point: Any, argv: list[str], expected: str):
    assert entry_point.parse_arguments(argv).backend == expected


@pytest.mark.parametrize("argv", [["ruby"], ["--backend", "ruby"], ["Node"], ["node", "python"]])
def test_an_unknown_backend_is_refused_rather_than_defaulted(entry_point: Any, argv: list[str]):
    """Silently falling back to python would start a backend the reader did not ask for.

    Everything downstream would then look right - the frontend URL works, the sockets connect, the
    board fills - which is the failure this demo is least able to survive: it exists to show that the
    two backends are indistinguishable, so a launcher that runs the wrong one is unfalsifiable.
    """
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2


def test_asking_for_both_backends_at_once_is_refused(entry_point: Any, capsys: pytest.CaptureFixture[str]):
    """Two spellings of one choice are accepted; two *different* choices are not."""
    with pytest.raises(SystemExit):
        entry_point.parse_arguments(["node", "--backend", "python"])
    assert "pick one" in capsys.readouterr().err


def test_help_names_both_backends_and_reads_as_prose(entry_point: Any):
    """`--help` is where a reader learns there are two, so it has to say so in words.

    Asserted loosely - this is not a golden file - but it does pin that the two directory names and
    both runners are named, because "python or node" alone tells a reader nothing about what either
    one is or where to look at it.
    """
    text = entry_point.build_parser().format_help()
    for expected in ("demo/backend_python", "demo/backend_node", "uvicorn", "tsx", "python demo.py node"):
        assert expected in text, expected
    # The hidden alias stays hidden: one documented spelling, or the help becomes the parameter dump
    # it was written not to be.
    assert "--backend" not in text


def test_the_frontend_runs_unless_no_fe_says_otherwise(entry_point: Any):
    """`--no-fe` starts the backend alone, for a reader driving it with a client of their own."""
    assert entry_point.parse_arguments([]).frontend is True
    assert entry_point.parse_arguments(["node"]).frontend is True
    assert entry_point.parse_arguments(["--no-fe"]).frontend is False
    assert entry_point.parse_arguments(["node", "--no-fe"]).frontend is False


def test_no_fe_does_not_demand_the_frontend_dependencies(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """Refusing to start over something this run will never load is the lie the check exists to avoid.

    It is the same reasoning that makes the check backend-aware: `python demo.py node` is not told to
    install uvicorn, and `--no-fe` is not told to install several hundred megabytes of `node_modules`
    for a dev server it was explicitly asked not to start.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda name: name != "vue")

    with_frontend = entry_point.missing_dependencies("python", True)
    assert [problem.split(" - ")[0].strip() for problem in with_frontend] == ["npm install"]

    assert entry_point.missing_dependencies("python", False) == [], (
        "--no-fe was refused over a dev server it was told not to start"
    )


def test_the_node_backend_still_needs_npm_install_under_no_fe(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """`--no-fe` skips the *frontend's* dependencies, not the backend's.

    The Node backend is run by `tsx` out of the same `node_modules`, so an absent one is fatal however
    the frontend was asked for - and reporting it as "the frontend's dependencies" would send the
    reader looking in the wrong place.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: False)

    problems = entry_point.missing_dependencies("node", False)
    assert problems, "the node backend cannot run without node_modules and the check said nothing"
    assert not any("frontend" in problem for problem in problems), problems


def test_uds_is_a_mode_of_its_own_and_not_a_third_backend(entry_point: Any):
    """`--uds` selects a different demo, so it must not be reachable as a value of `backend`.

    The two are not variants of one run: the socket demo has no frontend, no port and no choice of
    language, so a third `BACKENDS` entry would answer "which language serves the sockets" with a
    mode that does not serve them.
    """
    assert entry_point.parse_arguments([]).uds is False
    assert entry_point.parse_arguments(["--uds"]).uds is True
    assert "uds" not in entry_point.BACKENDS


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        # Answering a request for the TypeScript port by silently running the Python one is the
        # failure this refusal exists for; the socket demo is Python at both ends.
        (["node", "--uds"], "no backend argument"),
        (["python", "--uds"], "no backend argument"),
        (["--backend", "node", "--uds"], "no backend argument"),
        # Not an error worth being clever about, but silence here would leave a reader believing they
        # had turned something off.
        (["--uds", "--no-fe"], "nothing to turn off"),
    ],
)
def test_uds_refuses_the_arguments_that_cannot_mean_anything(
    entry_point: Any, capsys: pytest.CaptureFixture[str], argv: list[str], expected: str
):
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert expected in capsys.readouterr().err


def test_the_uds_demo_asks_for_two_packages_and_not_the_browser_demo_s_six(
    entry_point: Any, monkeypatch: pytest.MonkeyPatch
):
    """It starts neither uvicorn nor a dev server, so demanding either would be a lie.

    The check is also where a Windows reader is told before anything starts: an absent `AF_UNIX` is
    not a missing package and no install fixes it, but it belongs in the same list rather than three
    seconds later inside the client.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: False)

    assert entry_point.missing_dependencies(uds=True) == [], (
        "the socket demo was refused over dependencies it never loads"
    )

    # `raising=False` so this reads the same on a machine that has no `AF_UNIX` to remove, which is
    # the very platform the branch is about.
    monkeypatch.delattr(entry_point.socket, "AF_UNIX", raising=False)
    problems = entry_point.missing_dependencies(uds=True)
    assert len(problems) == 1
    assert "AF_UNIX" in problems[0]


def test_help_names_the_socket_demo_and_where_its_two_scripts_live(entry_point: Any):
    """A reader who does not know the mode exists will not type `--uds`, so `--help` has to say it."""
    text = entry_point.build_parser().format_help()
    for expected in ("--uds", "socket file", "docs/examples/uds_"):
        assert expected in text, expected
