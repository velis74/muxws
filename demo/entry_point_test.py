"""`python demo.py` picks a demo and refuses to start with a list of what to install.

There are three demos and one flag each, so the command line's first job is to say which one this run
is. A bare `python demo.py` is the help and nothing else; every argument that belongs to one demo is
refused on the others, because a flag accepted and ignored reads as a flag obeyed.

Every dependency the demo needs fails *late* and somewhere other than where the cause is. A missing
`fastapi` is an ImportError inside a child process nobody is watching. A missing `websockets` is the
worst of them: uvicorn serves the page perfectly and answers 404 to every upgrade, so the browser
shows a muxws handshake error and every part of the diagnosis points away from the package that is
not installed.

Declaring `websockets` in the `[demo]` extra is the primary fix and is asserted by
`muxws/packaging_test.py::test_the_demo_extra_can_actually_serve_a_websocket`. This is the second
half: a declared dependency is not an installed one, and the reader who installed the wrong extra, or
`muxws` from PyPI rather than the repository, gets told which rather than debugging a handshake.

Since there are two backends there is a second way for that check to be wrong, and it is the friendly
direction: demanding what the *other* backend needs. `python demo.py --browser node` imports no
fastapi, no uvicorn and no `websockets` at all, so a check that still asked for them would stop a run
that was about to work and send the reader to install three packages nothing would load. Half the
tests below exist to pin that, and the rest pin the command line that chooses the demo and, within
`--browser`, the backend.
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

    By path because `demo.py` and the `demo/` package share a name and Python resolves the package,
    so this file cannot be reached by an ordinary import at all.
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
    `None` case is the no-argument call, pinning that the default parameter is the Python backend
    rather than "check everything".
    """
    problems = entry_point.missing_dependencies() if backend is None else entry_point.missing_dependencies(backend)
    assert problems == []


def test_a_missing_websocket_implementation_is_named(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """The one an import check of the demo's own imports cannot find.

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


def test_a_bare_invocation_prints_the_help_and_exits_zero(entry_point: Any, capsys: pytest.CaptureFixture[str]):
    """`python demo.py` starts nothing, because no one of the three demos is the obvious one.

    Exit 0 on stdout rather than exit 2 on stderr: naming no demo is the question the help answers,
    not a mistake, and a reader who piped the output would not find it in stderr.
    """
    with pytest.raises(SystemExit) as ended:
        entry_point.parse_arguments([])

    assert ended.value.code == 0
    printed = capsys.readouterr()
    assert printed.err == ""
    for flag in ("--browser", "--uds", "--bench"):
        assert flag in printed.out, flag


@pytest.mark.parametrize("flag", ["--browser", "--uds", "--bench"])
def test_each_flag_selects_its_own_demo_and_leaves_the_others_alone(entry_point: Any, flag: str):
    """One flag, one demo: the entry point branches on these three and must never see two set."""
    arguments = entry_point.parse_arguments([flag])
    selected = {name for name in ("browser", "uds", "bench") if getattr(arguments, name)}
    assert selected == {flag.removeprefix("--")}


@pytest.mark.parametrize(
    "argv",
    [
        ["--browser", "--uds"],
        ["--browser", "--bench"],
        ["--uds", "--bench"],
        ["--browser", "--uds", "--bench"],
    ],
)
def test_two_demos_in_one_command_are_refused(entry_point: Any, capsys: pytest.CaptureFixture[str], argv: list[str]):
    """They are three separate programs sharing a launcher, and one run can only be one of them.

    Refused rather than ordered by precedence: a reader who typed two would otherwise watch one demo
    run and have nothing to tell them the other was dropped.
    """
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert "not allowed with" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["node"], "needs --browser"),
        (["--backend", "node"], "needs --browser"),
        (["--no-fe"], "needs --browser"),
        (["--bench", "node"], "no backend argument"),
        (["--bench", "--backend", "python"], "no backend argument"),
        (["--bench", "--no-fe"], "nothing to turn off"),
    ],
)
def test_the_browser_demos_arguments_are_refused_everywhere_else(
    entry_point: Any, capsys: pytest.CaptureFixture[str], argv: list[str], expected: str
):
    """A language and a dev server are `--browser`'s, and neither of the other two demos has either.

    With no demo named at all these are refused too, rather than taken as a request for the browser
    demo: `python demo.py node` would then be the one command line that starts something.
    """
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert expected in capsys.readouterr().err


@pytest.mark.parametrize("argv", [["--full"], ["--browser", "--full"], ["--uds", "--full"]])
def test_full_is_refused_by_every_demo_but_bench(entry_point: Any, capsys: pytest.CaptureFixture[str], argv: list[str]):
    """`--full` widens a measurement matrix, and only one of the three demos has one."""
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert "--bench" in capsys.readouterr().err


def test_bench_runs_a_trimmed_matrix_unless_full_says_otherwise(entry_point: Any):
    """`--full` is the only thing the entry point tells the measurement, so it has to arrive intact."""
    assert entry_point.parse_arguments(["--bench"]).full is False
    assert entry_point.parse_arguments(["--bench", "--full"]).full is True


def test_the_default_backend_is_python(entry_point: Any):
    """`--browser` with no language named serves the sockets from Python."""
    assert entry_point.parse_arguments(["--browser"]).backend == "python"


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["--browser"], "python"),
        (["--browser", "python"], "python"),
        (["--browser", "node"], "node"),
        # The second accepted spelling, kept working on purpose.
        (["--browser", "--backend", "node"], "node"),
        (["--browser", "--backend", "python"], "python"),
        # Both, agreeing, is not a mistake worth refusing.
        (["--browser", "node", "--backend", "node"], "node"),
    ],
)
def test_the_backend_argument_accepts_what_it_should(entry_point: Any, argv: list[str], expected: str):
    assert entry_point.parse_arguments(argv).backend == expected


@pytest.mark.parametrize(
    "argv",
    [
        ["--browser", "ruby"],
        ["--browser", "--backend", "ruby"],
        ["--browser", "Node"],
        ["--browser", "node", "python"],
    ],
)
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
        entry_point.parse_arguments(["--browser", "node", "--backend", "python"])
    assert "pick one" in capsys.readouterr().err


def test_help_names_both_backends_and_reads_as_prose(entry_point: Any):
    """`--help` is where a reader learns there are two, so it has to say so in words.

    Asserted loosely - this is not a golden file - but it does pin that the two directory names and
    both runners are named, because "python or node" alone tells a reader nothing about what either
    one is or where to look at it.
    """
    text = entry_point.build_parser().format_help()
    for expected in ("demo/backend_python", "demo/backend_node", "uvicorn", "tsx", "python demo.py --browser node"):
        assert expected in text, expected
    # The hidden alias stays hidden: one documented spelling, or the help becomes the parameter dump
    # it was written not to be.
    assert "--backend" not in text


def test_the_frontend_runs_unless_no_fe_says_otherwise(entry_point: Any):
    """`--no-fe` starts the backend alone, for a reader driving it with a client of their own."""
    assert entry_point.parse_arguments(["--browser"]).frontend is True
    assert entry_point.parse_arguments(["--browser", "node"]).frontend is True
    assert entry_point.parse_arguments(["--browser", "--no-fe"]).frontend is False
    assert entry_point.parse_arguments(["--browser", "node", "--no-fe"]).frontend is False


def test_no_fe_does_not_demand_the_frontend_dependencies(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """Refusing to start over something this run will never load is the lie the check exists to avoid.

    It is the same reasoning that makes the check backend-aware: `--browser node` is not told to
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
    assert entry_point.parse_arguments(["--browser"]).uds is False
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


def test_the_measurement_asks_for_websockets_before_it_starts_measuring(
    entry_point: Any, monkeypatch: pytest.MonkeyPatch
):
    """`--bench` is checked like the demos that start a server, and asks for neither msgpack nor AF_UNIX.

    Every cell is a pair of child processes, so a `websockets` that is not installed surfaces as an
    ImportError inside one of them after the first cells have already been measured. The two the
    matrix works around are not in the list: msgpack cells and unix cells are skipped with a note
    naming them, which is a smaller answer than refusing to measure the rest.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: False)
    monkeypatch.delattr(entry_point.socket, "AF_UNIX", raising=False)

    assert entry_point.missing_dependencies(bench=True) == [], (
        "the measurement was refused over something it skips or never loads"
    )

    monkeypatch.setattr(entry_point, "BENCH_IMPORTS", (("no_such_module", "invented for this test"),))
    problems = entry_point.missing_dependencies(bench=True)
    assert len(problems) == 1
    assert "no_such_module" in problems[0]

    with pytest.raises(SystemExit) as refusal:
        entry_point.check_before_starting(bench=True)
    assert refusal.value.code == 1


def test_help_names_the_socket_demo_and_where_its_two_scripts_live(entry_point: Any):
    """A reader who does not know the mode exists will not type `--uds`, so `--help` has to say it."""
    text = entry_point.build_parser().format_help()
    for expected in ("--uds", "socket file", "docs/examples/uds_"):
        assert expected in text, expected


def test_help_names_the_measurement_and_what_it_compares(entry_point: Any):
    """The help is the whole of a bare run, so a reader meets `--bench` there or not at all.

    Naming the three modes rather than only the flag: "throughput" alone reads as another frame count,
    and the reason to run this one is that two of the three carry the payload with muxws taken out.
    """
    text = entry_point.build_parser().format_help()
    for expected in ("--bench", "raw socket", "raw WebSocket", "--full"):
        assert expected in text, expected
