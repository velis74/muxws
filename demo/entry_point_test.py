"""`python demo.py` selects a demo, and refuses to start with a list of missing dependencies.

There are three demos with one flag each. `python demo.py` without a flag prints the help. An
argument that belongs to one demo is rejected with the others instead of being ignored.

Without the dependency check, missing dependencies fail late and away from the cause. A missing
`fastapi` is an ImportError inside a child process. Without `websockets`, uvicorn serves the page and
answers 404 to every upgrade request, so the browser shows a muxws handshake error.

`muxws/packaging_test.py::test_the_demo_extra_includes_a_websocket_implementation` asserts that the
`[demo]` extra declares `websockets`. The check tested here reports packages that are declared but
not installed, for example after installing the wrong extra or `muxws` from PyPI instead of the
repository.

The check is per backend: `python demo.py --browser node` imports no fastapi, uvicorn or
`websockets`, so it must not require them. The tests below cover the per-backend requirements, the
command line that selects the demo, and, within `--browser`, the backend.
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

    `demo.py` and the `demo/` package share a name and `import demo` resolves to the package, so the
    script is loaded by path.
    """
    spec = importlib.util.spec_from_file_location("demo_entry_point", ROOT / "demo.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


@pytest.fixture(autouse=True)
def node_modules_present(entry_point: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts with `node_modules` reported as installed, whether or not it is.

    The Python CI job installs the `dev` extra and does not run `npm install`, so the real directory
    differs between jobs. Tests about a missing package replace this stub with their own.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: True)


@pytest.mark.parametrize("backend", ["python", "node", None])
def test_a_complete_environment_reports_nothing_missing(entry_point: Any, backend: str | None):
    """With both backends' dependencies present, the check is silent.

    This is the control for the tests below, which would also pass against a check that always
    reports problems. The `None` case is the no-argument call, whose default is the Python backend.
    """
    problems = entry_point.missing_dependencies() if backend is None else entry_point.missing_dependencies(backend)
    assert problems == []


def test_a_missing_websocket_implementation_is_named(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """A missing WebSocket implementation for uvicorn is reported.

    Nothing on the demo path imports `websockets`; uvicorn loads it at runtime, so the check lists it
    explicitly.
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
    # The message names the symptom: a 404 to the upgrade appears in the browser as a muxws handshake
    # failure that names no package.
    assert "404" in problems[0], "the message does not mention the 404 handshake failure"


def test_either_websocket_implementation_satisfies_the_check(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """`wsproto` is uvicorn's other implementation and is equally sufficient.

    Either package satisfies the check.
    """
    real = importlib.util.find_spec

    def only_wsproto(name: str, *args: Any, **kwargs: Any) -> Any:
        # `wsproto` is not a dev dependency, so its presence is faked.
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
    assert "FastAPI" in problems[0], "the message names the package without its purpose"


def test_the_node_backend_is_not_asked_for_the_python_backends_packages(
    entry_point: Any, monkeypatch: pytest.MonkeyPatch
):
    """Without fastapi, uvicorn, starlette and `websockets`, the `node` backend reports nothing missing.

    `demo/backend_node/` creates its own `WebSocketServer` over `ws` and imports no Python, so none of
    the Python backend's packages are required for it.
    """
    absent = {"fastapi", "uvicorn", "starlette", "muxws", "websockets", "wsproto"}
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **k: None if name in absent else real(name, *a, **k),
    )

    assert entry_point.missing_dependencies("node") == []
    # Control: the same environment reports the missing packages for the Python backend.
    assert len(entry_point.missing_dependencies("python")) == 5


def test_the_websocket_implementation_check_does_not_fire_for_the_node_backend(
    entry_point: Any, monkeypatch: pytest.MonkeyPatch
):
    """The WebSocket implementation is required by uvicorn only, not by the Node backend.

    The demo needs `websockets` because uvicorn has no WebSocket implementation; the Node server has
    its own.
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
    """Missing `tsx` and `ws` are reported for the Node backend, each with its purpose.

    Simulated by replacing `node_package_installed`, which checks for directories that exist in the
    repository running these tests.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda name: name not in {"tsx", "ws"})

    problems = entry_point.missing_dependencies("node")
    assert [problem.split(" - ")[0].strip() for problem in problems] == ["tsx", "ws"]
    # The reason is part of the message; `tsx` is invoked from `package.json`, not imported.
    assert "build step" in problems[0]
    assert entry_point.missing_dependencies("python") == [], "tsx and ws must not be reported for the python backend"


def test_both_backends_require_the_frontend_dependencies(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """Both backends use the same frontend, so both require its dependencies."""
    monkeypatch.setattr(entry_point, "node_package_installed", lambda name: name != "vue")

    for backend in ("python", "node"):
        problems = entry_point.missing_dependencies(backend)
        assert any("npm install" in problem for problem in problems), backend


def test_a_bare_invocation_prints_the_help_and_exits_zero(entry_point: Any, capsys: pytest.CaptureFixture[str]):
    """`python demo.py` without a flag prints the help to stdout and exits 0, starting nothing.

    Not exit 2 on stderr: a missing demo flag is a request for the help, not an error.
    """
    with pytest.raises(SystemExit) as ended:
        entry_point.parse_arguments([])

    assert ended.value.code == 0
    printed = capsys.readouterr()
    assert printed.err == ""
    for flag in ("--browser", "--uds", "--bench"):
        assert flag in printed.out, flag


@pytest.mark.parametrize("flag", ["--browser", "--uds", "--bench"])
def test_each_flag_sets_only_its_own_demo_attribute(entry_point: Any, flag: str):
    """Each flag sets exactly one of the three demo attributes."""
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
    """Two demo flags in one command are rejected.

    Rejected instead of resolved by precedence, so that no requested demo is silently dropped.
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
    """The backend argument and `--no-fe` apply only to `--browser` and are rejected elsewhere.

    Without a demo flag they are rejected too, instead of implying `--browser`.
    """
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert expected in capsys.readouterr().err


@pytest.mark.parametrize("argv", [["--full"], ["--browser", "--full"], ["--uds", "--full"]])
def test_full_is_refused_by_every_demo_but_bench(entry_point: Any, capsys: pytest.CaptureFixture[str], argv: list[str]):
    """`--full` applies only to `--bench`."""
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert "--bench" in capsys.readouterr().err


def test_full_flag_is_passed_to_bench(entry_point: Any):
    """`--full` is passed through to the measurement."""
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
        # The alternative `--backend` spelling.
        (["--browser", "--backend", "node"], "node"),
        (["--browser", "--backend", "python"], "python"),
        # Both spellings, naming the same backend, are accepted.
        (["--browser", "node", "--backend", "node"], "node"),
    ],
)
def test_each_accepted_backend_spelling_selects_its_backend(entry_point: Any, argv: list[str], expected: str):
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
    """An unknown backend is rejected instead of falling back to python.

    A fallback would run a backend that was not requested, and the frontend works the same with
    either, so nothing would show the substitution.
    """
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2


def test_asking_for_both_backends_at_once_is_refused(entry_point: Any, capsys: pytest.CaptureFixture[str]):
    """Two spellings naming different backends are rejected."""
    with pytest.raises(SystemExit):
        entry_point.parse_arguments(["--browser", "node", "--backend", "python"])
    assert "pick one" in capsys.readouterr().err


def test_help_names_both_backend_directories_and_runners(entry_point: Any):
    """`--help` describes both backends.

    Not a golden-file comparison: it asserts that both backend directories and both runners are
    named.
    """
    text = entry_point.build_parser().format_help()
    for expected in ("demo/backend_python", "demo/backend_node", "uvicorn", "tsx", "python demo.py --browser node"):
        assert expected in text, expected
    # The `--backend` alias is not shown in the help.
    assert "--backend" not in text


def test_no_fe_disables_the_frontend(entry_point: Any):
    """`--no-fe` starts the backend without the frontend, for use with another client."""
    assert entry_point.parse_arguments(["--browser"]).frontend is True
    assert entry_point.parse_arguments(["--browser", "node"]).frontend is True
    assert entry_point.parse_arguments(["--browser", "--no-fe"]).frontend is False
    assert entry_point.parse_arguments(["--browser", "node", "--no-fe"]).frontend is False


def test_no_fe_does_not_demand_the_frontend_dependencies(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """With `--no-fe` the frontend's dependencies are not required.

    The dev server is not started, so `node_modules` is not needed for the Python backend.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda name: name != "vue")

    with_frontend = entry_point.missing_dependencies("python", True)
    assert [problem.split(" - ")[0].strip() for problem in with_frontend] == ["npm install"]

    assert entry_point.missing_dependencies("python", False) == [], "--no-fe reported frontend dependencies as missing"


def test_the_node_backend_still_needs_npm_install_under_no_fe(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """`--no-fe` skips the *frontend's* dependencies, not the backend's.

    The Node backend is run by `tsx` from the same `node_modules`, so it is required with or without
    the frontend, and is reported as a backend dependency, not a frontend one.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: False)

    problems = entry_point.missing_dependencies("node", False)
    assert problems, "missing node_modules was not reported for the node backend"
    assert not any("frontend" in problem for problem in problems), problems


def test_uds_is_a_mode_of_its_own_and_not_a_third_backend(entry_point: Any):
    """`--uds` selects a separate demo and is not a value of `backend`.

    The socket demo has no frontend, no port and no choice of language.
    """
    assert entry_point.parse_arguments(["--browser"]).uds is False
    assert entry_point.parse_arguments(["--uds"]).uds is True
    assert "uds" not in entry_point.BACKENDS


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        # The socket demo runs Python at both ends, so a backend argument is rejected.
        (["node", "--uds"], "no backend argument"),
        (["python", "--uds"], "no backend argument"),
        (["--backend", "node", "--uds"], "no backend argument"),
        # The socket demo starts no frontend, so `--no-fe` is rejected instead of ignored.
        (["--uds", "--no-fe"], "nothing to turn off"),
    ],
)
def test_uds_rejects_arguments_of_other_demos(
    entry_point: Any, capsys: pytest.CaptureFixture[str], argv: list[str], expected: str
):
    with pytest.raises(SystemExit) as refusal:
        entry_point.parse_arguments(argv)
    assert refusal.value.code == 2
    assert expected in capsys.readouterr().err


def test_uds_demo_requires_only_its_own_two_packages(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """The socket demo starts neither uvicorn nor a dev server and does not require them.

    A missing `AF_UNIX` (Windows) is reported in the same list before anything starts.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: False)

    assert entry_point.missing_dependencies(uds=True) == [], (
        "the uds demo reported packages it does not import as missing"
    )

    # `raising=False` so the removal also succeeds on a platform without `AF_UNIX`.
    monkeypatch.delattr(entry_point.socket, "AF_UNIX", raising=False)
    problems = entry_point.missing_dependencies(uds=True)
    assert len(problems) == 1
    assert "AF_UNIX" in problems[0]


def test_bench_checks_websockets_before_the_first_cell(entry_point: Any, monkeypatch: pytest.MonkeyPatch):
    """`--bench` is checked like the demos that start a server, and asks for neither msgpack nor AF_UNIX.

    Every cell runs a pair of child processes, so a missing `websockets` would otherwise raise
    ImportError in a child after the first cells were measured. msgpack and `AF_UNIX` are not
    required: the matrix skips those cells with a note.
    """
    monkeypatch.setattr(entry_point, "node_package_installed", lambda _name: False)
    monkeypatch.delattr(entry_point.socket, "AF_UNIX", raising=False)

    assert entry_point.missing_dependencies(bench=True) == [], (
        "bench reported a package it skips or does not import as missing"
    )

    monkeypatch.setattr(entry_point, "BENCH_IMPORTS", (("no_such_module", "invented for this test"),))
    problems = entry_point.missing_dependencies(bench=True)
    assert len(problems) == 1
    assert "no_such_module" in problems[0]

    with pytest.raises(SystemExit) as refusal:
        entry_point.check_before_starting(bench=True)
    assert refusal.value.code == 1


def test_help_names_the_socket_demo_and_where_its_two_scripts_live(entry_point: Any):
    """`--help` describes `--uds` and names its scripts."""
    text = entry_point.build_parser().format_help()
    for expected in ("--uds", "socket file", "docs/examples/uds_"):
        assert expected in text, expected


def test_help_names_the_measurement_and_what_it_compares(entry_point: Any):
    """`--help` describes `--bench` and names the modes it compares."""
    text = entry_point.build_parser().format_help()
    for expected in ("--bench", "raw socket", "raw WebSocket", "--full"):
        assert expected in text, expected
