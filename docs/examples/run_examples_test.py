"""Runs the shipped Python examples and checks them against the output the guide prints.

The guide's expected-output blocks are the expected values. `documented_output` reads them from the
given page (`docs/guide/getting-started.md` for the quick start, `docs/guide/transports.md` for the
Unix-socket pair), so an example whose output differs from the page fails the test.
"""

import importlib.util
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest

EXAMPLES = Path(__file__).resolve().parent
DOCS = EXAMPLES.parent
REPOSITORY = DOCS.parent
GETTING_STARTED = DOCS / "guide" / "getting-started.md"
TRANSPORTS = DOCS / "guide" / "transports.md"
API = DOCS / "api"

#: How long to wait for an example server to start listening, in seconds.
STARTUP_TIMEOUT_SECONDS = 30.0
#: How long any one example client may run before it is killed, in seconds.
CLIENT_TIMEOUT_SECONDS = 60.0
#: How long a terminated example server gets to exit, in seconds.
SHUTDOWN_TIMEOUT_SECONDS = 10.0
#: How long to wait between attempts to connect to a starting server, in seconds.
POLL_INTERVAL_SECONDS = 0.05

#: `<!-- expected-output: name -->` followed by the fenced block the page prints.
EXPECTED_OUTPUT = re.compile(
    r"<!--\s*expected-output:\s*(?P<name>[a-z][a-z-]*)\s*-->\s*\n+```[a-z]*\n(?P<body>.*?)^```",
    re.DOTALL | re.MULTILINE,
)
#: A fenced `python` block under an `### Example` heading on an API page.
API_EXAMPLE = re.compile(
    r"^#{3,}\s+Example\b[^\n]*\n(?:(?!^#{1,6}\s).)*?^```python\n(?P<code>.*?)^```", re.DOTALL | re.MULTILINE
)

#: The environment of every example subprocess, with this checkout first on `PYTHONPATH`.
#:
#: Python puts the script's directory on `sys.path`, not the working directory, and `docs/examples/`
#: contains no package. Without this an example imports an installed `muxws`, if any, instead of the
#: checkout under test. `cwd=REPOSITORY` does not add the repository to `sys.path` for a script.
EXAMPLE_ENV = {
    **os.environ,
    "PYTHONPATH": os.pathsep.join([str(REPOSITORY), *filter(None, [os.environ.get("PYTHONPATH")])]),
}

MISSING_ACCEPTOR = importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("uvicorn") is None
needs_acceptor = pytest.mark.skipif(
    MISSING_ACCEPTOR,
    reason="the quick-start acceptor is a FastAPI app under uvicorn; install fastapi and uvicorn to run it",
)

#: The Unix-socket pair needs neither fastapi nor uvicorn (its acceptor is `websockets.unix_serve`),
#: but it needs `websockets` and `AF_UNIX`, which Windows does not have.
MISSING_WEBSOCKETS = importlib.util.find_spec("websockets") is None
needs_unix_sockets = pytest.mark.skipif(
    MISSING_WEBSOCKETS or not hasattr(socket, "AF_UNIX"),
    reason="the Unix-socket pair needs the websockets library and an AF_UNIX kernel",
)


def documented_output(name: str, page: Path = GETTING_STARTED) -> str:
    """The exact block `page` prints for `name`."""
    text = page.read_text(encoding="utf-8")
    for match in EXPECTED_OUTPUT.finditer(text):
        if match.group("name") == name:
            return match.group("body")
    pytest.fail(f"{page.name} has no <!-- expected-output: {name} --> block")


def free_port() -> int:
    """A port that is free at the time of the call; it is released before the server binds it."""
    with closing(socket.socket()) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_until_listening(family: int, address: Any, process: subprocess.Popen[str], what: str) -> None:
    """Poll `address` until the server accepts a connection, or fail with the reason.

    Readiness is tested by connecting: a socket file exists from `bind()` but accepts connections only
    after `listen()`, so checking `os.path.exists` could let the client connect too early and get
    `ECONNREFUSED`. `connect_ex` returns a non-zero errno in both not-ready states, so one loop
    handles both TCP ports and socket files.
    """
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"the example server exited with {process.returncode}: {process.communicate()[1]}")
        with closing(socket.socket(family)) as probe:
            probe.settimeout(POLL_INTERVAL_SECONDS)
            if probe.connect_ex(address) == 0:
                return
        time.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"the example server did not start accepting on {what} within {STARTUP_TIMEOUT_SECONDS} seconds")


@contextmanager
def example_server(script: str) -> Iterator[str]:
    """Start one of the example servers on an ephemeral port and yield the URL to dial."""
    port = free_port()
    process = subprocess.Popen(  # noqa: S603 - a fixed script, run with this interpreter
        [sys.executable, str(EXAMPLES / script)],
        cwd=str(REPOSITORY),
        env={**EXAMPLE_ENV, "MUXWS_PORT": str(port)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_until_listening(socket.AF_INET, ("127.0.0.1", port), process, f"port {port}")
        yield f"ws://127.0.0.1:{port}/ws"
    finally:
        process.terminate()
        try:
            process.communicate(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()


@contextmanager
def example_server_on_a_socket_file(script: str) -> Iterator[str]:
    """Start one of the example servers on a socket **file** and yield the `ws+unix://` URL to dial.

    A separate function from `example_server`: the two differ in the environment variable they set,
    the address they probe and the URL they build.

    The directory is short. A Unix socket path goes into `sockaddr_un.sun_path`, which is about 108
    bytes on Linux and less elsewhere, and pytest's `tmp_path` can exceed that. The length is checked
    here, so a long `TMPDIR` skips the test with the length in the message instead of failing in the
    subprocess's `bind()` with `OSError: AF_UNIX path too long`.
    """
    with tempfile.TemporaryDirectory(prefix="muxws-") as directory:
        path = Path(directory) / "s.sock"
        measured = len(os.fsencode(path))
        if measured > 100:
            pytest.skip(f"the temporary socket path is {measured} bytes, too near the ~108-byte sun_path limit")
        process = subprocess.Popen(  # noqa: S603 - a fixed script, run with this interpreter
            [sys.executable, str(EXAMPLES / script)],
            cwd=str(REPOSITORY),
            env={**EXAMPLE_ENV, "MUXWS_SOCKET": str(path)},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_until_listening(socket.AF_UNIX, str(path), process, str(path))
            yield f"ws+unix://{path}:/ws"
        finally:
            process.terminate()
            try:
                process.communicate(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()


def run_client(script: str, url: str) -> str:
    """Run one of the example clients against `url` and return its stdout."""
    completed = subprocess.run(  # noqa: S603 - a fixed script, run with this interpreter
        [sys.executable, str(EXAMPLES / script)],
        cwd=str(REPOSITORY),
        env={**EXAMPLE_ENV, "MUXWS_URL": url},
        capture_output=True,
        text=True,
        timeout=CLIENT_TIMEOUT_SECONDS,
        check=False,
    )
    assert completed.returncode == 0, f"{script} exited with {completed.returncode}:\n{completed.stderr}"
    return completed.stdout


@needs_acceptor
def test_quickstart_runs_end_to_end() -> None:
    """The quick-start server and client print exactly the documented output."""
    with example_server("quickstart_server.py") as url:
        assert run_client("quickstart_client.py", url) == documented_output("quickstart")


@needs_acceptor
def test_server_push_runs_end_to_end() -> None:
    """The acceptor opens a stream and the dialer's handler receives it."""
    with example_server("push_server.py") as url:
        assert run_client("push_client.py", url) == documented_output("push")


@needs_acceptor
def test_reconnecting_client_runs_end_to_end() -> None:
    """`hello=` and `reconnect=` against an acceptor with no reconnect-specific code."""
    with example_server("quickstart_server.py") as url:
        assert run_client("reconnect_client.py", url) == documented_output("reconnect")


@needs_unix_sockets
def test_the_unix_socket_pair_runs_end_to_end() -> None:
    """`connect()` with a `ws+unix://` URL reaches a `unix_serve` acceptor.

    The client uses only the public `connect()`; only the URL differs from the TCP quick start, and
    the output matches the documented output. A dial that fell back to TCP, or a URL split at the
    wrong colon, does not produce these lines.
    """
    with example_server_on_a_socket_file("uds_server.py") as url:
        assert run_client("uds_client.py", url) == documented_output("unix-socket", TRANSPORTS)


def api_examples() -> list[tuple[str, str]]:
    """Every fenced `python` block under an `### Example` heading in `docs/api/`."""
    if not API.is_dir():
        return []
    found: list[tuple[str, str]] = []
    for page in sorted(API.glob("*.md")):
        text = page.read_text(encoding="utf-8")
        for index, match in enumerate(API_EXAMPLE.finditer(text)):
            found.append((f"{page.name}[{index}]", match.group("code")))
    return found


def test_every_api_example_executes(tmp_path: Path) -> None:
    """Every API-reference example runs as written."""
    examples = api_examples()
    if not examples:
        pytest.skip("docs/api/ has no `### Example` python blocks yet")
    failures: list[str] = []
    for name, code in examples:
        script = tmp_path / f"{name.replace('[', '_').replace(']', '')}.py"
        script.write_text(code, encoding="utf-8")
        completed = subprocess.run(  # noqa: S603 - a documented example, run with this interpreter
            [sys.executable, str(script)],
            cwd=str(REPOSITORY),
            env=EXAMPLE_ENV,
            capture_output=True,
            text=True,
            timeout=CLIENT_TIMEOUT_SECONDS,
            check=False,
        )
        if completed.returncode != 0:
            failures.append(f"{name} exited with {completed.returncode}:\n{completed.stderr}")
    assert not failures, "\n\n".join(failures)
