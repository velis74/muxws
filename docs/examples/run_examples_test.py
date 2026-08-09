"""Runs the shipped Python examples and checks them against the output the guide prints.

The guide's expected-output blocks are not prose here: they are the fixture. `documented_output`
reads them out of `docs/guide/getting-started.md`, so an example whose behaviour drifts from the page
fails in CI rather than in a reader's terminal.
"""

import importlib.util
import os
import re
import socket
import subprocess
import sys
import time

from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent
DOCS = EXAMPLES.parent
REPOSITORY = DOCS.parent
GETTING_STARTED = DOCS / "guide" / "getting-started.md"
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

MISSING_ACCEPTOR = importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("uvicorn") is None
needs_acceptor = pytest.mark.skipif(
    MISSING_ACCEPTOR,
    reason="the quick-start acceptor is a FastAPI app under uvicorn; install fastapi and uvicorn to run it",
)


def documented_output(name: str) -> str:
    """The exact block `getting-started.md` prints for `name`."""
    text = GETTING_STARTED.read_text(encoding="utf-8")
    for match in EXPECTED_OUTPUT.finditer(text):
        if match.group("name") == name:
            return match.group("body")
    pytest.fail(f"getting-started.md has no <!-- expected-output: {name} --> block")


def free_port() -> int:
    """A port the operating system is not using, released again before the server takes it."""
    with closing(socket.socket()) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_until_listening(port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"the example server exited with {process.returncode}: {process.communicate()[1]}")
        with closing(socket.socket()) as probe:
            probe.settimeout(POLL_INTERVAL_SECONDS)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"the example server did not listen on port {port} within {STARTUP_TIMEOUT_SECONDS} seconds")


@contextmanager
def example_server(script: str) -> Iterator[str]:
    """Start one of the example servers on an ephemeral port and yield the URL to dial."""
    port = free_port()
    process = subprocess.Popen(  # noqa: S603 - a fixed script, run with this interpreter
        [sys.executable, str(EXAMPLES / script)],
        cwd=str(REPOSITORY),
        env={**os.environ, "MUXWS_PORT": str(port)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_until_listening(port, process)
        yield f"ws://127.0.0.1:{port}/ws"
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
        env={**os.environ, "MUXWS_URL": url},
        capture_output=True,
        text=True,
        timeout=CLIENT_TIMEOUT_SECONDS,
        check=False,
    )
    assert completed.returncode == 0, f"{script} exited with {completed.returncode}:\n{completed.stderr}"
    return completed.stdout


@needs_acceptor
def test_quickstart_runs_end_to_end() -> None:
    """The whole of the quick start: the documented output is the assertion, not a hope."""
    with example_server("quickstart_server.py") as url:
        assert run_client("quickstart_client.py", url) == documented_output("quickstart")


@needs_acceptor
def test_server_push_runs_end_to_end() -> None:
    """The symmetry claim, executed: the acceptor opens a stream and the dialer's handler gets it."""
    with example_server("push_server.py") as url:
        assert run_client("push_client.py", url) == documented_output("push")


@needs_acceptor
def test_reconnecting_client_runs_end_to_end() -> None:
    """`hello=` and `reconnect=` against an acceptor with no reconnect-specific code at all."""
    with example_server("quickstart_server.py") as url:
        assert run_client("reconnect_client.py", url) == documented_output("reconnect")


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
    """Every API-reference example runs as written. A fragment that cannot run is not an example."""
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
            capture_output=True,
            text=True,
            timeout=CLIENT_TIMEOUT_SECONDS,
            check=False,
        )
        if completed.returncode != 0:
            failures.append(f"{name} exited with {completed.returncode}:\n{completed.stderr}")
    assert not failures, "\n\n".join(failures)
