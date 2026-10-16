"""Entry point for three demos, one flag each. `python demo.py` without a flag prints the help.

`--browser` serves a web page. There are two backends and one frontend: `python demo.py --browser`
serves the sockets from Python and `python demo.py --browser node` serves them from TypeScript on
Node. The frontend is identical for both backends; the Node backend exists to show that the frontend
works unchanged against either implementation of the wire protocol.

The Vite dev server runs in a daemon child process. The backend runs in this process when it is
uvicorn and in its own child process group when it is Node; one Ctrl-C stops both. The backend
listens on 127.0.0.1:8020 (override with `MUXWS_DEMO_PORT`) and the dev server proxies `/ws` to it
(`demo/frontend/vite.config.ts`), so the page and the socket share one origin and need no CORS.

This file uses muxws and is not part of it. Nothing under `muxws/` imports it, and it is not in
either published artefact: `[tool.hatch.build.targets.wheel]` ships only `muxws` and the npm package
ships only `dist/*`. `muxws/packaging_test.py` builds a wheel and checks its contents.

    pip install -e ".[demo,starlette]"    # the Python backend only
    npm install                           # both backends and the frontend
    python demo.py --browser              # or: python demo.py --browser node

`python demo.py --uds` runs the Unix-domain-socket pair from `docs/examples/`: an acceptor bound to
a socket file and a client dialling it as `ws+unix:///…/muxws.sock:/ws`. It needs only
`pip install -e ".[websockets]"`, since it starts neither uvicorn nor a browser. A browser page
cannot open a socket file, so this transport is not part of `--browser`.

`python demo.py --bench` sends one payload three ways over one transport (a raw socket, a raw
WebSocket, and muxws) and prints the throughput of each. The raw socket is the baseline and the raw
WebSocket is the intermediate step, so muxws's overhead is reported as a percentage of what the
transport carries. `muxws/throughput_test.py` reports only frames per second.

`[demo]` includes `websockets`: uvicorn has no WebSocket protocol implementation of its own, and
without one it answers 404 to every upgrade request while serving the page.
"""

import argparse
import contextlib
import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

#: The two backends, in the order `--help` should list them.
BACKENDS = ("python", "node")

#: Python, the interpreter running this file.
DEFAULT_BACKEND = "python"

#: What `pip install -e ".[demo,starlette]"` provides, as `(import name, why it is needed)`.
#:
#: Used by the Python backend only; `--browser node` loads none of these.
#:
#: Checked before anything starts. Otherwise a missing `fastapi` is an ImportError inside a child
#: process, and a missing `starlette` shows up as the route never being reached.
PYTHON_BACKEND_IMPORTS = (
    ("fastapi", "the Python demo backend is a FastAPI app"),
    ("uvicorn", "which serves it"),
    ("starlette", "muxws.accept() upgrades a Starlette WebSocket"),
    ("muxws", "the library this demo exists to show; install the repository itself with -e"),
)

#: uvicorn implements no WebSocket protocol itself and needs one of these.
#:
#: Nothing on the demo path imports `websockets`, so it does not appear among the demo's imports.
#: Without one of these uvicorn serves the page, answers 404 to every upgrade request and logs the
#: cause only in the server log; the browser shows a muxws handshake error.
#:
#: The requirement is uvicorn's: `demo/backend_node/main.ts` creates its own `WebSocketServer`, so
#: this check does not apply to the Node backend.
WEBSOCKET_IMPLEMENTATIONS = ("websockets", "wsproto")

#: What `npm install` provides for the **Node backend**, as `(package, why it is needed)`.
#:
#: Checked before starting; otherwise a missing package fails inside `npm run`, in a child process,
#: after the frontend has started.
NODE_BACKEND_PACKAGES = (
    ("tsx", "which runs demo/backend_node/*.ts with no build step"),
    ("ws", "the WebSocket server muxws's Node acceptor upgrades"),
)


#: What the Unix-domain-socket demo needs, as `(import name, why it is needed)`.
#:
#: `--uds` starts neither FastAPI nor a browser, so it does not require uvicorn, vue or
#: `node_modules`. `websockets` is used both to dial and to listen.
UDS_IMPORTS = (
    ("websockets", "unix_serve accepts and connect() dials, both through it"),
    ("muxws", "the library this demo exists to show; install the repository itself with -e"),
)

#: What the throughput measurement needs, as `(import name, why it is needed)`.
#:
#: msgpack is not required: without it the msgpack cells are skipped with a note. `websockets` is
#: required: two of the three modes use it, and every cell runs in a pair of child processes, so a
#: missing package would raise ImportError in a child after the first cells were measured.
BENCH_IMPORTS = (
    ("websockets", "both WebSocket modes dial and accept through it"),
    ("muxws", "the library this demo exists to show; install the repository itself with -e"),
)

#: The two shipped scripts `--uds` runs, relative to `docs/examples/`.
UDS_SERVER = "uds_server.py"
UDS_CLIENT = "uds_client.py"

#: How long the acceptor gets to bind its socket file, in seconds, and how often to look.
UDS_STARTUP_TIMEOUT_SECONDS = 15.0
UDS_POLL_INTERVAL_SECONDS = 0.05


def node_package_installed(name):
    """Whether `npm install` put `name` in this repository's `node_modules`.

    A separate function so that tests can replace it to simulate a missing package; the test
    environment has every package installed.
    """
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.isdir(os.path.join(root, "node_modules", name))


def missing_dependencies(backend=DEFAULT_BACKEND, frontend=True, uds=False, bench=False):
    """Everything the chosen demo needs and does not have, as human-readable lines.

    Each demo has its own requirement set: the two backends need almost disjoint packages, and
    `--browser node` imports no Python beyond this file. `--uds` and `--bench` return before the
    frontend check because neither starts a frontend.
    """
    import importlib.util

    problems = []
    if uds or bench:
        for module, why in UDS_IMPORTS if uds else BENCH_IMPORTS:
            if importlib.util.find_spec(module) is None:
                problems.append(f"{module:12} - {why}")
        if uds and not hasattr(socket, "AF_UNIX"):
            # Not a package, but reported in the same list before anything starts, instead of as a
            # `UnixSocketsUnsupportedError` from the client.
            #
            # Not checked for `--bench`, which skips the unix cells with a note and still measures tcp.
            problems.append(f"{'AF_UNIX':12} - this platform has no Unix domain sockets; the demo cannot run here")
        return problems

    if backend == "node":
        for package, why in NODE_BACKEND_PACKAGES:
            if not node_package_installed(package):
                problems.append(f"{package:12} - {why}")
    else:
        for module, why in PYTHON_BACKEND_IMPORTS:
            if importlib.util.find_spec(module) is None:
                problems.append(f"{module:12} - {why}")

        if not any(importlib.util.find_spec(name) for name in WEBSOCKET_IMPLEMENTATIONS):
            problems.append(
                f"{'websockets':12} - uvicorn has no WebSocket implementation of its own. Without this "
                "it serves the page and answers 404 to every upgrade, which surfaces in the browser as a "
                "muxws handshake error rather than as a missing package."
            )

    # The frontend is the same for both backends. Without this check a missing `npm install` fails
    # inside the `npm run demo:dev` child process after the backend has started.
    #
    # Skipped under `--no-fe`, which does not start the frontend.
    if frontend and not node_package_installed("vue"):
        problems.append(f"{'npm install':12} - the frontend's dependencies are not installed")
    return problems


def check_before_starting(backend=DEFAULT_BACKEND, frontend=True, uds=False, bench=False):
    """Refuse to start with a list of what to install, rather than failing later and elsewhere."""
    problems = missing_dependencies(backend, frontend, uds=uds, bench=bench)
    if not problems:
        return

    if uds or bench:
        headline = "The Unix-socket demo" if uds else "The measurement"
        print(f"{headline} cannot start. Missing:\n", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print('\nFrom the repository root:\n\n    pip install -e ".[websockets]"\n', file=sys.stderr)
        raise SystemExit(1)

    print("This demo cannot start. Missing:\n", file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)

    # Only the commands this run needs: the Node backend imports none of the Python packages.
    print("\nFrom the repository root:\n", file=sys.stderr)
    if backend != "node":
        print('    pip install -e ".[demo,starlette]"', file=sys.stderr)
    # The Node backend needs `npm install` even with `--no-fe`, for `tsx`, `ws` and the library's
    # TypeScript sources.
    if frontend or backend == "node":
        print("    npm install\n", file=sys.stderr)
    raise SystemExit(1)


def build_parser():
    """The command line: one flag per demo, and a help page that says what each of the three shows."""
    parser = argparse.ArgumentParser(
        prog="python demo.py",
        # Raw, so the paragraphs and line breaks below are kept; the default formatter reflows the
        # description into one block.
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Three demos of muxws, one flag each. Without a flag, only this help is printed.\n"
            "\n"
            "--browser  a Vue application on http://127.0.0.1:5173 and a backend on :8020 that\n"
            "           sends twenty live tick streams, a 1500-level depth book and a\n"
            "           million-byte export over one WebSocket, concurrently and cancellable.\n"
            "           The backend is either the Python one under uvicorn or the TypeScript\n"
            "           one under Node. The two implement the same behaviour, and the frontend\n"
            "           is the same for both.\n"
            "\n"
            "--uds      no browser and no port: a daemon on a socket file and a client dialling it\n"
            "           with a ws+unix: URL. A browser page cannot open a socket file, so this\n"
            "           transport is not part of --browser.\n"
            "\n"
            "--bench    one payload sent three ways (a raw socket, a raw WebSocket, muxws) over\n"
            "           loopback and a socket file; muxws's overhead is reported as a percentage\n"
            "           of the raw transport's throughput."
        ),
        epilog=(
            "examples:\n"
            "  python demo.py --browser          the Python backend (demo/backend_python), uvicorn\n"
            "  python demo.py --browser node     the TypeScript backend (demo/backend_node), tsx\n"
            "  python demo.py --browser --no-fe  either backend alone, for a client of your own\n"
            "  python demo.py --uds              the socket-file pair: docs/examples/uds_*.py\n"
            "  python demo.py --bench            the throughput report: json, one stream\n"
            "  python demo.py --bench --full     msgpack and twenty concurrent streams as well\n"
            "\n"
            "Ctrl-C stops the backend and the dev server together. MUXWS_DEMO_PORT moves the\n"
            "backend off 8020; the Vite proxy in demo/frontend/vite.config.ts has to be told too."
        ),
    )
    # At most one demo per run; argparse rejects combinations. No demo is the default: without a
    # flag the help is printed.
    demos = parser.add_mutually_exclusive_group()
    demos.add_argument(
        "--browser",
        action="store_true",
        help="the Vue application and a backend behind it, on :5173 and :8020",
    )
    demos.add_argument(
        "--uds",
        action="store_true",
        help="the Unix-domain-socket pair: a daemon on a socket file and a client dialling it",
    )
    demos.add_argument(
        "--bench",
        action="store_true",
        help="measure a raw socket, a raw WebSocket and muxws against the same payload",
    )
    parser.add_argument(
        "backend",
        nargs="?",
        choices=BACKENDS,
        default=None,
        help="which language serves --browser's sockets; omit it and you get python",
    )
    # An alternative spelling of the positional above, not shown in `--help`.
    parser.add_argument("--backend", dest="backend_option", choices=BACKENDS, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-fe",
        dest="frontend",
        action="store_false",
        help="with --browser: start the backend alone, without the Vite dev server",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="with --bench: every codec, payload and stream count rather than the trimmed default",
    )
    return parser


def parse_arguments(argv=None):
    """`argv` parsed, with `backend` resolved to one of `BACKENDS` and never to None.

    Without a demo flag it prints the help and exits 0. A flag that has no effect with the chosen
    demo (for example `--no-fe` with `--uds`) is rejected with an error instead of being ignored.
    """
    parser = build_parser()
    arguments = parser.parse_args(argv)
    named_backend = arguments.backend or arguments.backend_option

    if arguments.backend and arguments.backend_option and arguments.backend != arguments.backend_option:
        # Two different backends named; neither is chosen silently.
        parser.error(f"asked for both '{arguments.backend}' and '--backend {arguments.backend_option}'; pick one")

    if arguments.uds or arguments.bench:
        # Rejected: both demos run Python at both ends, so `node` cannot be honoured. (The TypeScript
        # implementation dials `ws+unix:` in `interop/drive.sh <a> <b> unix`, not in these demos.)
        demo = "--uds runs the socket-file demo" if arguments.uds else "--bench measures the transports directly"
        if named_backend:
            parser.error(f"{demo}, which takes no backend argument")
        if not arguments.frontend:
            parser.error(f"{demo} and starts no frontend, so --no-fe has nothing to turn off")
    elif not arguments.browser:
        if named_backend:
            parser.error("a backend argument names which language serves the page, so it needs --browser")
        if not arguments.frontend:
            parser.error("--no-fe turns off the page's dev server, so it needs --browser")

    if arguments.full and not arguments.bench:
        parser.error("--full widens the measurement matrix, so it needs --bench")

    if not (arguments.browser or arguments.uds or arguments.bench):
        parser.print_help()
        raise SystemExit(0)

    arguments.backend = named_backend or DEFAULT_BACKEND
    return arguments


def run_fe():
    # `npm run demo:dev` starts a process tree (npm, a shell, vite), and `fe_proc.terminate()` reaches
    # only this process. If only this process ended, vite would keep port 5173, and the next run would
    # move to 5174 while the old dev server kept running.
    #
    # The tree therefore gets its own process group, and the `finally` below kills the group.
    # `start_new_session` also removes it from the terminal's foreground group, so Ctrl-C does not
    # reach npm directly; the `finally` sends the signal instead.
    process = subprocess.Popen(["npm", "run", "demo:dev"], start_new_session=True)  # noqa: S603, S607
    # SIGTERM's default action terminates the process without running `finally`. Converting it to
    # SystemExit makes the cleanup below run on `fe_proc.terminate()`.
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
    entry_point = os.getppid()
    try:
        # Also watches the parent: `fe_proc.terminate()` is not reached on every exit. When the entry
        # point receives SIGTERM (an IDE's stop button, `kill`, a supervisor), `uvicorn.run()` does not
        # return, its `finally` does not run, and this process would be reparented to init with the
        # dev server still running. `os.getppid()` changes however the parent exits, including
        # SIGKILL. The exit status is not checked: npm exits non-zero on every normal stop.
        while process.poll() is None and os.getppid() == entry_point:
            time.sleep(0.25)
    finally:
        # The whole group: `npm run demo:dev` is npm, a shell, npm again, a shell and vite, and
        # signalling only the spawned process leaves vite running on 5173.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGTERM)


def run_fastapi():
    # uvicorn is imported inside the function because it belongs to the `demo` extra (WSM-PKG-002:
    # `pip install muxws` installs no dependencies). This also lets `--browser node` run without
    # uvicorn installed.
    import uvicorn

    # `reload=False`: the reloader replaces this process with a supervisor and a new worker, which
    # would orphan the frontend child process; the next run would find port 5173 taken.
    # The port is imported from the app so that it is defined in one place.
    from demo.backend_python.main import HOST, PORT

    uvicorn.run("demo.backend_python.main:app", host=HOST, port=PORT, reload=False)


def run_node():
    """Run the TypeScript backend, started and stopped the same way `run_fe` handles the dev server.

    `npm run` starts a process tree (npm, a shell, tsx, node). Signalling only the spawned process
    leaves node running on port 8020, and the next run's frontend connects to that old backend.
    The tree therefore gets its own process group, and the `finally` below kills the group.
    """
    process = subprocess.Popen(["npm", "run", "demo:backend:node"], start_new_session=True)  # noqa: S603, S607
    # SIGTERM's default action terminates the process without running the `finally` below, which
    # would leave the group running. `run_fastapi` needs no handler because uvicorn installs its own.
    # An IDE's stop button, `kill` and supervisors send SIGTERM.
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
    try:
        # The exit status is not checked: npm exits non-zero on every normal stop, including Ctrl-C.
        process.wait()
    finally:
        # The group, not the process. See `run_fe`.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGTERM)
        # Waits for the backend to exit so that "Stopped." is printed after port 8020 is released.
        # `fe_proc.join()` does the same for the dev server. Bounded, so a backend that ignores
        # SIGTERM does not block the exit.
        with contextlib.suppress(subprocess.TimeoutExpired, KeyboardInterrupt):
            process.wait(timeout=5)


def example_environment():
    """`os.environ` with this checkout first on `PYTHONPATH`.

    Python puts the script's directory on `sys.path`, not the working directory, and `docs/examples/`
    contains no package. Without this an example subprocess imports an installed `muxws`, if any,
    instead of this checkout. `docs/examples/run_examples_test.py` builds the same environment.
    """
    root = os.path.dirname(os.path.abspath(__file__))
    return {**os.environ, "PYTHONPATH": os.pathsep.join([root, *filter(None, [os.environ.get("PYTHONPATH")])])}


def wait_for_socket(path, process):
    """Block until `path` is a socket file, or the acceptor died trying.

    Checks the child process on every poll, so an acceptor that exits immediately (for example on a
    stale socket file or a missing dependency) is reported at once instead of after the timeout.
    """
    deadline = time.monotonic() + UDS_STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SystemExit(
                f"the acceptor exited with {process.returncode} before binding:\n{process.communicate()[0]}"
            )
        if os.path.exists(path):
            return
        time.sleep(UDS_POLL_INTERVAL_SECONDS)
    raise SystemExit(f"the acceptor did not bind {path} within {UDS_STARTUP_TIMEOUT_SECONDS} seconds")


def run_uds():
    """The Unix-domain-socket demo: the shipped acceptor in a child, the shipped dialer against it.

    The acceptor's output is captured and printed after the client's output, so the transcript has
    the same order on every run. Its second line shows the caller's pid, uid and gid, which the
    acceptor receives from the kernel through `SO_PEERCRED` at the upgrade, without any data on the
    wire.

    Both scripts run as subprocesses, unmodified: they are the documentation examples whose output
    the guide quotes.
    """
    examples = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs", "examples")
    environment = example_environment()

    # A short directory: `sun_path` is limited to about 108 bytes, and `bind()` fails for longer paths.
    with tempfile.TemporaryDirectory(prefix="muxws-") as directory:
        socket_path = os.path.join(directory, "muxws.sock")
        acceptor = subprocess.Popen(  # noqa: S603 - a shipped script, run with this interpreter
            [sys.executable, os.path.join(examples, UDS_SERVER)],
            env={**environment, "MUXWS_SOCKET": socket_path},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            wait_for_socket(socket_path, acceptor)
            print(f"  socket:   {socket_path}")
            print(f"  dialled:  ws+unix://{socket_path}:/ws\n")
            sys.stdout.flush()
            dialer = subprocess.run(  # noqa: S603 - the same
                [sys.executable, os.path.join(examples, UDS_CLIENT)],
                env={**environment, "MUXWS_URL": f"ws+unix://{socket_path}:/ws"},
                check=False,
            )
        finally:
            acceptor.terminate()
            # Bounded wait, then SIGKILL, so an acceptor that ignores SIGTERM does not block the exit
            # and the temporary directory can be removed.
            try:
                transcript = acceptor.communicate(timeout=5)[0]
            except subprocess.TimeoutExpired:
                acceptor.kill()
                transcript = acceptor.communicate()[0]

    if transcript.strip():
        print("what the acceptor saw:")
        for line in transcript.strip().splitlines():
            print(f"  {line}")
    raise SystemExit(dialer.returncode)


def run_bench(full=False):
    """The throughput matrix, measured and then printed as one report.

    `demo/bench/` is imported inside the function so the other demos do not load it.
    `demo.bench` performs the measurement; this function only passes `--full`.
    """
    from demo.bench import format_report, run_matrix

    # On stderr, so `python demo.py --bench > report.txt` writes only the report to the file. Each cell
    # runs a pair of processes for a fixed time and prints nothing until the matrix is finished.
    print("Measuring. The report prints when the last cell has finished.", file=sys.stderr)
    try:
        results, round_trips, notes = run_matrix(full=full)
    except KeyboardInterrupt:
        # Ctrl-C exits with a message instead of a traceback from `subprocess.run`. Cells already
        # measured are not printed, because an incomplete table is not comparable with a complete one.
        raise SystemExit("Stopped. Nothing was reported: the matrix was not finished.") from None
    print(format_report(results, round_trips, notes))


def run_backend(backend):
    """Whichever backend was asked for: uvicorn in this process, or Node in a group under it."""
    if backend == "node":
        run_node()
    else:
        run_fastapi()


if __name__ == "__main__":
    options = parse_arguments()

    if options.uds:
        check_before_starting(uds=True)
        print("Starting the muxws Unix-socket demo...")
        print("  transport: a socket file, dialled with the ordinary connect() and a ws+unix: URL")
        run_uds()

    if options.bench:
        check_before_starting(bench=True)
        run_bench(options.full)
        raise SystemExit(0)

    chosen_backend = options.backend
    check_before_starting(chosen_backend, options.frontend)

    if chosen_backend == "node":
        # The frontend looks the same with either backend; this line confirms the choice.
        print("Using the Node/TypeScript backend.")
    else:
        # Points to the Node backend, which serves the same frontend.
        print("Using the Python backend (the default).")
        print("Run `python demo.py --browser node` for the equivalent backend in TypeScript on Node.")

    print("Starting the muxws demo...")
    if chosen_backend == "node":
        # No port is printed here: importing `PORT` from the Python backend would require fastapi.
        # `demo/backend_node/main.ts` prints its own address once it is listening.
        print("  backend:  node, which prints its own address as soon as it is listening")
    else:
        from demo.backend_python.main import PORT

        print(f"  backend:  http://127.0.0.1:{PORT}")
    if options.frontend:
        print("  frontend: http://127.0.0.1:5173")
    else:
        # States that nothing serves :5173 under `--no-fe`.
        print("  frontend: not started (--no-fe); nothing is serving :5173")

    # Flushed before the long-running part. Python block-buffers stdout to a pipe and `uvicorn.run`
    # does not return, so under `python demo.py > log` the lines above would stay in the buffer and be
    # lost on SIGTERM.
    sys.stdout.flush()

    # None under `--no-fe`; the teardown below checks this instead of the flag.
    fe_proc = None
    if options.frontend:
        fe_proc = multiprocessing.Process(target=run_fe, daemon=True)
        fe_proc.start()
    try:
        run_backend(chosen_backend)
    except KeyboardInterrupt:
        pass
    finally:
        # A daemon child is killed at interpreter shutdown, after this block; without the join,
        # "Stopped." is printed while the dev server is still running. This is also the only teardown
        # when the backend exits on its own (for example on a taken port) instead of on Ctrl-C.
        if fe_proc is not None:
            fe_proc.terminate()
            fe_proc.join()
        print("Stopped.")
