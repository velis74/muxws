"""One entry point for three demos, one flag each. `python demo.py` prints the help and starts nothing.

`--browser` is the one with a page. There are two backends and one frontend: `python demo.py
--browser` serves the sockets from Python and `python demo.py --browser node` serves them from
TypeScript on Node, and **the frontend does not change by a single line between them** - that is the
whole reason the second one exists. If a Vue application cannot tell which language answered, the
wire really is the contract.

The Vite dev server runs in a daemon child process; the backend runs in this process when it is
uvicorn and in a child process group of its own when it is Node - so a single Ctrl-C stops the set
either way. The backend listens on 127.0.0.1:8020 (override with `MUXWS_DEMO_PORT`) and the dev
server proxies `/ws` to it (`demo/frontend/vite.config.ts`), which is why neither half needs a CORS
story or a second origin.

This file is a *consumer* of muxws, not part of it. Nothing under `muxws/` imports it, no test
depends on it, and it is absent from both published artefacts - `[tool.hatch.build.targets.wheel]`
ships `muxws` alone and the npm package ships `dist/*` alone. `muxws/packaging_test.py` builds a
wheel and looks inside it rather than taking that on trust.

    pip install -e ".[demo,starlette]"    # the Python backend only
    npm install                           # both backends and the frontend
    python demo.py --browser              # or: python demo.py --browser node

`python demo.py --uds` runs the Unix-domain-socket pair from `docs/examples/`: an acceptor bound to
a socket file and a client dialling it as `ws+unix:///…/muxws.sock:/ws`. It needs only
`pip install -e ".[websockets]"`, since it starts neither uvicorn nor a browser, and it cannot be
part of the page: a page has no way to open a file as a socket.

`python demo.py --bench` carries one payload three ways over one transport - a raw socket, a raw
WebSocket, and muxws - and prints what each one moves per second. `muxws/throughput_test.py` reports
frames a second with nothing to divide by; the raw socket is that denominator and the raw WebSocket
is the step between the two, so the envelope's cost comes out as a percentage of what the transport
can carry rather than as a number on its own.

`[demo]` carries `websockets` deliberately: uvicorn has no WebSocket protocol implementation of its
own and answers 404 to every upgrade without one, while serving the page perfectly - so the demo
would load and only the socket would fail.
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

#: Python, because it is the interpreter that is already running this file.
DEFAULT_BACKEND = "python"

#: What `pip install -e ".[demo,starlette]"` provides, as `(import name, why it is needed)`.
#:
#: The **Python backend's** list and nobody else's: `--browser node` loads not one of these.
#:
#: Checked before anything starts, because every one of these fails *late* and in a way that points
#: somewhere else. A missing `fastapi` is an ImportError from inside a child process nobody is
#: watching; a missing `starlette` surfaces as the route never being reached.
PYTHON_BACKEND_IMPORTS = (
    ("fastapi", "the Python demo backend is a FastAPI app"),
    ("uvicorn", "which serves it"),
    ("starlette", "muxws.accept() upgrades a Starlette WebSocket"),
    ("muxws", "the library this demo exists to show; install the repository itself with -e"),
)

#: uvicorn implements no WebSocket protocol itself and needs one of these.
#:
#: Nothing in this repository imports `websockets` on the demo path, so scanning the demo's own
#: imports does not find this one. Without one of these uvicorn serves the page perfectly and answers
#: **404 to every upgrade**, logging its complaint into a server log nobody is reading - so the
#: browser shows a muxws handshake error and every part of the diagnosis points away from the cause.
#:
#: It is *uvicorn's* gap, not the protocol's: `demo/backend_node/main.ts` builds its own
#: `WebSocketServer`, so this check must not fire for the Node backend and send a reader to install a
#: Python package that backend will never import.
WEBSOCKET_IMPLEMENTATIONS = ("websockets", "wsproto")

#: What `npm install` provides for the **Node backend**, as `(package, why it is needed)`.
#:
#: Both fail inside `npm run`, in a child process, underneath a frontend that started fine - the same
#: shape of failure the Python list above exists to pre-empt, in the other language.
NODE_BACKEND_PACKAGES = (
    ("tsx", "which runs demo/backend_node/*.ts with no build step"),
    ("ws", "the WebSocket server muxws's Node acceptor upgrades"),
)


#: What the Unix-domain-socket demo needs, as `(import name, why it is needed)`.
#:
#: Two entries and not six: `--uds` starts neither FastAPI nor a browser, so demanding uvicorn, vue
#: or `node_modules` would send a reader to install several hundred megabytes for a run that opens
#: one file and prints five lines. `websockets` both dials and listens here.
UDS_IMPORTS = (
    ("websockets", "unix_serve accepts and connect() dials, both through it"),
    ("muxws", "the library this demo exists to show; install the repository itself with -e"),
)

#: What the throughput measurement needs, as `(import name, why it is needed)`.
#:
#: Two, and msgpack is not one of them: its cells are skipped with a note saying so, which is a
#: better answer than refusing to measure the rest. `websockets` is not optional in the same way -
#: two of the three modes are nothing without it, and every cell runs in a pair of child processes,
#: so its absence surfaces as an ImportError inside a child after the first cells have been measured.
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

    A named function rather than an inline `os.path.isdir`, because it is the only seam the tests
    have: the environment that runs them has every package installed, so the *missing* case can be
    witnessed only by replacing this.
    """
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.isdir(os.path.join(root, "node_modules", name))


def missing_dependencies(backend=DEFAULT_BACKEND, frontend=True, uds=False, bench=False):
    """Everything the chosen demo needs and does not have, as human-readable lines.

    Backend-aware, because the two need almost disjoint things and demanding the other's is a lie:
    `--browser node` imports no Python beyond this file, and refusing to start it over an absent
    uvicorn would send the reader to install a package that would never be loaded. `--uds` and
    `--bench` are the third and fourth such sets, and the smallest: both return early rather than
    falling through to the frontend check below, because neither run has a frontend to check for.
    """
    import importlib.util

    problems = []
    if uds or bench:
        for module, why in UDS_IMPORTS if uds else BENCH_IMPORTS:
            if importlib.util.find_spec(module) is None:
                problems.append(f"{module:12} - {why}")
        if uds and not hasattr(socket, "AF_UNIX"):
            # Not a missing package and not fixable by installing one, but this is the list a reader
            # is shown before anything starts, and a Windows reader has to learn it here rather than
            # from a `UnixSocketsUnsupportedError` out of the client three seconds later.
            #
            # Not asked of `--bench`, which measures the unix cells if it can and prints a note
            # naming them as skipped if it cannot: the tcp half of that table is still a measurement.
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

    # The frontend half, which is the same one for both backends - it is the claim being demonstrated.
    # `npm run demo:dev` failing is loud, but it fails *inside the child process* underneath a backend
    # that started fine, which reads as the demo being broken rather than as one command not having
    # been run.
    #
    # Skipped under `--no-fe` for the same reason the check is backend-aware at all: refusing to start
    # over a dependency this run will never load is a lie, and it is the kind that sends a reader to
    # install several hundred megabytes of `node_modules` to run a backend that does not use them.
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

    # Only the commands that would fix *this* run. Printing `pip install` at a reader who chose the
    # Node backend tells them to install four Python packages none of which their backend imports,
    # and the one thing they actually need is then the second line of advice rather than the only one.
    print("\nFrom the repository root:\n", file=sys.stderr)
    if backend != "node":
        print('    pip install -e ".[demo,starlette]"', file=sys.stderr)
    # `--no-fe` on the node backend needs `npm install` anyway - that is where `tsx`, `ws` and the
    # library's own TypeScript live - so the line is suppressed only where it would be noise.
    if frontend or backend == "node":
        print("    npm install\n", file=sys.stderr)
    raise SystemExit(1)


def build_parser():
    """The command line: one flag per demo, and a help page that says what each of the three shows."""
    parser = argparse.ArgumentParser(
        prog="python demo.py",
        # Raw, so these paragraphs survive as paragraphs. argparse's default formatter reflows
        # everything into one block, which is how a description becomes a parameter dump.
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Three demos of muxws, one flag each. With no flag this help is the whole of the run.\n"
            "\n"
            "--browser  a Vue application on http://127.0.0.1:5173 and, behind it, a backend on\n"
            "           :8020 pushing twenty live tick streams, a 1500-level depth book and a\n"
            "           million-byte export down one WebSocket, all at once and all cancellable.\n"
            "           The backend is either of two: the Python one under uvicorn, or the\n"
            "           TypeScript one under Node. They are ports of each other, and the frontend\n"
            "           is byte-for-byte the same against both - which is the most interesting\n"
            "           thing this demo has to show.\n"
            "\n"
            "--uds      no browser and no port: a daemon on a socket file and a client dialling it\n"
            "           with a ws+unix: URL. A page cannot open a file as a socket, so that\n"
            "           transport has nowhere to appear in the demo above.\n"
            "\n"
            "--bench    one payload carried three ways - a raw socket, a raw WebSocket, muxws -\n"
            "           over loopback and a socket file, so what the envelope costs is a\n"
            "           percentage of what this machine can do rather than a bare frame count."
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
    # Exactly one demo per run, and argparse refuses the pairs by name. Nothing here has a default:
    # a run that names no demo prints the help rather than starting whichever one came first.
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
    # A second accepted spelling of the positional above. `--help` documents the positional only, so
    # there is still exactly one way to learn this.
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

    A bare command line is not a run: it prints the help and exits 0. Every other combination that
    cannot mean anything is refused by name, because the alternative is a flag that reads as having
    been obeyed - `--no-fe` accepted on a demo that starts no frontend leaves a reader believing they
    turned something off.
    """
    parser = build_parser()
    arguments = parser.parse_args(argv)
    named_backend = arguments.backend or arguments.backend_option

    if arguments.backend and arguments.backend_option and arguments.backend != arguments.backend_option:
        # Silently preferring one would start a backend the reader did not ask for and then print a
        # frontend URL that works, so nothing about the run would look wrong.
        parser.error(f"asked for both '{arguments.backend}' and '--backend {arguments.backend_option}'; pick one")

    if arguments.uds or arguments.bench:
        # Refused rather than ignored: both are Python at both ends, so accepting `node` here would
        # answer a request for the TypeScript port by silently running the Python one. (The
        # TypeScript port does dial `ws+unix:` - `interop/drive.sh <a> <b> unix` runs the two against
        # each other - but neither of these demos is where that is shown.)
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
    # `npm run demo:dev` is a tree - npm, a shell, and vite under it - and `fe_proc.terminate()`
    # below reaches only *this* process. Terminating that alone orphans vite, which goes on holding
    # 5173: the next `python demo.py` finds the port taken, steps to 5174, and serves the reader a
    # dev server from the previous run.
    #
    # So: the tree gets its own process group, and whatever ends this function kills the group.
    # `start_new_session` also takes it out of the foreground group, which means Ctrl-C no longer
    # reaches npm on its own - that is fine, because the wait below ends either way and the
    # `finally` does the killing explicitly rather than relying on the terminal to do it.
    process = subprocess.Popen(["npm", "run", "demo:dev"], start_new_session=True)  # noqa: S603, S607
    # SIGTERM's default action kills this process outright and the `finally` never runs. Turning it
    # into SystemExit is the whole reason the cleanup below is reachable from `fe_proc.terminate()`.
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
    entry_point = os.getppid()
    try:
        # Watching the parent rather than only waiting on npm, because `fe_proc.terminate()` in the
        # block below is not reached on every exit: `uvicorn.run()` does not return to us when the
        # entry point is sent SIGTERM - an IDE's stop button, `kill`, a supervisor - so `finally`
        # there never executes and this process would be inherited by init with the dev server still
        # under it. `os.getppid()` changing is the one signal that arrives however the parent died,
        # SIGKILL included. Never `check=True` on the exit status either: npm exits non-zero for
        # every ordinary end of this process, and a `CalledProcessError` traceback out of a child
        # nobody is watching, underneath a backend still running fine, reads as a muxws failure.
        while process.poll() is None and os.getppid() == entry_point:
            time.sleep(0.25)
    finally:
        # The group, not the process: `npm run demo:dev` is npm, a shell, npm again, a shell and
        # vite, and signalling only the one we spawned leaves vite orphaned and still holding 5173.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGTERM)


def run_fastapi():
    # uvicorn is imported inside the function, not at module scope, because it belongs to the `demo`
    # extra. WSM-PKG-002 says `pip install muxws` pulls in nothing at all, and this file is shipped
    # source that ruff lints beside the library - a module-scope import here is the shape of the
    # mistake that rule exists to prevent, even though hatch never puts this file in a wheel.
    # It is also what makes `--browser node` runnable with no uvicorn installed at all.
    import uvicorn

    # `reload=False`: the reloader replaces this process with a supervisor and a fresh worker, and
    # the frontend child below belongs to *this* process. A reload would orphan it and the next one
    # would find port 5173 taken.
    # The port comes from the app rather than from a second literal here: two copies of a port
    # number drift, and the one that drifts is whichever the reader is not looking at.
    from demo.backend_python.main import HOST, PORT

    uvicorn.run("demo.backend_python.main:app", host=HOST, port=PORT, reload=False)


def run_node():
    """The TypeScript backend, spawned and torn down exactly the way `run_fe` spawns the dev server.

    `npm run` is a tree - npm, a shell, tsx, node - and signalling only the process we spawned leaves
    the leaf orphaned and still holding its port, so the *next* run meets a server from the previous
    one. Here that port is 8020, where the symptom is worse than the dev server's: the demo comes up,
    the sockets connect, and the reader is watching a backend they did not start - possibly the other
    language's.

    So: its own process group, and whatever ends this function kills the group.
    """
    process = subprocess.Popen(["npm", "run", "demo:backend:node"], start_new_session=True)  # noqa: S603, S607
    # SIGTERM's default action kills this process outright and the `finally` below never runs - which
    # is precisely how the group gets orphaned. `run_fastapi` needs no such handler because uvicorn
    # installs its own and returns; on this path there is nobody but us, and an IDE's stop button, a
    # `kill` or a supervisor all arrive as this signal.
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
    try:
        # No `check=True` and no `run()`: npm exits non-zero for every ordinary end of this process,
        # and a `CalledProcessError` traceback out of a backend the reader has just Ctrl-C'd reads as
        # a crash in the thing being demonstrated.
        process.wait()
    finally:
        # The group, not the process. See `run_fe`.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGTERM)
        # And then wait for it, so "Stopped." is not printed over a backend still holding 8020 - the
        # next run would find the port taken and fail for a reason belonging to the previous one.
        # `fe_proc.join()` below is the same guarantee for the dev server. Bounded, because a
        # backend that ignores SIGTERM must not turn Ctrl-C into a hang.
        with contextlib.suppress(subprocess.TimeoutExpired, KeyboardInterrupt):
            process.wait(timeout=5)


def example_environment():
    """`os.environ` with **this checkout** ahead of anything installed.

    Python puts the *script's* directory on `sys.path` and never the working directory, and
    `docs/examples/` holds no package - so without this an example subprocess can only import `muxws`
    when the interpreter happens to have a copy installed, and what it imports then is that copy
    rather than the tree the reader is standing in. `docs/examples/run_examples_test.py` builds the
    same environment for the same reason.
    """
    root = os.path.dirname(os.path.abspath(__file__))
    return {**os.environ, "PYTHONPATH": os.pathsep.join([root, *filter(None, [os.environ.get("PYTHONPATH")])])}


def wait_for_socket(path, process):
    """Block until `path` is a socket file, or the acceptor died trying.

    Polling the filesystem rather than sleeping a fixed interval, and checking the child on every
    turn: an acceptor that exits immediately - a stale socket it refuses to unlink, a missing
    dependency `check_before_starting` did not cover - would otherwise be waited on for the full
    timeout and then reported as slow rather than as dead.
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

    The acceptor's output is captured rather than interleaved. Its second line is the one thing here
    the client cannot know: `SO_PEERCRED` hands the acceptor the caller's pid, uid and gid straight
    from the kernel, so the connection is authenticated at the upgrade with nothing on the wire.
    Printing it after the client's output keeps the transcript in one order on every run.

    Both scripts are run as subprocesses instead of being imported: they are shipped documentation
    whose output the guide asserts byte for byte, and a reimplementation would be free to drift.
    """
    examples = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs", "examples")
    environment = example_environment()

    # A short directory on purpose: `sun_path` is capped at about 108 bytes and a longer one fails in
    # `bind()` naming the limit but not the component to shorten.
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
            # Bounded, then killed: an acceptor that ignores SIGTERM must not turn this into a hang,
            # and the temporary directory above cannot be removed while it is still bound.
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

    `demo/bench/` is imported here rather than at module scope for the reason `run_fastapi` imports
    uvicorn inside itself: this file is linted beside the library and shipped as source, and the two
    demos above must not pay for a package they never use.

    The measuring is the package's; this function chooses nothing about it beyond `--full`.
    """
    from demo.bench import format_report, run_matrix

    # On stderr, so `python demo.py --bench > report.txt` keeps the report alone in the file. Every
    # cell is a pair of processes measured for a fixed wall-clock budget, so the run is silent for as
    # long as the matrix takes and a reader has no other way to tell it apart from a hang.
    print("Measuring. The report prints when the last cell has finished.", file=sys.stderr)
    try:
        results, round_trips, notes = run_matrix(full=full)
    except KeyboardInterrupt:
        # The one demo of the three whose ordinary exit is Ctrl-C: it prints nothing for the minutes
        # the matrix takes, and a stack out of `subprocess.run` reads as a crash in the tool. The
        # cells already measured are not printed - a table missing whichever rows the interrupt
        # landed between is a table a reader would compare against a whole one.
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
        # A reader who typed it wants to see that it took - the frontend will look identical either
        # way, so this line is the only confirmation there is.
        print("Using the Node/TypeScript backend.")
    else:
        # Printed rather than left to `--help`, because a reader who never learns there are two never
        # tests the claim this demo exists to make: that the frontend cannot tell them apart. It is
        # the most interesting thing here and it is invisible until someone runs the other one.
        print("Using the Python backend (the default).")
        print("Run `python demo.py --browser node` for the equivalent backend in TypeScript on Node.")

    print("Starting the muxws demo...")
    if chosen_backend == "node":
        # No port printed on this path, and `MUXWS_DEMO_PORT` deliberately not read here: importing
        # the Python backend for its `PORT` would demand fastapi on a run that needs none of it, and
        # a second literal 8020 in this file is the copy that drifts. `demo/backend_node/main.ts`
        # prints its own address the moment the socket is listening, and that one cannot be wrong.
        print("  backend:  node, which prints its own address as soon as it is listening")
    else:
        from demo.backend_python.main import PORT

        print(f"  backend:  http://127.0.0.1:{PORT}")
    if options.frontend:
        print("  frontend: http://127.0.0.1:5173")
    else:
        # Said plainly, because the demo's whole point is on the page: a reader who passed `--no-fe`
        # by habit and then found nothing at :5173 would have every reason to think the demo broke.
        print("  frontend: not started (--no-fe); nothing is serving :5173")

    # Flushed before anything long-running starts. Python line-buffers stdout to a terminal and
    # block-buffers it to a pipe, and `uvicorn.run` below does not return - so under
    # `python demo.py > log` every line above sits in a buffer that SIGTERM then discards. The lost
    # lines include the one telling the reader the other backend exists, which is the only reason it
    # is printed at all.
    sys.stdout.flush()

    # None under `--no-fe`, and the teardown below reads that rather than a second flag: one thing to
    # get wrong instead of two.
    fe_proc = None
    if options.frontend:
        fe_proc = multiprocessing.Process(target=run_fe, daemon=True)
        fe_proc.start()
    try:
        run_backend(chosen_backend)
    except KeyboardInterrupt:
        pass
    finally:
        # A daemon child is killed at interpreter shutdown, which is after this block: without the
        # join, "Stopped." prints while the dev server is still up. It is also the only teardown
        # there is when the backend exits for a reason of its own - a taken port, say - rather than by
        # the Ctrl-C that would have reached the whole process group.
        if fe_proc is not None:
            fe_proc.terminate()
            fe_proc.join()
        print("Stopped.")
