"""One entry point for the demo: `python demo.py` starts both halves.

The Vite dev server runs in a daemon child process and uvicorn runs in this one, so a single Ctrl-C
stops the pair. The backend listens on 127.0.0.1:8020 (override with `MUXWS_DEMO_PORT`) and
the dev server proxies `/ws` to it
(`demo/frontend/vite.config.ts`), which is why neither half needs a CORS story or a second origin.

This file is a *consumer* of muxws, not part of it. Nothing under `muxws/` imports it, no test
depends on it, and it is absent from both published artefacts - `[tool.hatch.build.targets.wheel]`
ships `muxws` alone and the npm package ships `dist/*` alone. `muxws/packaging_test.py` builds a
wheel and looks inside it rather than taking that on trust.

    pip install -e ".[demo,starlette]"
    npm install
    python demo.py

`[demo]` carries `websockets` deliberately: uvicorn has no WebSocket protocol implementation of its
own and answers 404 to every upgrade without one, while serving the page perfectly - so the demo
would load and only the socket would fail.
"""

import contextlib
import multiprocessing
import os
import signal
import subprocess
import sys
import time

#: What `pip install -e ".[demo,starlette]"` provides, as `(import name, why it is needed)`.
#:
#: Checked before anything starts, because every one of these fails *late* and in a way that points
#: somewhere else. A missing `fastapi` is an ImportError from inside a child process nobody is
#: watching; a missing `starlette` surfaces as the route never being reached.
DIRECT_IMPORTS = (
    ("fastapi", "the demo backend is a FastAPI app"),
    ("uvicorn", "which serves it"),
    ("starlette", "muxws.accept() upgrades a Starlette WebSocket"),
    ("muxws", "the library this demo exists to show; install the repository itself with -e"),
)

#: uvicorn implements no WebSocket protocol itself and needs one of these.
#:
#: This is the check that matters, and an import test of the demo's *own* imports would never have
#: found it: nothing in this repository imports `websockets` on the demo path. Without one of these
#: uvicorn serves the page perfectly and answers **404 to every upgrade**, logging its complaint into
#: a server log nobody is reading - so the browser shows a muxws handshake error and every part of
#: the diagnosis points away from the cause. That is exactly how the first reader of this demo lost
#: an hour.
WEBSOCKET_IMPLEMENTATIONS = ("websockets", "wsproto")


def missing_dependencies():
    """Everything the demo needs and does not have, as human-readable lines."""
    import importlib.util

    problems = []
    for module, why in DIRECT_IMPORTS:
        if importlib.util.find_spec(module) is None:
            problems.append(f"{module:12} - {why}")

    if not any(importlib.util.find_spec(name) for name in WEBSOCKET_IMPLEMENTATIONS):
        problems.append(
            f"{'websockets':12} - uvicorn has no WebSocket implementation of its own. Without this "
            "it serves the page and answers 404 to every upgrade, which surfaces in the browser as a "
            "muxws handshake error rather than as a missing package."
        )
    return problems


def check_before_starting():
    """Refuse to start with a list of what to install, rather than failing later and elsewhere."""
    problems = missing_dependencies()

    # The frontend half. `npm run demo:dev` failing is loud, but it fails *inside the child process*
    # underneath a backend that started fine, which reads as the demo being broken rather than as one
    # command not having been run.
    root = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isdir(os.path.join(root, "node_modules", "vue")):
        problems.append(f"{'npm install':12} - the frontend's dependencies are not installed")

    if not problems:
        return

    print("This demo cannot start. Missing:\n", file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    print(
        '\nFrom the repository root:\n\n    pip install -e ".[demo,starlette]"\n    npm install\n',
        file=sys.stderr,
    )
    raise SystemExit(1)


def run_fe():
    # `npm run demo:dev` is a tree - npm, a shell, and vite under it - and `fe_proc.terminate()`
    # below reaches only *this* process. Terminating it left vite orphaned and still holding 5173:
    # the next `python demo.py` then found the port taken, stepped to 5174, and served the reader a
    # dev server from the previous run. Measured on this machine, not hypothetical.
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
        # vite, and signalling only the one we spawned left vite orphaned and still holding 5173 -
        # so the next `python demo.py` served the reader a dev server from the previous run.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGTERM)


def run_fastapi():
    # uvicorn is imported inside the function, not at module scope, because it belongs to the `demo`
    # extra. WSM-PKG-002 says `pip install muxws` pulls in nothing at all, and this file is shipped
    # source that ruff lints beside the library - a module-scope import here is the shape of the
    # mistake that rule exists to prevent, even though hatch never puts this file in a wheel.
    import uvicorn

    # `reload=False`: the reloader replaces this process with a supervisor and a fresh worker, and
    # the frontend child below belongs to *this* process. A reload would orphan it and the next one
    # would find port 5173 taken.
    # The port comes from the app rather than from a second literal here: two copies of a port
    # number drift, and the one that drifts is whichever the reader is not looking at.
    from demo.backend.main import HOST, PORT

    uvicorn.run("demo.backend.main:app", host=HOST, port=PORT, reload=False)


if __name__ == "__main__":
    check_before_starting()

    print("Starting the muxws demo...")
    from demo.backend.main import PORT

    print(f"  backend:  http://127.0.0.1:{PORT}")
    print("  frontend: http://127.0.0.1:5173")
    fe_proc = multiprocessing.Process(target=run_fe, daemon=True)
    fe_proc.start()
    try:
        run_fastapi()
    except KeyboardInterrupt:
        pass
    finally:
        # A daemon child is killed at interpreter shutdown, which is after this block: without the
        # join, "Stopped." prints while the dev server is still up. It is also the only teardown
        # there is when uvicorn exits for a reason of its own - a taken port, say - rather than by
        # the Ctrl-C that would have reached the whole process group.
        fe_proc.terminate()
        fe_proc.join()
        print("Stopped.")
