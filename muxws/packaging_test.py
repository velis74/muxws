"""Packaging invariants read from the **source manifests** (WSM-PKG-001, WSM-PKG-002).

`version_test.py` asserts related facts through the installed distribution: `muxws.__version__`
against `package.json`, and `importlib.metadata.requires` against the extras. An editable install
carries the metadata from the last `pip install -e .`, so a runtime dependency added to
`pyproject.toml` afterwards does not appear there until the package is reinstalled. A wheel is built
from the manifest on disk, and WSM-PKG-001/002 apply to it.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
import tarfile
import textwrap
import zipfile

from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
PACKAGE_JSON = ROOT / "package.json"
DEMO_ENTRY_POINT = ROOT / "demo.py"
DEMO_FRONTEND_JSON = ROOT / "demo" / "frontend" / "package.json"

# --------------------------------------------------------------------------- a very small TOML reader


def _strip_comment(line: str) -> str:
    """Drop a trailing `#` comment, leaving `#` inside a string alone."""
    inside = False
    for index, char in enumerate(line):
        if char == '"':
            inside = not inside
        elif char == "#" and not inside:
            return line[:index].rstrip()
    return line.rstrip()


def _balanced(text: str) -> bool:
    depth = 0
    inside = False
    for char in text:
        if char == '"':
            inside = not inside
        elif not inside and char in "[{":
            depth += 1
        elif not inside and char in "]}":
            depth -= 1
    return depth == 0


def _tables(text: str) -> dict[str, dict[str, Any]]:
    """`{table name: {key: value}}` for the handful of keys WSM-PKG-001/002 are about.

    Not `tomllib`: `requires-python` is `>=3.10` and CI runs the suite on 3.10, where `tomllib` does
    not exist and `tomli` is not a dev dependency. Skipping on 3.10 would leave both rules unchecked
    on the oldest supported interpreter.

    Every value these tests read (a list of strings, a string) is also a Python literal, so
    `ast.literal_eval` parses it. A value that is not (`authors`, `license`: TOML inline tables) is
    kept as raw text rather than raising, so a valid manifest with such keys still parses.
    `test_the_toml_reader_agrees_with_tomllib` pins it against the real parser everywhere `tomllib`
    exists, which is three of the four interpreters CI runs.
    """
    tables: dict[str, dict[str, Any]] = {"": {}}
    current = tables[""]
    key = ""
    buffer = ""
    for raw in text.splitlines():
        line = _strip_comment(raw.strip())
        if buffer:
            buffer = f"{buffer} {line}"
            if _balanced(buffer):
                current[key] = _literal(buffer)
                buffer = ""
            continue
        if not line:
            continue
        if line.startswith("["):
            current = tables.setdefault(line.strip("[]"), {})
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().strip('"')
        value = value.strip()
        if _balanced(value):
            current[key] = _literal(value)
        else:
            buffer = value
    return tables


def _literal(text: str) -> Any:
    try:
        return ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return text


@pytest.fixture(scope="module")
def pyproject() -> dict[str, dict[str, Any]]:
    return _tables(PYPROJECT.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def npm_manifest() -> dict[str, Any]:
    return json.loads(PACKAGE_JSON.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- one version stream


def test_python_and_typescript_versions_match(pyproject: dict[str, dict[str, Any]]):
    """WSM-PKG-001: two manifests, one version stream.

    `pyproject.toml` declares the version `dynamic` and points hatch at `muxws/__init__.py`, so the
    test reads the version from that file. The indirection is asserted too: if the manifest declared a
    static version or pointed hatch at another file, comparing `package.json` against
    `muxws/__init__.py` would check a file no wheel is built from.
    """
    project = pyproject["project"]
    assert "version" not in project, "a literal version here would be a second source of truth"
    assert project["dynamic"] == ["version"], "the version must be declared dynamic"

    source = ROOT / str(pyproject["tool.hatch.version"]["path"])
    assert source == ROOT / "muxws" / "__init__.py", f"hatch reads the version from {source}"

    found = re.findall(r'^__version__ = "([^"]+)"', source.read_text(encoding="utf-8"), re.MULTILINE)
    assert len(found) == 1, f"exactly one __version__ assignment, found {len(found)}"
    python_version = found[0]

    npm_version = json.loads(PACKAGE_JSON.read_text(encoding="utf-8"))["version"]
    # Two empty strings, or two `None` values, would also be equal, so the format is checked first.
    assert re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?", python_version), python_version
    assert python_version == npm_version


def test_the_version_appears_in_exactly_one_python_source(pyproject: dict[str, dict[str, Any]]):
    """WSM-PKG-005: no second, finer version number anywhere - the generation is on the wire alone.

    A `__version__` copied into a second module can diverge from the one hatch reads while neither
    manifest changes.
    """
    _ = pyproject
    carriers = sorted(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "muxws").rglob("*.py")
        if re.search(r"^__version__ = ", path.read_text(encoding="utf-8"), re.MULTILINE)
    )
    assert carriers == ["muxws/__init__.py"]


# --------------------------------------------------------------------------- nothing to install


def test_python_package_has_no_required_runtime_dependencies(pyproject: dict[str, dict[str, Any]]):
    """WSM-PKG-002: `pip install muxws` installs no dependency."""
    assert pyproject["project"]["dependencies"] == []


def test_every_third_party_import_sits_behind_an_extra(pyproject: dict[str, dict[str, Any]]):
    """WSM-PKG-002's second clause: each third-party package is declared as an extra.

    An empty `dependencies` list is also what a manifest with the extras removed and the imports kept
    looks like. Checking that the three packages the rule names are extras distinguishes optional
    from undeclared.
    """
    extras = pyproject["project.optional-dependencies"]
    assert {"starlette", "websockets", "msgpack"} <= set(extras)
    for name in ("starlette", "websockets", "msgpack"):
        assert any(name in requirement for requirement in extras[name]), extras[name]


# WSM-PKG-002 checked by execution. `import muxws` and a full request/reply exchange run in a child
# interpreter that cannot import anything outside the standard library, so a module-scope
# `import msgpack` (or starlette, or websockets) anywhere under `muxws/` fails here. The manifest
# tests above read only what the manifest declares and do not detect it.
_ISOLATED = textwrap.dedent(
    '''
    import sys

    ALLOWED = set(sys.stdlib_module_names) | {"muxws"}


    class Blocker:
        """Refuse every top-level name that is neither stdlib nor muxws."""

        def find_spec(self, name, path=None, target=None):
            top = name.split(".")[0]
            # Private names are the interpreter's own machinery - the editable-install finder among
            # them - and are already imported before this runs; blocking them would test the
            # installation rather than the package.
            if top not in ALLOWED and not top.startswith("_"):
                raise ModuleNotFoundError(f"blocked third-party import: {name}")
            return None


    sys.meta_path.insert(0, Blocker())

    import asyncio

    import muxws

    from muxws.transports.memory import memory_pair


    async def main():
        left, right = memory_pair()
        codec = muxws.JsonCodec()
        dialer = muxws.Peer(left, codec=codec, is_dialer=True)
        acceptor = muxws.Peer(right, codec=codec, is_dialer=False)

        async def handler(payload, stream):
            await stream.reply({"echo": payload})

        acceptor.on_stream(handler)
        tasks = [asyncio.create_task(dialer.serve()), asyncio.create_task(acceptor.serve())]
        result = await asyncio.wait_for(dialer.request({"q": 1}), 5)
        for task in tasks:
            task.cancel()
        if result != {"echo": {"q": 1}}:
            raise SystemExit(f"unexpected reply: {result!r}")


    asyncio.run(main())
    print("MUXWS-EXCHANGED")
    '''
)


def _run_isolated(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=60,
        check=False,
    )


def test_importing_muxws_and_serving_a_stream_needs_no_third_party_module():
    """WSM-PKG-002, checked by running code rather than by reading a manifest."""
    done = _run_isolated(_ISOLATED)
    assert done.returncode == 0, done.stderr
    assert "MUXWS-EXCHANGED" in done.stdout


def test_the_import_blocker_refuses_an_installed_third_party_package():
    """Control for the test above: the import blocker refuses a third-party package.

    `msgpack` is installed by the `dev` extra, so it is importable in this process; the child must
    refuse it, or the previous test does not run in isolation.
    """
    pytest.importorskip("msgpack", reason="the control needs a third-party module that is present")
    done = _run_isolated(_ISOLATED.replace("import muxws\n", "import msgpack\nimport muxws\n"))
    assert done.returncode != 0
    assert "blocked third-party import: msgpack" in done.stderr


# --------------------------------------------------------------------------- the reader itself


def test_the_toml_reader_agrees_with_tomllib(pyproject: dict[str, dict[str, Any]]):
    """`_tables` is hand-written because 3.10 has no `tomllib`; where `tomllib` exists it is compared."""
    tomllib = pytest.importorskip("tomllib", reason="Python 3.10 has no tomllib and no tomli here")
    parsed = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

    # `.get` throughout: this test checks the reader, not the manifest's content. A missing key fails
    # the test for the corresponding rule, not this one.
    for key in ("dependencies", "dynamic", "version", "requires-python"):
        assert pyproject["project"].get(key) == parsed["project"].get(key), key
    assert pyproject["tool.hatch.version"].get("path") == parsed["tool"]["hatch"]["version"].get("path")

    extras = parsed["project"].get("optional-dependencies", {})
    assert sorted(pyproject.get("project.optional-dependencies", {})) == sorted(extras)
    for name, requirements in extras.items():
        assert pyproject["project.optional-dependencies"][name] == requirements


# --------------------------------------------------------------------------- the demo is a consumer


def test_the_demo_adds_no_runtime_dependency(pyproject: dict[str, dict[str, Any]], npm_manifest: dict[str, Any]):
    """WSM-PKG-002 and WSM-PKG-003, checked against the demo's dependencies.

    The demo needs a web framework, a server and a UI toolkit that the library does not depend on.
    Moving `fastapi` into the required dependencies, or declaring `vue` as a peer dependency, would
    make `pip install muxws` and `npm install muxws` install the demo's dependencies.

    The npm half reads the manifest. `ts/packaging.spec.ts` checks that the bundle imports nothing
    optional by running a Vite build; this test checks the declarations, which install for every
    consumer regardless of what the bundle imports.
    """
    assert pyproject["project"]["dependencies"] == [], "required dependencies must stay empty"

    extras = pyproject["project.optional-dependencies"]
    assert "demo" in extras, "the demo's Python dependencies must be declared in the demo extra"
    for name in ("fastapi", "uvicorn"):
        assert any(name in requirement for requirement in extras["demo"]), extras["demo"]

    assert npm_manifest.get("dependencies", {}) == {}, "the published npm package installs nothing"
    # A non-optional peer dependency is installed for every consumer. The demo's frontend packages
    # (vue, vuetify, @vitejs/plugin-vue) are declared by the demo's own workspace manifest, so this
    # set is exactly the two WSM-PKG-003 names.
    assert set(npm_manifest.get("peerDependencies", {})) == {"ws", "@msgpack/msgpack"}
    for name in ("ws", "@msgpack/msgpack"):
        assert npm_manifest["peerDependenciesMeta"][name]["optional"] is True, name

    # `demo/frontend` is a workspace of this repository but not a file of this package: `files`
    # decides what npm puts in the tarball, and `dist/*` cannot reach it.
    assert npm_manifest["files"] == ["dist/*"]
    assert "demo/frontend" in npm_manifest["workspaces"], "demo.py runs the dev server through it"

    # Optional: this test checks the root manifest, so an absent frontend manifest is not a failure.
    # When present, every package it names is checked against the root manifest, where
    # `npm install --save` run in the root would add it.
    frontend: dict[str, Any] = {}
    if DEMO_FRONTEND_JSON.is_file():
        frontend = json.loads(DEMO_FRONTEND_JSON.read_text(encoding="utf-8"))
    demo_packages = set(frontend.get("dependencies", {})) | set(frontend.get("devDependencies", {}))
    published = set(npm_manifest.get("dependencies", {})) | set(npm_manifest.get("peerDependencies", {}))
    leaked = demo_packages & published
    assert leaked == set(), f"the demo's packages reached the published manifest: {sorted(leaked)}"


# --------------------------------------------------------------------------- built artefacts


def _demo_members(names: list[str], *, strip_leading_directory: bool) -> list[str]:
    """The members of an archive that came from the demo.

    An sdist puts everything under one `<name>-<version>/` directory and a wheel does not;
    `strip_leading_directory` selects the layout.
    """
    found = []
    for name in names:
        path = name.split("/", 1)[1] if strip_leading_directory and "/" in name else name
        if path == "demo.py" or path.startswith("demo/"):
            found.append(name)
    return found


@pytest.fixture(scope="module")
def built_artefacts(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A real sdist and a real wheel, built from this working tree into a temporary directory.

    `--outdir` is required: the default is `dist/`, which in this repository holds the Vite bundle
    that `npm test` checks.

    `python -m build` with neither flag builds the sdist and then builds the wheel from it, as a
    release does, so a file the sdist include-list admits appears in both.
    """
    pytest.importorskip("build", reason="`pip install -e .[dev]` provides it; without it nothing is built")
    outdir = tmp_path_factory.mktemp("artefacts")
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "build", "--outdir", str(outdir)],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=600,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    wheels = sorted(outdir.glob("*.whl"))
    sdists = sorted(outdir.glob("*.tar.gz"))
    assert len(wheels) == 1, wheels
    assert len(sdists) == 1, sdists
    return wheels[0], sdists[0]


def test_the_published_artefacts_contain_no_demo_files(built_artefacts: tuple[Path, Path]):
    """The demo ships with the repository and not with the package.

    Read from the artefacts rather than from `pyproject.toml`. `[tool.hatch.build.targets.wheel]
    packages = ["muxws"]` and the sdist's include-list both exclude the demo; the build shows whether
    hatch applies them.

    The first assertion checks that the archives are not empty. `demo.py` is a top-level module beside
    `muxws/`, and a wheel built without that `packages` line would include it.
    """
    wheel, sdist = built_artefacts
    assert DEMO_ENTRY_POINT.is_file(), "nothing to exclude: this test would pass against any manifest"

    with zipfile.ZipFile(wheel) as archive:
        wheel_names = archive.namelist()
    with tarfile.open(sdist) as archive:
        sdist_names = archive.getnames()

    # An empty archive would satisfy every "no demo" assertion below.
    assert "muxws/__init__.py" in wheel_names
    assert any(name.endswith("/muxws/__init__.py") for name in sdist_names), sdist_names[:5]

    assert _demo_members(wheel_names, strip_leading_directory=False) == []
    assert _demo_members(sdist_names, strip_leading_directory=True) == []

    # The wheel contains only `muxws/` and its own metadata. Any other top-level entry (the demo, a
    # script, a notebook) fails here without being named.
    tops = {name.split("/", 1)[0] for name in wheel_names}
    assert tops <= {"muxws", f"muxws-{_wheel_version(wheel)}.dist-info"}, tops


def _wheel_version(wheel: Path) -> str:
    """`muxws-1.0.0-py3-none-any.whl` -> `1.0.0`; the `.dist-info` directory is named from it."""
    return wheel.name.split("-")[1]


# --------------------------------------------------------------------------- dependency direction

#: Packages that sit **above** muxws: a web framework, an application server, an ORM, a task queue,
#: an HTTP client, a validation layer. Importing any of them would require every user of muxws to
#: install that stack. `starlette` is listed although an extra exists for it: the extra is for one
#: adapter module, and this asserts that no other module imports it.
_ABOVE_MUXWS_IN_THE_STACK = frozenset(
    {
        "aiohttp",
        "asgiref",
        "bottle",
        "celery",
        "channels",
        "daphne",
        "django",
        "fastapi",
        "flask",
        "graphene",
        "gunicorn",
        "httpx",
        "hypercorn",
        "kombu",
        "litestar",
        "pydantic",
        "pyramid",
        "quart",
        "redis",
        "requests",
        "sanic",
        "socketio",
        "sqlalchemy",
        "starlette",
        "starlite",
        "strawberry",
        "tornado",
        "uvicorn",
    }
)

#: The three third-party packages the extras exist for, and the library module each may be reached
#: from. `api.py` is on the `websockets` line because `connect()`'s dialer closure imports it inside
#: the function that dials; a transport is below muxws, and the import runs only when a caller dials
#: a real socket.
_OPTIONAL_DEPENDENCIES: dict[str, frozenset[str]] = {
    "starlette": frozenset({"muxws/transports/starlette.py"}),
    "websockets": frozenset({"muxws/transports/websockets_.py", "muxws/api.py"}),
    "msgpack": frozenset({"muxws/codecs/msgpack_.py"}),
}

#: The modules that bind muxws to one of those three. WSM-INV-001 does not apply to them: a
#: Starlette application reaches muxws through the Starlette adapter, so the dependency points from
#: the adapter to the framework the application already uses.
_ADAPTERS = frozenset({"muxws/transports/starlette.py", "muxws/transports/websockets_.py", "muxws/codecs/msgpack_.py"})


def _library_modules() -> list[Path]:
    """Every shipped `.py` under `muxws/`. Tests and their fixtures are not shipped."""
    return sorted(
        path
        for path in (ROOT / "muxws").rglob("*.py")
        if not path.name.endswith("_test.py") and path.name != "conftest.py"
    )


def _imported_top_level_names(path: Path) -> set[str]:
    """Every top-level package name `path` imports, from anywhere in the file.

    Uses `ast.walk`, not a scan of the module body: a lazily imported name inside a function and a
    name imported under `if TYPE_CHECKING:` are both invisible to
    `test_importing_muxws_and_serving_a_stream_needs_no_third_party_module`, which sees only what a
    run imports. WSM-INV-001 includes type-checking-only imports, so the source is read.
    """
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.add(node.module.split(".")[0])
    return found


def test_no_library_module_imports_anything_above_it_in_the_stack():
    """WSM-INV-001: muxws depends on nothing above it, including for type checking only."""
    offences = {
        path.relative_to(ROOT).as_posix(): sorted(_imported_top_level_names(path) & _ABOVE_MUXWS_IN_THE_STACK)
        for path in _library_modules()
        if path.relative_to(ROOT).as_posix() not in _ADAPTERS
    }
    assert {name: found for name, found in offences.items() if found} == {}


def test_every_third_party_import_is_an_extra_and_lives_only_in_its_own_module():
    """WSM-INV-001: every import is the standard library, muxws, or an extra in its own module.

    The denylist above catches only the packages it names. This test uses no list: every name a
    shipped module imports is the standard library, muxws itself, or one of the three packages an
    extra exists for, and each of those three is confined to its adapter module. Any other dependency
    fails here with its name in the message.
    """
    allowed = set(sys.stdlib_module_names) | {"muxws"}
    stray: dict[str, list[str]] = {}
    misplaced: dict[str, list[str]] = {}
    for path in _library_modules():
        name = path.relative_to(ROOT).as_posix()
        for imported in sorted(_imported_top_level_names(path) - allowed):
            if imported not in _OPTIONAL_DEPENDENCIES:
                stray.setdefault(name, []).append(imported)
            elif name not in _OPTIONAL_DEPENDENCIES[imported]:
                misplaced.setdefault(name, []).append(imported)
    assert stray == {}, "a third-party import that no extra covers"
    assert misplaced == {}, "an optional dependency reached from outside the module its extra is for"


def test_the_import_walker_finds_modules_and_nested_imports():
    """Control for both tests above: the walker finds modules and imports.

    Checks three failure modes, each of which would make the assertions above pass for any library:
    finding no modules, missing an import that exists, and missing an import in a shape the source
    uses (a lazy import inside a function body, and an import under `if TYPE_CHECKING:`).
    """
    modules = {path.relative_to(ROOT).as_posix() for path in _library_modules()}
    assert len(modules) > 15, modules
    assert _ADAPTERS <= modules, "an excluded adapter module does not exist"
    assert not any(name.endswith("_test.py") for name in modules)

    # Each adapter imports the package its extra is named for.
    for package, homes in _OPTIONAL_DEPENDENCIES.items():
        found = {home for home in homes if package in _imported_top_level_names(ROOT / home)}
        assert found, f"{package} is imported by none of {sorted(homes)}"

    # `api.py` imports `websockets` inside a nested function; `stream.py` imports `Peer` under
    # `TYPE_CHECKING`. Both are read from the source files.
    assert "websockets" in _imported_top_level_names(ROOT / "muxws" / "api.py")
    assert "muxws" in _imported_top_level_names(ROOT / "muxws" / "stream.py")


def test_the_published_wheel_ships_the_library_and_nothing_else(built_artefacts: tuple[Path, Path]):
    """The wheel contains the library modules and no tests.

    Read from the built artefact rather than from `pyproject.toml`: `[tool.hatch.build.targets.wheel]`
    needs its own `exclude`, because the sdist's list applies to the sdist only, and `python -m build`
    builds the wheel from the sdist, so a wheel built that way can look correct whether or not that
    list exists. Inspecting the built wheel detects a missing list.

    Tests are not part of the library, and a package-level `conftest.py` on the import path changes
    the behaviour of a consumer's pytest run.

    Asserted against the archive rather than an install, because `pip` adds `__pycache__` at install
    time.
    """
    wheel, _sdist = built_artefacts
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()

    # Non-vacuity first: an empty archive satisfies every exclusion below.
    assert "muxws/__init__.py" in names
    assert (ROOT / "muxws" / "peer_test.py").is_file(), "no test file exists to exclude"

    shipped_tests = [name for name in names if name.endswith("_test.py") or name.endswith("conftest.py")]
    assert shipped_tests == [], f"the wheel ships the test suite: {shipped_tests[:5]}"

    compiled = [name for name in names if "__pycache__" in name or name.endswith(".pyc")]
    assert compiled == [], f"the wheel ships bytecode: {compiled[:5]}"


def test_the_demo_extra_includes_a_websocket_implementation(pyproject: dict[str, Any]):
    """`pip install -e ".[demo,starlette]"` installs everything the demo needs, including a WebSocket library.

    uvicorn has no WebSocket protocol implementation of its own. Without `websockets` or `wsproto` it
    serves HTTP and answers 404 to every upgrade, logging "No supported WebSocket library detected".
    The demo page loads but the socket fails, and because a browser does not expose the HTTP status,
    the dialer reports a codec mismatch.

    Asserted against the manifest and not against the environment: the test environment has `[dev]`,
    which includes `websockets`, so a working socket here does not show what the `demo` extra
    installs.
    """
    demo = pyproject["project.optional-dependencies"]["demo"]

    assert any("uvicorn" in requirement for requirement in demo), "the demo extra does not install a server"
    implementations = ("websockets", "wsproto", "uvicorn[standard]")
    assert any(any(name in requirement for name in implementations) for requirement in demo), (
        f"the demo extra installs uvicorn with no WebSocket implementation: {demo}. uvicorn answers "
        f"404 to every upgrade without one, and the failure surfaces in the browser as a codec mismatch"
    )
