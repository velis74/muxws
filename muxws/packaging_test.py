"""Packaging invariants read from the **source manifests** (WSM-PKG-001, WSM-PKG-002).

`version_test.py` asserts the neighbouring facts through the *installed* distribution -
`muxws.__version__` against `package.json`, and `importlib.metadata.requires` against the extras.
That is a different witness, and a weaker one for these two rules: an editable install carries the
metadata of whatever `pip install -e .` last saw, so a `pyproject.toml` that grew a runtime
dependency this morning still reports the old, clean requirement set until somebody reinstalls. The
manifest on disk is what a wheel is built from, and it is what WSM-PKG-001/002 are written about.
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
    not exist and `tomli` is not a dev dependency (and `pyproject.toml` belongs to another change).
    A skip on the oldest supported interpreter would silently retire both rules exactly where a
    packaging mistake is most likely, so the reader is written out instead.

    Every value these tests read - a list of strings, a string - is also a Python literal, so
    `ast.literal_eval` is the whole parser. A value that is not (`authors`, `license`: TOML inline
    tables) is kept as raw text rather than raising, because refusing to parse a key nobody reads
    would make this reader fail on a manifest that is perfectly correct.
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

    `pyproject.toml` does not spell the number out - it declares the version `dynamic` and points
    hatch at `muxws/__init__.py` - so following that pointer *is* reading the manifest. The
    indirection is asserted rather than assumed: a manifest that stopped being dynamic, or that
    pointed hatch somewhere else, would otherwise leave this test comparing `package.json` against a
    file no wheel is built from, and passing.
    """
    project = pyproject["project"]
    assert "version" not in project, "a literal version here would be a second source of truth"
    assert project["dynamic"] == ["version"], "the version is hatch's to read"

    source = ROOT / str(pyproject["tool.hatch.version"]["path"])
    assert source == ROOT / "muxws" / "__init__.py", f"hatch reads the version from {source}"

    found = re.findall(r'^__version__ = "([^"]+)"', source.read_text(encoding="utf-8"), re.MULTILINE)
    assert len(found) == 1, f"exactly one __version__ assignment, found {len(found)}"
    python_version = found[0]

    npm_version = json.loads(PACKAGE_JSON.read_text(encoding="utf-8"))["version"]
    # Both halves being an empty string, or both being `None`, would satisfy equality and prove
    # nothing; a version stream that is not a version is not one manifest agreeing with another.
    assert re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?", python_version), python_version
    assert python_version == npm_version


def test_the_version_appears_in_exactly_one_python_source(pyproject: dict[str, dict[str, Any]]):
    """WSM-PKG-005: no second, finer version number anywhere - the generation is on the wire alone.

    A `__version__` copied into a second module is how the two manifests start disagreeing without
    either of them being edited.
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
    """WSM-PKG-002: `pip install muxws` must pull in nothing at all."""
    assert pyproject["project"]["dependencies"] == []


def test_every_third_party_import_sits_behind_an_extra(pyproject: dict[str, dict[str, Any]]):
    """WSM-PKG-002's second clause, which the empty list above cannot see.

    An empty `dependencies` is also what a manifest looks like when somebody deleted the extras and
    left the imports; naming the three the rule names is what makes the emptiness above mean
    "optional" rather than "undeclared".
    """
    extras = pyproject["project.optional-dependencies"]
    assert {"starlette", "websockets", "msgpack"} <= set(extras)
    for name in ("starlette", "websockets", "msgpack"):
        assert any(name in requirement for requirement in extras[name]), extras[name]


# The whole of WSM-PKG-002, executed rather than declared. `import muxws` and a full request/reply
# exchange run in a child interpreter that cannot import anything outside the standard library, so a
# module-scope `import msgpack` (or starlette, or websockets) added anywhere under `muxws/` fails
# here - which the manifest tests above, reading only what the manifest *claims*, never would.
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
    """WSM-PKG-002, proven by running rather than by reading a manifest."""
    done = _run_isolated(_ISOLATED)
    assert done.returncode == 0, done.stderr
    assert "MUXWS-EXCHANGED" in done.stdout


def test_the_third_party_blocker_actually_blocks():
    """The control for the test above. Without it, a blocker that allowed everything would agree.

    `msgpack` is installed by the `dev` extra, so it is importable in this process; the child must
    still refuse it, or the isolation the previous test rests on is imaginary.
    """
    pytest.importorskip("msgpack", reason="the control needs a third-party module that is present")
    done = _run_isolated(_ISOLATED.replace("import muxws\n", "import msgpack\nimport muxws\n"))
    assert done.returncode != 0
    assert "blocked third-party import: msgpack" in done.stderr


# --------------------------------------------------------------------------- the reader itself


def test_the_toml_reader_agrees_with_tomllib(pyproject: dict[str, dict[str, Any]]):
    """`_tables` is hand-written because 3.10 has no `tomllib`; everywhere else, it is checked."""
    tomllib = pytest.importorskip("tomllib", reason="Python 3.10 has no tomllib and no tomli here")
    parsed = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

    # `.get` throughout: this test is a statement about the reader, not about what the manifest
    # currently happens to declare. A key that moves must fail the rule's own test, not this one.
    for key in ("dependencies", "dynamic", "version", "requires-python"):
        assert pyproject["project"].get(key) == parsed["project"].get(key), key
    assert pyproject["tool.hatch.version"].get("path") == parsed["tool"]["hatch"]["version"].get("path")

    extras = parsed["project"].get("optional-dependencies", {})
    assert sorted(pyproject.get("project.optional-dependencies", {})) == sorted(extras)
    for name, requirements in extras.items():
        assert pyproject["project.optional-dependencies"][name] == requirements


# --------------------------------------------------------------------------- the demo is a consumer


def test_the_demo_adds_no_runtime_dependency(pyproject: dict[str, dict[str, Any]], npm_manifest: dict[str, Any]):
    """WSM-PKG-002 and WSM-PKG-003, restated against the one change most likely to break them.

    The demo is the first consumer of muxws in this repository that is not a test, and it needs a web
    framework, a server and a UI toolkit that the library itself has always refused to need. The
    failure this guards is not exotic: it is somebody moving `fastapi` up one table so `python
    demo.py` stops complaining, or declaring `vue` as a peer dependency so the workspace resolves -
    either of which makes `pip install muxws` and `npm install muxws` pull in the demo's world.

    The npm half is a manifest reading, deliberately: `ts/packaging.spec.ts` proves the *bundle*
    imports nothing optional by running a real Vite build, which is the stronger witness and the one
    that cannot be written from here. What this adds is the half a bundle cannot see - a declaration
    that would install for every consumer whether the bundle reaches for it or not.
    """
    assert pyproject["project"]["dependencies"] == [], "the demo is not a reason to grow this list"

    extras = pyproject["project.optional-dependencies"]
    assert "demo" in extras, "the demo's Python needs live behind an extra or they live nowhere"
    for name in ("fastapi", "uvicorn"):
        assert any(name in requirement for requirement in extras["demo"]), extras["demo"]

    assert npm_manifest.get("dependencies", {}) == {}, "the published npm package installs nothing"
    # A peer dependency is a required install for every consumer, spelled politely. The demo's
    # frontend packages (vue, vuetify, @vitejs/plugin-vue) must be declared by the demo's own
    # workspace manifest, so this set stays exactly the two WSM-PKG-003 names.
    assert set(npm_manifest.get("peerDependencies", {})) == {"ws", "@msgpack/msgpack"}
    for name in ("ws", "@msgpack/msgpack"):
        assert npm_manifest["peerDependenciesMeta"][name]["optional"] is True, name

    # `demo/frontend` is a workspace of this repository but not a file of this package: `files`
    # decides what npm puts in the tarball, and `dist/*` cannot reach it.
    assert npm_manifest["files"] == ["dist/*"]
    assert "demo/frontend" in npm_manifest["workspaces"], "demo.py runs the dev server through it"

    # Read while it exists rather than required to: this test's job is the root manifest, and it
    # must not start failing because the frontend has not landed yet. Once it has, every package it
    # names is checked against the root, which is where a stray `npm install --save` would put it.
    frontend: dict[str, Any] = {}
    if DEMO_FRONTEND_JSON.is_file():
        frontend = json.loads(DEMO_FRONTEND_JSON.read_text(encoding="utf-8"))
    demo_packages = set(frontend.get("dependencies", {})) | set(frontend.get("devDependencies", {}))
    published = set(npm_manifest.get("dependencies", {})) | set(npm_manifest.get("peerDependencies", {}))
    leaked = demo_packages & published
    assert leaked == set(), f"the demo's packages reached the published manifest: {sorted(leaked)}"


# --------------------------------------------------------------------------- what actually ships


def _demo_members(names: list[str], *, strip_leading_directory: bool) -> list[str]:
    """The members of an archive that came from the demo.

    An sdist puts everything under one `<name>-<version>/` directory and a wheel does not, so the
    same question is asked of two different shapes.
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

    `--outdir` is not a nicety: the default is `dist/`, which in this repository is the *Vite*
    bundle, and a test that overwrites the artefact `npm test` measures would be a fine way to lose
    an afternoon.

    `python -m build` with neither flag builds the sdist and then builds the wheel *from it*, which
    is the path a release actually takes - so a file that the sdist include-list lets through would
    be visible in both.
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

    Read from the artefacts rather than from `pyproject.toml`, because the manifest is the thing
    that would be wrong. `[tool.hatch.build.targets.wheel] packages = ["muxws"]` and the sdist's
    include-list both say the demo is out; a build says whether hatch agreed.

    The first assertion is what makes the rest mean anything. `demo.py` is a top-level module beside
    `muxws/`, and a wheel built without that `packages` line would sweep it in - so this test only
    proves an exclusion for as long as there is something on disk to exclude.
    """
    wheel, sdist = built_artefacts
    assert DEMO_ENTRY_POINT.is_file(), "nothing to exclude: this test would pass against any manifest"

    with zipfile.ZipFile(wheel) as archive:
        wheel_names = archive.namelist()
    with tarfile.open(sdist) as archive:
        sdist_names = archive.getnames()

    # A build that emitted an empty archive would satisfy every "no demo" assertion below.
    assert "muxws/__init__.py" in wheel_names
    assert any(name.endswith("/muxws/__init__.py") for name in sdist_names), sdist_names[:5]

    assert _demo_members(wheel_names, strip_leading_directory=False) == []
    assert _demo_members(sdist_names, strip_leading_directory=True) == []

    # Stronger than the two lines above and the reason they are cheap to keep: the wheel's whole
    # member list is `muxws/` plus its own metadata. Anything new at the top level - the demo, a
    # scratch script, a stray notebook - fails here rather than waiting for someone to name it.
    tops = {name.split("/", 1)[0] for name in wheel_names}
    assert tops <= {"muxws", f"muxws-{_wheel_version(wheel)}.dist-info"}, tops


def _wheel_version(wheel: Path) -> str:
    """`muxws-1.0.0-py3-none-any.whl` -> `1.0.0`; the `.dist-info` directory is named from it."""
    return wheel.name.split("-")[1]
