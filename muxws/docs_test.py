"""The Python half of the documentation checker (M7, brief §7 tests 3 and 4).

`docs/check-docs.mjs` enumerates the TypeScript surface, which node can import. This module is the
mirror for Python, and it does the one thing node cannot: it resolves each documented signature to
the real callable and compares them parameter by parameter, so a renamed argument or a changed
default fails here rather than in a reader's editor.

Two tests, deliberately separate:

* `test_every_public_python_symbol_is_documented` - the surface has an entry at all.
* `test_documented_signatures_match_the_source` - the entry still describes the thing it names. A
  documented symbol that no longer resolves fails this test, which is the reverse direction of
  coverage: the site cannot keep an entry for something that was deleted.

Optional extras (starlette, websockets, msgpack) are imported lazily; a block that needs one that is
not installed is skipped individually, and the test asserts a floor on how many comparisons actually
ran so that a stripped environment cannot quietly turn this file into a no-op.
"""

import ast
import dataclasses
import enum
import importlib
import inspect
import re

from pathlib import Path
from typing import Any

import pytest

import muxws

from muxws import Peer, PeerRegistry, Stream

DOCS_API = Path(__file__).resolve().parent.parent / "docs" / "api"

#: Where a documented name may live when it is not re-exported from the package root. `memory_pair`
#: is `muxws.transports.memory`, `exception_for_reset` is `muxws.errors`, and the site says so at
#: each site; this list is how the test follows them.
SEARCH_MODULES = [
    "muxws",
    "muxws.api",
    "muxws.errors",
    "muxws.peer",
    "muxws.stream",
    "muxws.registry",
    "muxws.reconnect",
    "muxws.frames",
    "muxws.fragment",
    "muxws.conf",
    "muxws.observability",
    "muxws.subprotocol",
    "muxws.codecs",
    "muxws.codecs.json_",
    "muxws.codecs.msgpack_",
    "muxws.transports",
    "muxws.transports.memory",
    "muxws.transports.starlette",
    "muxws.transports.unix",
    "muxws.transports.websockets_",
]

#: Modules behind an optional extra. A signature block that needs one of these and cannot have it is
#: skipped rather than failed - but see `MINIMUM_SIGNATURES_COMPARED`.
OPTIONAL_MODULES = {
    "muxws.codecs.msgpack_": "msgpack",
    "muxws.transports.starlette": "starlette",
    "muxws.transports.websockets_": "websockets",
}

#: A floor under `test_documented_signatures_match_the_source`, so that a resolution bug or a
#: stripped environment shows up as a failure rather than as a green run that compared nothing.
#: 106 comparisons run with every extra installed; the three optional-extra pages account for about
#: sixteen of them, so this floor still bites in the leanest environment the library supports.
MINIMUM_SIGNATURES_COMPARED = 85

#: Dunders the site documents under a name of their own. `len(registry)` is not a symbol.
DUNDER_SPELLINGS = {"__len__": "len(registry)"}


# ------------------------------------------------------------------------------------------------
# Reading the pages
# ------------------------------------------------------------------------------------------------


def _pages() -> list[Path]:
    pages = sorted(p for p in DOCS_API.glob("*.md") if p.name != "index.md")
    if not pages:
        raise AssertionError(f"no API reference pages under {DOCS_API}")
    return pages


def _without_fences(text: str) -> str:
    """Blank fenced blocks so a `##` inside an example is not read as a heading.

    Every blanked line keeps its length, so an offset into the masked text is the same offset into
    the original. `_entries` slices the raw page with offsets found here.
    """
    out = []
    fence: str | None = None
    for line in text.split("\n"):
        opener = re.match(r"\s*(`{3,}|~{3,})", line)
        if fence is None and opener:
            fence = opener.group(1)[0] * 3
            out.append(" " * len(line))
        elif fence is not None:
            if re.match(rf"\s*{fence}", line):
                fence = None
            out.append(" " * len(line))
        else:
            out.append(line)
    return "\n".join(out)


@dataclasses.dataclass(frozen=True)
class Entry:
    """One `##` symbol entry on an API page."""

    page: Path
    heading: str
    line: int
    body: str

    @property
    def language(self) -> str:
        if "(Python" in self.heading:
            return "python"
        if "(TypeScript" in self.heading:
            return "typescript"
        return "unspecified"

    @property
    def spans(self) -> list[str]:
        return re.findall(r"`([^`]+)`", self.heading)

    def signature_blocks(self) -> list[str]:
        section = re.search(r"^### Signature\s*\n(.*?)(?=^### |\Z)", self.body, re.M | re.S)
        if section is None:
            return []
        return re.findall(r"^```python\n(.*?)^```", section.group(1), re.M | re.S)


def _entries() -> list[Entry]:
    entries: list[Entry] = []
    for page in _pages():
        raw = page.read_text(encoding="utf-8")
        masked = _without_fences(raw)
        starts = [(m.start(), m.group(1)) for m in re.finditer(r"^## (.+)$", masked, re.M)]
        for index, (offset, heading) in enumerate(starts):
            end = starts[index + 1][0] if index + 1 < len(starts) else len(raw)
            entries.append(
                Entry(
                    page=page,
                    heading=heading.strip(),
                    line=masked.count("\n", 0, offset) + 1,
                    body=raw[offset:end],
                )
            )
    return entries


def _documented_names() -> set[str]:
    """Every name a `##` entry heading claims, plus the receiver of every member entry."""
    names: set[str] = set()
    for entry in _entries():
        for span in entry.spans:
            span = re.sub(r"^new\s+", "", span.strip())
            if "/" in span:
                continue
            bracket = re.fullmatch(r"([A-Za-z_$][\w$]*)\[(Symbol\.[\w$]+)\]\(\)", span)
            if bracket:
                names.add(bracket.group(1))
                names.add(bracket.group(2))
                continue
            span = re.sub(r"\(\)$", "", span)
            if re.search(r"[()\[\]<>]", span):
                continue
            names.add(span)
            if "." in span:
                head, _, tail = span.rpartition(".")
                names.add(head)
                names.add(tail)
    return names


def _all_api_text() -> str:
    return "\n".join(page.read_text(encoding="utf-8") for page in _pages())


# ------------------------------------------------------------------------------------------------
# Test 3 - coverage
# ------------------------------------------------------------------------------------------------


def _is_documented(name: str, documented: set[str]) -> bool:
    if name in documented:
        return True
    # A class documented member by member is named by its receiver: `stream.send()` names `Stream`.
    return name.lower() in {candidate.lower() for candidate in documented}


def test_every_public_python_symbol_is_documented() -> None:
    documented = _documented_names()
    api_text = _all_api_text()
    missing: list[str] = []

    for name in muxws.__all__:
        if not _is_documented(name, documented):
            missing.append(f"muxws.__all__ exports `{name}`, which has no entry in docs/api/")

    for cls in (Peer, Stream, PeerRegistry):
        for member, _ in inspect.getmembers(cls):
            if member.startswith("_"):
                continue
            if not _is_documented(member, documented):
                missing.append(f"`{cls.__name__}.{member}` is public but has no entry in docs/api/")
        # The dunders these classes define are part of how they are used - `await stream`,
        # `async for`, `len(registry)` - so the site must name each of them somewhere.
        for member in vars(cls):
            if not (member.startswith("__") and member.endswith("__")):
                continue
            # `__firstlineno__` and `__static_attributes__` are written by the 3.13+ compiler.
            if member in {"__init__", "__module__", "__qualname__", "__doc__", "__dict__", "__weakref__"}:
                continue
            if member in {"__firstlineno__", "__static_attributes__"}:
                continue
            if member in {"__slots__", "__annotations__", "__repr__"}:
                continue
            spelling = DUNDER_SPELLINGS.get(member, member)
            if spelling not in api_text:
                missing.append(f"`{cls.__name__}.{member}` is defined but is never mentioned in docs/api/")

    error_classes = [
        getattr(muxws, name)
        for name in muxws.__all__
        if inspect.isclass(getattr(muxws, name)) and issubclass(getattr(muxws, name), BaseException)
    ]
    inherited = set(dir(Exception)) | set(dir(object))
    errors_page = (DOCS_API / "errors.md").read_text(encoding="utf-8")
    for cls in error_classes:
        for member in dir(cls):
            if member.startswith("_") or member in inherited:
                continue
            if member not in errors_page:
                missing.append(f"`{cls.__name__}.{member}` is public but is never mentioned in docs/api/errors.md")

    # `ResetCode`'s members are the reader's decision table; each of the nine must be on the page.
    for member in muxws.ResetCode:
        if member.name not in errors_page:
            missing.append(f"`ResetCode.{member.name}` is never mentioned in docs/api/errors.md")

    assert missing == [], "undocumented public Python surface:\n  " + "\n  ".join(sorted(set(missing)))


# ------------------------------------------------------------------------------------------------
# Test 4 - documented signatures against inspect.signature
# ------------------------------------------------------------------------------------------------


class _UnavailableError(Exception):
    """The real symbol lives behind an optional extra that is not installed."""


def _module(name: str) -> Any:
    try:
        return importlib.import_module(name)
    except ImportError as exc:  # pragma: no cover - depends on which extras are installed
        if name in OPTIONAL_MODULES:
            raise _UnavailableError(f"{name} needs the {OPTIONAL_MODULES[name]} extra") from exc
        raise


def _resolve_name(name: str) -> Any:
    """The object a bare documented name refers to, searched in package-root-first order."""
    unavailable: str | None = None
    for module_name in SEARCH_MODULES:
        try:
            module = _module(module_name)
        except _UnavailableError as exc:
            unavailable = str(exc)
            continue
        if hasattr(module, name):
            return getattr(module, name)
    if unavailable is not None:
        raise _UnavailableError(unavailable)
    raise LookupError(name)


def _receiver_classes() -> dict[str, type]:
    """`peer` -> `Peer`, `stream` -> `Stream`, and every class by its own name."""
    classes: dict[str, type] = {}
    for module_name in SEARCH_MODULES:
        try:
            module = _module(module_name)
        except _UnavailableError:
            continue
        for attribute, value in vars(module).items():
            if inspect.isclass(value) and not attribute.startswith("_"):
                classes.setdefault(attribute, value)
                classes.setdefault(attribute[0].lower() + attribute[1:], value)
    return classes


def _unwrap(target: Any) -> Any:
    """A property is documented by its getter; a decorated function by the function it wraps."""
    if isinstance(target, property):
        return target.fget
    if isinstance(target, (staticmethod, classmethod)):
        return target.__func__
    return target


def _resolve_function(entry: Entry, node: ast.AST, owner_class: str | None, receivers: dict[str, type]) -> Any:
    """The real callable that a documented `def` in this entry describes."""
    name = node.name
    if owner_class is not None:
        owner = receivers.get(owner_class)
        if owner is None:
            raise LookupError(owner_class)
        if not hasattr(owner, name):
            raise LookupError(f"{owner_class}.{name}")
        return _unwrap(inspect.getattr_static(owner, name))

    for span in entry.spans:
        span = re.sub(r"^new\s+", "", span.strip())
        span = re.sub(r"\(\)$", "", span)
        if "/" in span or re.search(r"[()\[\]<>]", span):
            continue
        receiver = span.rpartition(".")[0] if "." in span else span
        owner = receivers.get(receiver)
        if owner is not None and hasattr(owner, name):
            return _unwrap(inspect.getattr_static(owner, name))
    return _resolve_name(name)


def _documented_parameters(node: ast.AST) -> list[tuple[str, str, str | None, str | None]]:
    """`(name, kind, default source, annotation source)` for a documented `def`, in order."""
    args = node.args
    out: list[tuple[str, str, str | None, str | None]] = []
    positional = list(args.posonlyargs) + list(args.args)
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    for index, arg in enumerate(args.posonlyargs):
        default = defaults[index]
        out.append((arg.arg, "positional-only", None if default is None else ast.unparse(default), _annotation(arg)))
    for index, arg in enumerate(args.args, start=len(args.posonlyargs)):
        default = defaults[index]
        out.append((arg.arg, "positional", None if default is None else ast.unparse(default), _annotation(arg)))
    if args.vararg is not None:
        out.append((args.vararg.arg, "var-positional", None, _annotation(args.vararg)))
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        out.append((arg.arg, "keyword-only", None if default is None else ast.unparse(default), _annotation(arg)))
    if args.kwarg is not None:
        out.append((args.kwarg.arg, "var-keyword", None, _annotation(args.kwarg)))
    return out


def _annotation(arg: ast.arg) -> str | None:
    return None if arg.annotation is None else ast.unparse(arg.annotation)


_KINDS = {
    inspect.Parameter.POSITIONAL_ONLY: "positional-only",
    inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional",
    inspect.Parameter.VAR_POSITIONAL: "var-positional",
    inspect.Parameter.KEYWORD_ONLY: "keyword-only",
    inspect.Parameter.VAR_KEYWORD: "var-keyword",
}


def _defaults_agree(documented: str | None, actual: Any, namespace: dict[str, Any]) -> bool:
    if documented is None:
        return actual is inspect.Parameter.empty
    if actual is inspect.Parameter.empty:
        return False
    try:
        evaluated = eval(documented, dict(namespace))  # noqa: S307 - the input is this repository's own docs
    except Exception:
        # Not evaluable here (a private helper in another module, say); fall back on the text.
        return documented.replace("_", "") == repr(actual).replace("_", "")
    if evaluated is actual:
        return True
    try:
        return bool(evaluated == actual) and type(evaluated) is type(actual)
    except Exception:  # pragma: no cover - a default with an exploding __eq__
        return False


def _compare_function(entry: Entry, node: ast.AST, target: Any, problems: list[str]) -> None:
    where = f"{entry.page.name}:{entry.line} {entry.heading} -> {getattr(target, '__qualname__', target)}"
    try:
        actual = inspect.signature(target)
    except (TypeError, ValueError) as exc:  # pragma: no cover - a builtin would land here
        problems.append(f"{where}: cannot introspect ({exc})")
        return

    documented = _documented_parameters(node)
    real = [(p.name, _KINDS[p.kind], p.default) for p in actual.parameters.values()]

    if [d[0] for d in documented] != [r[0] for r in real]:
        problems.append(
            f"{where}: documented parameters {[d[0] for d in documented]} but the source has {[r[0] for r in real]}"
        )
        return
    for (name, kind, default, _), (_, real_kind, real_default) in zip(documented, real, strict=True):
        if kind != real_kind:
            problems.append(f"{where}: `{name}` is documented {kind} but is {real_kind} in the source")
        namespace = dict(vars(muxws)) | dict(getattr(target, "__globals__", {}))
        if not _defaults_agree(default, real_default, namespace):
            shown = "no default" if real_default is inspect.Parameter.empty else repr(real_default)
            problems.append(f"{where}: `{name}` is documented `= {default}` but the source has {shown}")

    # Annotations are compared against the source text rather than against `inspect.signature`,
    # because half these modules use `from __future__ import annotations` and half do not, so the
    # objects `signature()` reports are strings in one module and types in the next.
    source_node = _source_def(target)
    if source_node is None:
        return
    source_parameters = {name: annotation for name, _, _, annotation in _documented_parameters(source_node)}
    for name, _, _, annotation in documented:
        expected = source_parameters.get(name)
        if _normalise(annotation) != _normalise(expected):
            problems.append(f"{where}: `{name}` is documented `{annotation}` but the source says `{expected}`")
    documented_return = None if node.returns is None else ast.unparse(node.returns)
    source_return = None if source_node.returns is None else ast.unparse(source_node.returns)
    if _normalise(documented_return) != _normalise(source_return):
        problems.append(f"{where}: documented to return `{documented_return}` but the source returns `{source_return}`")


def _normalise(annotation: str | None) -> str | None:
    return None if annotation is None else re.sub(r"\s+", "", annotation).strip("'\"")


def _source_def(target: Any) -> ast.AST | None:
    """The `def` node of the real function, for an exact annotation comparison."""
    function = inspect.unwrap(target)
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError):  # pragma: no cover - a C function would land here
        return None
    tree = ast.parse(_dedent(source))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function.__name__:
            return node
    return None


def _dedent(source: str) -> str:
    lines = source.split("\n")
    indent = min((len(line) - len(line.lstrip()) for line in lines if line.strip()), default=0)
    return "\n".join(line[indent:] if len(line) >= indent else line for line in lines)


def _compare_dataclass(entry: Entry, node: ast.ClassDef, target: Any, problems: list[str]) -> None:
    where = f"{entry.page.name}:{entry.line} {entry.heading} -> {target.__name__}"
    documented = [
        (item.target.id, None if item.value is None else ast.unparse(item.value))
        for item in node.body
        if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
    ]
    real = {field.name: field for field in dataclasses.fields(target)}
    if [name for name, _ in documented] != list(real):
        problems.append(f"{where}: documented fields {[n for n, _ in documented]} but the source has {list(real)}")
        return
    for name, default in documented:
        field = real[name]
        if field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING:
            if default is not None and not default.startswith("field("):
                problems.append(f"{where}: `{name}` is documented `= {default}` but the source has no default")
            continue
        if default is None or default.startswith("field("):
            continue
        if not _defaults_agree(default, field.default, dict(vars(muxws))):
            problems.append(f"{where}: `{name}` is documented `= {default}` but the source has {field.default!r}")


def _compare_enum(entry: Entry, node: ast.ClassDef, target: Any, problems: list[str]) -> None:
    where = f"{entry.page.name}:{entry.line} {entry.heading} -> {target.__name__}"
    documented = {
        item.targets[0].id: ast.literal_eval(item.value)
        for item in node.body
        if isinstance(item, ast.Assign) and isinstance(item.targets[0], ast.Name)
    }
    real = {member.name: member.value for member in target}
    if documented != real:
        problems.append(f"{where}: documented members {documented} but the source has {real}")


def _compare_constant(entry: Entry, node: ast.AnnAssign, target: Any, problems: list[str]) -> None:
    where = f"{entry.page.name}:{entry.line} {entry.heading} -> {node.target.id}"
    if node.value is None:
        return
    if not _defaults_agree(ast.unparse(node.value), target, dict(vars(muxws))):
        problems.append(f"{where}: documented as `{ast.unparse(node.value)}` but the source holds {target!r}")


def _parse_signature(block: str) -> ast.Module:
    """A signature block is a declaration, so it often ends at the colon with no body. Give it one."""
    try:
        return ast.parse(block)
    except SyntaxError:
        return ast.parse(block.rstrip() + "\n    ...\n")


def test_documented_signatures_match_the_source() -> None:
    receivers = _receiver_classes()
    problems: list[str] = []
    skipped: list[str] = []
    compared = 0

    for entry in _entries():
        if entry.language == "typescript":
            continue  # docs/check-docs.mjs owns the TypeScript side
        for block in entry.signature_blocks():
            try:
                tree = _parse_signature(block)
            except SyntaxError as exc:
                problems.append(
                    f"{entry.page.name}:{entry.line} {entry.heading}: signature block is not Python ({exc})"
                )
                continue
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    compared += _visit_function(entry, node, None, receivers, problems, skipped)
                elif isinstance(node, ast.ClassDef):
                    compared += _visit_class(entry, node, receivers, problems, skipped)
                elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                    compared += _visit_constant(entry, node, problems, skipped)

    assert problems == [], "documented Python signatures that disagree with the source:\n  " + "\n  ".join(problems)
    assert compared >= MINIMUM_SIGNATURES_COMPARED, (
        f"only {compared} documented signatures were compared, below the floor of "
        f"{MINIMUM_SIGNATURES_COMPARED}; skipped for missing extras: {skipped}"
    )


def _visit_function(
    entry: Entry,
    node: ast.AST,
    owner_class: str | None,
    receivers: dict[str, type],
    problems: list[str],
    skipped: list[str],
) -> int:
    try:
        target = _resolve_function(entry, node, owner_class, receivers)
    except _UnavailableError as exc:
        skipped.append(f"{entry.heading}: {exc}")
        return 0
    except LookupError:
        owner = f"{owner_class}." if owner_class else ""
        problems.append(
            f"{entry.page.name}:{entry.line} {entry.heading} documents `{owner}{node.name}`, "
            "which no longer exists in the source"
        )
        return 0
    _compare_function(entry, node, target, problems)
    return 1


def _visit_class(
    entry: Entry, node: ast.ClassDef, receivers: dict[str, type], problems: list[str], skipped: list[str]
) -> int:
    try:
        target = _resolve_name(node.name)
    except _UnavailableError as exc:
        skipped.append(f"{entry.heading}: {exc}")
        return 0
    except LookupError:
        problems.append(
            f"{entry.page.name}:{entry.line} {entry.heading} documents class `{node.name}`, "
            "which no longer exists in the source"
        )
        return 0

    compared = 0
    if dataclasses.is_dataclass(target):
        _compare_dataclass(entry, node, target, problems)
        compared += 1
    elif isinstance(target, type) and issubclass(target, enum.Enum):
        _compare_enum(entry, node, target, problems)
        compared += 1
    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            compared += _visit_function(entry, item, node.name, receivers, problems, skipped)
    return compared


def _visit_constant(entry: Entry, node: ast.AnnAssign, problems: list[str], skipped: list[str]) -> int:
    annotation = ast.unparse(node.annotation)
    if not annotation.startswith("Final"):
        return 0
    try:
        target = _resolve_name(node.target.id)
    except _UnavailableError as exc:
        skipped.append(f"{entry.heading}: {exc}")
        return 0
    except LookupError:
        problems.append(
            f"{entry.page.name}:{entry.line} {entry.heading} documents `{node.target.id}`, "
            "which no longer exists in the source"
        )
        return 0
    _compare_constant(entry, node, target, problems)
    return 1


@pytest.mark.parametrize("page", _pages(), ids=lambda p: p.name)
def test_every_api_page_has_entries(page: Path) -> None:
    """A page that lost its content would otherwise make both tests above quietly easier."""
    entries = [e for e in _entries() if e.page == page and e.heading != "See also"]
    assert entries, f"{page.name} has no `##` symbol entries"
