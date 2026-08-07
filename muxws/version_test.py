import json

from importlib.metadata import requires
from pathlib import Path

import muxws


def test_python_and_npm_versions_match():
    """WSM-PKG-001: one version stream, two package manifests."""
    package_json = json.loads((Path(__file__).parent.parent / "package.json").read_text())
    assert package_json["version"] == muxws.__version__


def test_package_has_no_required_runtime_dependencies():
    """WSM-PKG-002: every declared dependency must sit behind an extra."""
    for requirement in requires("muxws") or []:
        assert "extra ==" in requirement, requirement


def test_every_name_in_all_is_actually_importable():
    """`__all__` is a promise. A name listed but never imported is an AttributeError waiting.

    Regression: `Peer`, `Stream`, `CloseReason` and `SocketAdapter` were listed for a whole milestone
    without being imported, because an edit's anchor had been reformatted out from under it. Nothing
    noticed, because no test had reason to reach for them through the package root.
    """
    missing = [name for name in muxws.__all__ if not hasattr(muxws, name)]
    assert missing == [], f"listed in __all__ but not importable: {missing}"


def test_all_is_sorted_and_free_of_duplicates():
    assert muxws.__all__ == sorted(muxws.__all__)
    assert len(muxws.__all__) == len(set(muxws.__all__))


def test_star_import_works():
    """The cheapest end-to-end check that the package root is coherent."""
    namespace: dict[str, object] = {}
    exec("from muxws import *", namespace)  # noqa: S102
    assert "Peer" in namespace
    assert "connect" in namespace
