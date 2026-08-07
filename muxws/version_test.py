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
