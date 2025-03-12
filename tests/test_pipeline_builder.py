import toml

from opg_pipeline_builder import __version__


def test_pyproject_toml_matches_version():
    with open("pyproject.toml") as f:
        proj = toml.load(f)
    assert __version__ == proj["tool"]["poetry"]["version"]
