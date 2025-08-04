import shutil

import pytest


def pytest_unconfigure(config: pytest.Config) -> None:
    """Run after all tests complete to clear up test data and vars"""
    shutil.rmtree("tests/test_data")
