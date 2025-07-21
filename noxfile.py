import nox

# options
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["lint", "typecheck", "security", "test", "yamllint"]

PYTHON_VERSIONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]

# targets
TARGETS = ["src", "tests"]


def install_with_tools(session: nox.Session, *tools: str) -> None:
    """Install project and specified tools."""
    session.install(".")
    if tools:
        session.install(*tools)


def run_tool(session: nox.Session, tool: str, *args: str) -> None:
    """Run a tool with optional args or posargs override."""
    args = session.posargs or list(args) or TARGETS
    session.run(tool, *args)


@nox.session(tags=["lint"])
def lint(session: nox.Session) -> None:
    """Run ruff (lint + format) and isort."""
    install_with_tools(session, "ruff", "isort")
    run_tool(session, "ruff", "check", *TARGETS)
    run_tool(session, "isort", *TARGETS)


@nox.session(tags=["typecheck"])
def typecheck(session: nox.Session) -> None:
    """Run mypy on source code."""
    install_with_tools(session, "mypy")
    run_tool(session, "mypy", "--config-file=.github/linters/.mypy.ini", *TARGETS)


@nox.session(tags=["security"])
def security(session: nox.Session) -> None:
    """Run Bandit security checks."""
    install_with_tools(session, "bandit")
    session.run("bandit", "-r", *TARGETS, "--skip", "B101")


@nox.session(tags=["test"])
def test(session: nox.Session) -> None:
    """Run pytest test suite."""
    install_with_tools(session, "pytest", "pytest-cov")
    args = session.posargs or []
    session.run("pytest", *args)


@nox.session(tags=["lint"])
def yamllint(session: nox.Session) -> None:
    """Run YAML linter."""
    install_with_tools(session, "yamllint")
    args = session.posargs or ["src", "tests"]
    session.run("yamllint", *args)
