import nox

# options
nox.options.reuse_existing_virtualenvs = True
SILENT_DEFAULT = True
SILENT_CODE_MODIFIERS = False
RUNNER = "uv"

# targets
PACKAGE_LOCATION = "."


@nox.session(python="3.13", tags=["lint"])
def ruff(session: nox.Session) -> None:
    """Lint with ruff."""
    _install(session)
    _run(session, "ruff", "check", PACKAGE_LOCATION)


@nox.session(python="3.13", tags=["format"])
def isort(session: nox.Session) -> None:
    """Sort imports with isort."""
    _install(session)
    _run(session, "isort", PACKAGE_LOCATION)


@nox.session(python="3.13", tags=["test"])
def pytest(session: nox.Session) -> None:
    """Run the test suite with pytest."""
    args = session.posargs or ("--cov", "-m", "not e2e")
    _install(session)
    _run(session, "pytest", *args)


@nox.session(python="3.13", tags=["typecheck"])
def mypy(session: nox.Session) -> None:
    """Verify types using mypy (so it is static)."""
    _install(session)
    _run(session, "mypy", PACKAGE_LOCATION)


@nox.session(python="3.13", tags=["security"])
def bandit(session: nox.Session) -> None:
    _install(session)
    _run(session, "bandit", "-r", "src")


@nox.session(python="3.13", tags=["lint"])
def yamllint(session: nox.Session) -> None:
    _install(session, "yamllint")
    _run(session, "yamllint", "src/", ".pre-commit-config.yaml")


def _install(session: nox.Session, *args: str) -> None:
    session.run(
        RUNNER, "pip", "install", ".", *args, external=True, silent=SILENT_DEFAULT
    )


def _run(
    session: nox.Session,
    target: str,
    *args: str,
    silent: bool = SILENT_DEFAULT,
) -> None:
    session.run(RUNNER, "run", target, *args, external=True, silent=silent)
