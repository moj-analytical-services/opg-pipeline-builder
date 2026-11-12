"""Validation exceptions for the OPG Pipeline Builder."""


class InvalidIdentifierError(Exception):
    """Exception raised for invalid identifiers."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidPathTemplateError(Exception):
    """Exception raised for invalid path templates."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidPathError(Exception):
    """Exception raised for invalid paths."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidDatetimeError(Exception):
    """Exception raised for invalid datetimes."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with the given error message."""
        super().__init__(error)
        self.error = error
