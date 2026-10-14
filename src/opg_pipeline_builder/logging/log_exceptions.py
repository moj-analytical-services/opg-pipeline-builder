class InvalidPrefixError(Exception):
    """Raised when an invalid prefix is encountered."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with an error message."""
        super().__init__(error)
        self.error = error


class InvalidProcessError(Exception):
    """Raised when an invalid process is encountered."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with an error message."""
        super().__init__(error)
        self.error = error


class LoggingNotConfiguredError(RuntimeError):
    """Raised when logging is attempted before it is configured."""

    def __init__(self, error: str) -> None:
        """Initialize the exception with an error message."""
        super().__init__(error)
        self.error = error
