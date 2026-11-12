class InvalidPrefixError(Exception):
    """Exception raised for invalid S3 prefixes."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidPrefixError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidProcessError(Exception):
    """Exception raised for invalid processes."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidProcessError with the given error message."""
        super().__init__(error)
        self.error = error


class LoggingNotConfiguredError(RuntimeError):
    """Exception raised when logging is not configured."""

    def __init__(self, error: str) -> None:
        """Initialize the LoggingNotConfiguredError with the given error message."""
        super().__init__(error)
        self.error = error
