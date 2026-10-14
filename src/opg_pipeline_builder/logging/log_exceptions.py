"""Exceptions raised while validating or using structured logging."""


class InvalidPrefixError(Exception):
    """Raised when an invalid prefix is encountered."""

    def __init__(self, error: str) -> None:
        """Store the validation message on the exception.

        Args:
            error: Explanation of the invalid S3 prefix.
        """
        super().__init__(error)
        self.error = error


class InvalidProcessError(Exception):
    """Raised when an invalid process is encountered."""

    def __init__(self, error: str) -> None:
        """Store the validation message on the exception.

        Args:
            error: Explanation of the invalid process field.
        """
        super().__init__(error)
        self.error = error


class LoggingNotConfiguredError(RuntimeError):
    """Raised when logging is attempted before it is configured."""

    def __init__(self, error: str) -> None:
        """Store the state error message on the exception.

        Args:
            error: Explanation that logging has not been configured.
        """
        super().__init__(error)
        self.error = error
