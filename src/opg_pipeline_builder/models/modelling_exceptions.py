# METADATA MODELS


class InvalidColumnNameError(Exception):
    """Exception raised for invalid column names."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidColumnNameError with the given error message."""
        super().__init__(error)
        self.error = error


class MissingTableNameError(Exception):
    """Exception raised for missing table names."""

    def __init__(self, error: str) -> None:
        """Initialize the MissingTableNameError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidPipelineNameError(Exception):
    """Exception raised for invalid pipeline names."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidPipelineNameError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidPathError(Exception):
    """Exception raised for invalid paths."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidPathError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidFormatError(Exception):
    """Exception raised for invalid formats."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidFormatError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidSemanticTypeError(Exception):
    """Exception raised for invalid semantic types."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidSemanticTypeError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidStageError(Exception):
    """Exception raised for invalid stages."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidStageError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidTypeError(Exception):
    """Exception raised for invalid types."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidTypeError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidColumnError(Exception):
    """Exception raised for invalid columns."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidColumnError with the given error message."""
        super().__init__(error)
        self.error = error


class InvalidTableError(Exception):
    """Exception raised for invalid tables."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidTableError with the given error message."""
        super().__init__(error)
        self.error = error


class DuplicateFieldsError(Exception):
    """Exception raised for duplicate fields."""

    def __init__(self, error: str) -> None:
        """Initialize the DuplicateFieldsError with the given error message."""
        super().__init__(error)
        self.error = error


class DuplicateFileFormatStagesError(Exception):
    """Exception raised for duplicate file format stages."""

    def __init__(self, error: str) -> None:
        """Initialize the DuplicateFileFormatStagesError with the given error message."""
        super().__init__(error)
        self.error = error


# SETTINGS EXCEPTIONS


class InvalidEnvironmentError(Exception):
    """Exception raised for invalid environments."""

    def __init__(self, error: str) -> None:
        """Initialize the InvalidEnvironmentError with the given error message."""
        super().__init__(error)
        self.error = error
