class InvalidPrefixError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidProcessError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class LoggingNotConfiguredError(RuntimeError):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error
