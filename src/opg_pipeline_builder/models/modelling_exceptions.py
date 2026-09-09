# METADATA MODELS
class InvalidStageError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidColumnError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidTableError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidTypeError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class InvalidFormatError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error


class DuplicateColumnsError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error
