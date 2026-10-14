class InvalidPrefixError(Exception):
    def __init__(self, error: str):
        super().__init__(error)
        self.error = error
