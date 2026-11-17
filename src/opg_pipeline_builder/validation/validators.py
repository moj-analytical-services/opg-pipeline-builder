def is_valid_identifier(identifier: str) -> str:
    """Validate that the provided identifier is a valid SQL/Athena identifier.

    Checks:
    - Must not be empty
    - Must not contain any whitespace characters
    - Must contain only lowercase ASCII letters, digits, and underscores
    - Must not start with a number
    - Must be lowercase

    Returns an error message if invalid, or an empty string if valid.
    """
    if not identifier:
        return "Identifier cannot be empty"
    if any(char.isspace() for char in identifier):
        return "Identifier cannot contain whitespace characters"
    if not identifier.islower():
        return "Identifier must be lowercase"
    if identifier[0].isdigit():
        return "Identifier cannot start with a number"
    if not all(
        character.isascii() and (character.isalnum() or character == "_")
        for character in identifier
    ):
        return "Identifier contains invalid characters"
    return ""
