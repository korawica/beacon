import re

from .const import JINJA_PATTERN


def is_jinja(s: str, pure: bool = True) -> bool:
    """Check string value is Jinja template or not.

    Args
        s (str): A string value.
        pure (bool, default True): A flag to check pure Jinja tag only.
            It will return True if the entire string is exactly one Jinja tag.

    Examples:
        >>> is_jinja("{{ vars('start_date') }}", pure=True)
        True
        >>> is_jinja("{{ vars('start_date') }}{{ vars('end') }}{{ data }}", pure=True)
        True
        >>> is_jinja("The start date is {{ vars('start_date') }}", pure=True)
        False
        >>> is_jinja("The start date is {{ vars('start_date') }}", pure=False)
        True
        >>> is_jinja("No jinja template here", pure=False)
        False

    Returns:
        bool: A flag indicate that the string is Jinja template or not.
    """
    matches = list(JINJA_PATTERN.finditer(s))

    if not matches:
        return False

    if not pure:
        return True

    combined = "".join(m.group(0) for m in matches)
    return combined == s


def to_snake_case(value: str) -> str:
    """Convert string value to snake case.

    Examples:
        >>> to_snake_case('camel2_camel2_case')
        'camel2_camel2_case'
        >>> to_snake_case('getHTTPResponseCode')
        'get_http_response_code'
        >>> to_snake_case('HTTPResponseCodeXYZ')
        'http_response_code_xyz'
    """
    value = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", value).lower()
