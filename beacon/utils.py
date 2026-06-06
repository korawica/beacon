import hashlib
import pickle
import re
from re import DOTALL, VERBOSE

#: Match any Jinja2 tag: ``{{ ... }}``, ``{% ... %}``, ``{# ... #}``.
_JINJA_PATTERN = re.compile(
    r"""
    (
        \{\{.*?}}           # {{ ... }}
        |\{%.*?%}           # {% ... %}
        |\{#.*?#}           # {# ... #}
    )
    """,
    DOTALL | VERBOSE,
)


def is_jinja(s: str) -> bool:
    """Return True if ``s`` contains any Jinja tag.

    >>> is_jinja("{{ x }}")
    True
    >>> is_jinja("prefix {{ x }} suffix")
    True
    >>> is_jinja("no template here")
    False
    """
    return _JINJA_PATTERN.search(s) is not None


def to_snake_case(value: str) -> str:
    """Convert a string to snake_case.

    >>> to_snake_case('camel2_camel2_case')
    'camel2_camel2_case'
    >>> to_snake_case('getHTTPResponseCode')
    'get_http_response_code'
    >>> to_snake_case('HTTPResponseCodeXYZ')
    'http_response_code_xyz'
    """
    value = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", value).lower()


def get_object_checksum(obj):
    """Calculate the checksum of a Python object.

    >>> get_object_checksum([{"a": 1, "b": 2}, (3, 4), "hello"])
    """
    # Serialize the object to a stable byte sequence
    # Note: protocol=0 ensures higher text-based determinism across platforms
    obj_bytes = pickle.dumps(obj, protocol=0)

    # Generate a SHA-256 hash string
    return hashlib.sha256(obj_bytes).hexdigest()
