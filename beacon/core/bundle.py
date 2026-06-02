from pathlib import Path
from typing import Any

BUNDLE_MEMORY_DAGS: dict = {}


class BaseBundle:
    """Base Bundle.

    This bundle will be the default bundle for loaded data on memory.
    """

    def __init__(self): ...


class LocalBundle:
    """Local Bundle."""

    def __init__(
        self,
        name: str,
        path: str | Path,
    ) -> None:
        self.name = name
        self.path = path


class GcsBundle:
    """Gcs Bundle."""

    def __init__(
        self,
        name: str,
        ref: str,
        connection: Any,
    ) -> None:
        self.name = name
        self.ref = ref
        self.connection = connection
