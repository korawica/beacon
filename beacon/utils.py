import importlib
import pkgutil
from types import ModuleType


def load_all_plugins(package: str | ModuleType):
    """Accept either a package string 'myapp.plugins' or a module object
    `import myapp.plugins`.

    Examples:
        Load plugins from a package string

        ```python
        load_all_plugins("myapp.plugins")
        ```

        Load plugins from a module object

        ```python
        import myapp.plugins
        load_all_plugins(myapp.plugins)
        ```
    """
    if isinstance(package, str):
        package = importlib.import_module(package)

    for finder, name, is_pkg in pkgutil.walk_packages(
        path=package.__path__,
        prefix=package.__name__ + ".",
    ):
        importlib.import_module(name)
