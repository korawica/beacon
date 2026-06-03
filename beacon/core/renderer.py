import logging
from collections.abc import Callable
from functools import partial
from typing import Any, Self

from jinja2 import (
    DebugUndefined,
    Environment,
    FileSystemLoader,
    Template,
    Undefined,
    UndefinedError,
)
from jinja2.exceptions import TemplateAssertionError
from jinja2.nativetypes import NativeEnvironment
from jinja2.sandbox import SandboxedEnvironment

from beacon.utils import is_jinja

logger = logging.getLogger("beacon.jinja2")


def _is_preserve_mode(env: Environment) -> bool:
    """Return True when the environment should preserve unresolvable templates.

    In preserve mode (DebugUndefined or any subclass such as KeepUndefined),
    unresolvable variable references and unregistered filter names are returned
    as their original template string so downstream consumers (e.g. Airflow)
    can evaluate them at task run-time.
    """
    return issubclass(env.undefined, DebugUndefined)


class JinjaRenderer:
    """Jinja Renderer object.

    This object use for common create :simple-jinja: Jinja environment and template
    any component outside _Airflow_ Jinja templating.

    !!! example

        ```py
        renderer = JinjaRenderer(user_defined_macros={"bar": "baz"})
        renderer.render_template({"foo": "{{ bar }}"})
        # Output: {'foo': 'baz'}
        ```
    """

    __slots__ = (
        "template_fields",
        "template_ext",
        "template_fields_excluded",
        "jinja_environment_kwargs",
        "is_native",
        "user_defined_filters",
        "user_defined_macros",
        "template_searchpath",
        "env",
        "env_factory",
    )

    def __init__(
        self,
        *,
        template_fields: tuple[str, ...] | None = None,
        template_ext: tuple[str, ...] | None = None,
        template_fields_excluded: tuple[str, ...] | None = None,
        jinja_environment_kwargs: dict[str, Any] | None = None,
        user_defined_filters: dict[str, Callable] | None = None,
        user_defined_macros: dict[str, Callable | Any] | None = None,
        template_searchpath: tuple[str, ...] | None = None,
        env_factory: type[Environment] | None = None,
        is_native: bool | None = True,
    ) -> None:
        """Main initialize construct method.

        Args:
            template_fields (tuple[str, ...] | None):
                A sequence of fields that want to pass a Jinja template.
            template_ext (tuple[str, ...] | None):
                A sequence of file extensions that want to load from the
                ``template_searchpath``.
            template_fields_excluded (tuple[str, ...] | None):
                A sequence of fields that will exclude from render Jinja
                template even exists in the ``template_fields``.
            template_searchpath (tuple[str, ...] | None):
                A sequence of path strings that Jinja environment will search
                for template files.
            jinja_environment_kwargs (dict[str, Any]):
                A Jinja environment kwargs mapping.
            user_defined_filters (dict[str, Callable]):
                An user defined Jinja template filters that will add to Jinja
                environment.
            user_defined_macros (dict[str, Callable | str]):
                An user defined Jinja template macros that will add to Jinja
                environment.
            is_native (bool | None, default ``True``):
                Whether to use NativeEnvironment or SandboxedEnvironment.
                - native: The Jinja environment that renders Python native types.
                - sandboxed: The Jinja environment that provides a sandboxed
                    execution environment to evaluate untrusted templates.
        """
        self.template_fields = template_fields or ()
        self.template_ext = template_ext or ()
        self.template_fields_excluded = template_fields_excluded or ()
        self.jinja_environment_kwargs = jinja_environment_kwargs or {}
        self.is_native = is_native
        self.user_defined_filters = user_defined_filters or {}
        self.user_defined_macros = user_defined_macros or {}
        self.template_searchpath = template_searchpath or ()
        self.env_factory = env_factory

        self.env: Environment = self.get_template_env()

    def copy_override(
        self,
        *,
        template_fields: tuple[str, ...] | None = None,
        template_ext: tuple[str, ...] | None = None,
        template_fields_excluded: tuple[str, ...] | None = None,
        jinja_environment_kwargs: dict[str, Any] | None = None,
        user_defined_filters: dict[str, Callable] | None = None,
        user_defined_macros: dict[str, Callable | Any] | None = None,
        template_searchpath: tuple[str, ...] | None = None,
        env_factory: type[Environment] | None = None,
        is_native: bool | None = None,
    ) -> Self:
        """Create a new JinjaRenderer object by copying and overriding parameters.

        All collection parameters (filters, macros, searchpath, kwargs) are
        **merged** with the current instance's values rather than replaced, so
        callers only need to supply the delta. ``is_native`` inherits from the
        current instance when omitted.

        Returns:
            Self: A new JinjaRenderer object with the overridden parameters.
        """
        return self.__class__(
            template_fields=template_fields or self.template_fields,
            template_ext=template_ext or self.template_ext,
            template_fields_excluded=(
                template_fields_excluded or self.template_fields_excluded
            ),
            jinja_environment_kwargs=(
                self.jinja_environment_kwargs | (jinja_environment_kwargs or {})
            ),
            user_defined_filters=(
                self.user_defined_filters | (user_defined_filters or {})
            ),
            user_defined_macros=(
                self.user_defined_macros | (user_defined_macros or {})
            ),
            template_searchpath=(
                self.template_searchpath + (template_searchpath or ())
            ),
            env_factory=env_factory or self.env_factory,
            is_native=self.is_native if is_native is None else is_native,
        )

    def set_globals(self, values: dict[str, Any]) -> Self:
        """Update the Jinja Environment globals value.

        Args:
            values (dict[str, Any]): A mapping value that want to update to the
                current globals Jinja environment.

        Returns:
            Self: The current JinjaRenderer object.
        """
        self.user_defined_macros.update(values)
        self.env.globals.update(values)
        return self

    def render_template(self, data: Any, env: Environment | None = None) -> Any:
        """Render template to the value that its key that exists in the
        ``template_fields`` class variable.

        !!! note

            Currently, Render template does not support render the template with
            if-else condition or loop statement. Because the render method will not
            pass any context value to the Jinja environment. So, the user should
            only use simple variable replacement in the template string.

        Args:
            data (Any): Any data that want to render Jinja template.
            env (Environment): A Jinja environment.

        Returns:
            Any: A data that already pass Jinja template from the current Jinja
                environment.
        """
        env = env or self.env
        if not isinstance(data, dict):
            return self.render(data, env=env, template_ext=self.template_ext)

        for key in data:
            if (
                key not in self.template_fields_excluded
                and key in self.template_fields
            ):
                data[key] = self.render(
                    data[key], env=env, template_ext=self.template_ext
                )
        return data

    def render(  # NOSONAR
        self,
        value: Any,
        *,
        template_ext: tuple[str, ...] | None = None,
        env: Environment | None = None,
        _seen: set[int] | None = None,
    ) -> Any:
        """Render Jinja template to any value with the current Jinja environment.

        Args:
            value (Any): An any value.
            template_ext (tuple[str, ...] | None):
                A sequence of file extensions that want to load from the
                ``template_searchpath``.
            env (Environment): A Jinja environment object.
            _seen (set[int] | None): A set of seen object IDs to prevent infinite
                recursion.

        Returns:
            Any: The value that was rendered if it is string type.
        """
        env = env or self.env

        if _seen is None:
            _seen = set()

        obj_id = id(value)

        if isinstance(value, (list, dict, set)) and obj_id in _seen:
            return value

        if isinstance(value, str):
            if template_ext and value.endswith(template_ext):
                template: Template = env.get_template(value)

            elif is_jinja(value, pure=False):
                try:
                    template: Template = env.from_string(value)
                except TemplateAssertionError:
                    # Unknown filter/test names raise at compile time. In
                    # preserve mode return the original value so Airflow can
                    # evaluate it at task run-time.
                    if _is_preserve_mode(env):
                        return value
                    raise
            else:
                return value

            try:
                logger.debug("Render Template: %s", value)
                render_result = template.render()
            except UndefinedError:
                if _is_preserve_mode(env):
                    return value
                raise

            if isinstance(render_result, Undefined) and _is_preserve_mode(env):
                return value

            return render_result

        if isinstance(value, (list, dict, set)):
            _seen.add(obj_id)

        try:
            render_partial: Callable = partial(
                self.render,
                env=env,
                template_ext=template_ext,
                _seen=_seen,
            )
            if value.__class__ is tuple:
                return tuple(render_partial(e) for e in value)
            elif isinstance(value, tuple):
                # ℹ️ NOTE: For other tuple-like objects such as NamedTuple.
                return value.__class__(*(render_partial(e) for e in value))
            elif isinstance(value, list):
                return [render_partial(e) for e in value]
            elif isinstance(value, dict):
                return {k: render_partial(v) for k, v in value.items()}
            elif isinstance(value, set):
                return {render_partial(e) for e in value}
        finally:
            _seen.discard(obj_id)

        return value

    def get_template_env(
        self,
        *,
        user_defined_filters: dict[str, Callable] | None = None,
        user_defined_macros: dict[str, Callable | str] | None = None,
        jinja_environment_kwargs: dict[str, Any] | None = None,
        template_searchpath: tuple[str, ...] | None = None,
        is_native: bool | None = None,
    ) -> Environment:
        """Return Jinja Template Native Environment object for rendering template
        on the Airflow DAG parameters before building.

        Args:
            user_defined_filters (dict[str, Callable]):
                An user defined Jinja template filters that will add to Jinja
                environment.
            user_defined_macros (dict[str, Callable | str]): An user defined
                Jinja template macros that will add to Jinja environment.
            jinja_environment_kwargs (dict[str, Any]): Additional configuration
                options to be passed to Jinja `Environment` for template
                rendering.
            template_searchpath (tuple[str, ...]):
                A sequence of path strings that Jinja environment will search
                for template files.
            is_native (bool, default ``None``):
                Whether to use NativeEnvironment or SandboxedEnvironment.
                - native: The Jinja environment that renders Python native types.
                - sandboxed: The Jinja environment that provides a sandboxed
                    execution environment to evaluate untrusted templates.

        Note:
            A sandboxed environment can be useful, for example, to allow users of
        an internal reporting system to create custom emails. You would document
        what data is available in the templates, then the user would write a
        template using that information.
        Your code would generate the report data and pass it to the user's
        sandboxed template to render.

        Returns:
            Environment: A Jinja Environment instance.
        """
        resolved_searchpath: tuple[str, ...] = self.template_searchpath + (
            template_searchpath or ()
        )
        resolved_is_native: bool = (
            is_native if is_native is not None else self.is_native
        )

        jinja_env_options: dict[str, Any] = {
            "undefined": Undefined,
            "extensions": ["jinja2.ext.do"],
            "cache_size": 0,
            **(
                {"loader": FileSystemLoader(resolved_searchpath)}
                if resolved_searchpath
                else {}
            ),
        }
        _env_cls: type[Environment] = self.env_factory or (
            NativeEnvironment if resolved_is_native else SandboxedEnvironment
        )
        env: Environment = _env_cls(
            **(
                jinja_env_options
                | self.jinja_environment_kwargs
                | (jinja_environment_kwargs or {})
            )
        )

        if user_defined_macros := (
            self.user_defined_macros | (user_defined_macros or {})
        ):
            env.globals.update(user_defined_macros)

        if user_defined_filters := (
            self.user_defined_filters | (user_defined_filters or {})
        ):
            env.filters.update(user_defined_filters)

        return env
