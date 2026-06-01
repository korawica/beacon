import logging
from typing import Any, ClassVar

from pydantic import BaseModel, ValidationInfo
from pydantic.functional_validators import model_validator
from pydantic_core import PydanticUndefined

from .renderer import JinjaRender


logger = logging.getLogger("beacon.core")


class Templater(BaseModel):
    """Templater Model.

    This model using for any Builder model that want to use Jinja templating
    support for any of its fields before the model validation.

    !!! tip "Jinja Templating Support"

        For :simple-jinja: Jinja templating support, you can override the ``template_fields``
        and ``template_fields_ext`` class variables to add any extra template fields
        that you want to render before the model validation.


    !!! example

        ```python
        from typing import ClassVar
        from beacon.core import Templater

        class MyPlugin(Templater):
            template_fields: ClassVar[tuple[str, ...]] = ("name", "profile")
            template_fields_ext: ClassVar[dict[str, tuple[str, ...]]] = {
                "profile": (".json", ),
            }

            name: str
            profile: str

        plugin = MyPlugin.model_varidate(
            {
                "name": "{{ vars('name') }}",
                "profile": "{{ vars('profile') }}",
            },
            context={
               jinja_renderer="???",
            }
        )
        ```
    """

    template_fields: ClassVar[tuple[str, ...]] = ()
    template_fields_ext: ClassVar[dict[str, tuple[str, ...]]] = {}

    @model_validator(mode="before")
    @classmethod
    def render_template_fields(
        cls,
        data: Any,
        info: ValidationInfo,
    ) -> Any:  # NOSONAR
        """Pre Template Fields validator to add any extra template fields from
        `template_fields_ext` class variable.

        Args:
            data (dict): A model data that will validate.
            info (ValidationInfo): A validation info object that contains
                context data.

        Returns:
            dict | Any: A model data after render the template fields.
        """
        if (
            cls.template_fields
            and isinstance(data, dict)
            and info.context
            and "jinja_renderer" in info.context
        ):
            renderer: JinjaRender = info.context["jinja_renderer"]
            for field_name in cls.template_fields:
                field_value: Any = data.get(field_name)
                from_default: bool = False

                # ℹ️ NOTE: Pre-set default glob variable if the field value is
                #   None. This is to avoid rendering issues with glob variables.
                if (
                    field_value is None
                    and (field := cls.model_fields.get(field_name)) is not None
                    and (default := field.default) is not PydanticUndefined
                    and isinstance(default, str)
                ):
                    logger.debug(
                        "🔍 Pre-Setting default glob variable for %r",
                        field_name,
                    )
                    field_value: str = default
                    from_default: bool = True

                # ℹ️ NOTE: Only render if ``field_value`` is not None or empty
                if field_value:
                    data[field_name] = cls.render_field(
                        field_name,
                        field_value,
                        renderer,
                        from_default=from_default,
                    )
        return data
