from typing import ClassVar

from pydantic import BaseModel


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
