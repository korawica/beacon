from beacon.models.group import Group
from beacon.models.task import Task
from beacon.models.sensor import Sensor
from beacon.core import PLUGINS_REGISTRY


def test_group():
    from beacon.providers.standard.plugins import EmptyPlugin  # noqa

    print(PLUGINS_REGISTRY)

    group = Group(
        id="test",
        actions=[
            Task(id="start", uses="empty"),
            Group(
                id="nested",
                actions=[Sensor(id="sensor", uses="empty")],
            ),
        ],
    )
    assert group.id == "test"
    assert len(group.actions) == 2
