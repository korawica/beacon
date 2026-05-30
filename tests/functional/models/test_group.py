from beacon.models.group import Group
from beacon.models.task import Task
from beacon.models.sensor import Sensor


def test_group():
    group = Group(
        id="test",
        tasks=[
            Task(id="start", uses="empty"),
            Group(
                id="nested",
                tasks=[Sensor(id="sensor", uses="empty")],
            ),
        ],
    )
    assert group.id == "test"
    assert len(group.tasks) == 2
