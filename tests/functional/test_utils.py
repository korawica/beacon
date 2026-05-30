from beacon.const import PLUGINS_REGISTRY


def test_registry_from_standard():
    assert len(PLUGINS_REGISTRY) == 0

    assert len(PLUGINS_REGISTRY) == 1
    assert "empty" in PLUGINS_REGISTRY
