from beacon.core import PLUGINS_REGISTRY


def test_registry_from_standard():
    print(PLUGINS_REGISTRY)
    # assert len(PLUGINS_REGISTRY) == 0

    # from beacon.providers.standard.plugins import EmptyPlugin  # noqa

    assert len(PLUGINS_REGISTRY) == 2
    assert "empty" in PLUGINS_REGISTRY
