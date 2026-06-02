from beacon.core import PLUGINS_REGISTRY
from beacon.utils import to_snake_case

import pytest


def test_registry_from_standard():
    print(PLUGINS_REGISTRY)
    # assert len(PLUGINS_REGISTRY) == 0

    # from beacon.providers.standard.plugins import EmptyPlugin  # noqa

    assert len(PLUGINS_REGISTRY) > 0
    assert "empty" in PLUGINS_REGISTRY


class TestToSnakeCase:
    @pytest.mark.parametrize(
        "s, expect",
        [
            ("HTTPResponseCodeXYZ", "http_response_code_xyz"),
            ("camel2_camel2_case", "camel2_camel2_case"),
            ("getHTTPResponseCode", "get_http_response_code"),
            ("HTTPResponseCodeXYZ", "http_response_code_xyz"),
        ],
    )
    def test_to_snake_case(self, s: str, expect: str):
        assert to_snake_case(s) == expect
