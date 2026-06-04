"""Tests for ``beacon.cli.settings``."""

import pytest

from beacon.cli import settings


def test_defaults_when_no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, *_ in settings._SPEC:
        monkeypatch.delenv(name, raising=False)
    s = settings.load_settings()
    assert s["BEACON_METADATA_PATH"].value == "./metadata.db"
    assert s["BEACON_METADATA_PATH"].source == "default"
    assert s["BEACON_LOG_BATCH_SIZE"].value == 100


def test_env_override_with_cast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BEACON_LOG_BATCH_SIZE", "42")
    monkeypatch.setenv("BEACON_METADATA_PATH", "/tmp/meta")
    s = settings.load_settings()
    assert s["BEACON_LOG_BATCH_SIZE"].value == 42
    assert s["BEACON_LOG_BATCH_SIZE"].source == "env"
    assert s["BEACON_METADATA_PATH"].value == "/tmp/meta"


def test_bad_cast_preserves_raw_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BEACON_LOG_BATCH_SIZE", "not-an-int")
    s = settings.load_settings()
    assert s["BEACON_LOG_BATCH_SIZE"].value == "not-an-int"
    assert s["BEACON_LOG_BATCH_SIZE"].source == "env"


def test_get_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BEACON_METADATA_PATH", "/x")
    assert settings.get("BEACON_METADATA_PATH") == "/x"
