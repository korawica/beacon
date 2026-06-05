"""Unit tests for bug fixes (plugin registry, bundle recursion, callback cache)."""

import logging
import textwrap


from beacon import BasePlugin
from beacon.callback import CALLBACKS_REGISTRY, Callback, OnTaskEvent
from beacon.core.bundle import GitBundle, LocalBundle
from beacon.core.plugin import PLUGINS_REGISTRY, register_plugin


# ─── plugin registry ─────────────────────────────────────────────────────


def test_subclass_without_explicit_plugin_name_gets_snakecase_name():
    """Subclasses without an explicit plugin_name get snake_case name as fallback."""

    class IntermediateBase(BasePlugin):
        """Intermediate base — gets auto-registered with snakecase name."""

        # Intentionally NO plugin_name declared here.

        async def execute(self, context):  # pragma: no cover
            ...

    # With new behavior: auto-registered under snakecase name
    assert "intermediate_base" in PLUGINS_REGISTRY
    assert PLUGINS_REGISTRY["intermediate_base"] is IntermediateBase
    # Class gets the auto-generated name
    assert IntermediateBase.plugin_name == "intermediate_base"
    # Clean up
    PLUGINS_REGISTRY.pop("intermediate_base", None)


def test_abstract_base_class_is_not_registered():
    """Abstract bases with unimplemented execute() are NOT registered."""

    from abc import abstractmethod

    class AbstractGcsBase(BasePlugin):
        """Abstract base for plugin family - should NOT be registered."""

        source_bucket: str
        dest_bucket: str

        @abstractmethod
        async def execute(self, context):
            """Subclasses must implement."""

    class GcsCopyTask(AbstractGcsBase, plugin_name="gcs-copy-test"):
        """Concrete implementation - SHOULD be registered."""

        async def execute(self, context):
            return {"copied": 0}

    # Abstract base should NOT be registered
    assert "abstract_gcs_base" not in PLUGINS_REGISTRY
    # Concrete subclass should be registered
    assert "gcs-copy-test" in PLUGINS_REGISTRY
    assert PLUGINS_REGISTRY["gcs-copy-test"] is GcsCopyTask
    # Clean up
    PLUGINS_REGISTRY.pop("gcs-copy-test", None)


def test_subclass_with_plugin_name_is_registered():
    """Subclasses that declare plugin_name auto-register."""

    from typing import ClassVar

    class ConcretePlugin(BasePlugin):
        plugin_name: ClassVar[str] = "_test_concrete_plugin"

        async def execute(self, context):
            return None

    try:
        assert PLUGINS_REGISTRY["_test_concrete_plugin"] is ConcretePlugin
    finally:
        PLUGINS_REGISTRY.pop("_test_concrete_plugin", None)


def test_registry_override_warns(caplog):
    """Re-registering an existing name without allow_override warns."""
    from typing import ClassVar

    class FirstPlugin(BasePlugin):
        plugin_name: ClassVar[str] = "_test_override"

        async def execute(self, context): ...

    try:
        with caplog.at_level(logging.WARNING, logger="beacon.core.plugin"):

            class SecondPlugin(BasePlugin):
                plugin_name: ClassVar[str] = "_test_override"

                async def execute(self, context): ...

        assert PLUGINS_REGISTRY["_test_override"] is SecondPlugin
        assert any("being overridden" in r.message for r in caplog.records)
    finally:
        PLUGINS_REGISTRY.pop("_test_override", None)


def test_register_plugin_allow_override_silences_warning(caplog):
    from typing import ClassVar

    class A(BasePlugin):
        plugin_name: ClassVar[str] = "_test_allow"

        async def execute(self, context): ...

    class B(BasePlugin):
        plugin_name: ClassVar[str] = "_other_for_allow"

        async def execute(self, context): ...

    try:
        with caplog.at_level(logging.WARNING, logger="beacon.core.plugin"):
            register_plugin(B, name="_test_allow", allow_override=True)
        assert PLUGINS_REGISTRY["_test_allow"] is B
        assert not any("being overridden" in r.message for r in caplog.records)
    finally:
        PLUGINS_REGISTRY.pop("_test_allow", None)
        PLUGINS_REGISTRY.pop("_other_for_allow", None)


# ─── GitBundle.local recursion ───────────────────────────────────────────


def test_git_bundle_local_does_not_recurse(tmp_path):
    """Previously GitBundle.local was `return self.local` → infinite recursion."""
    gb = GitBundle(
        name="demo",
        repo_url="https://example.com/repo.git",
        sync_path=tmp_path,
    )
    # Two accesses should return the same cached LocalBundle.
    a = gb.local
    b = gb.local
    assert a is b
    assert isinstance(a, LocalBundle)


def test_git_bundle_subpath_navigates(tmp_path):
    gb = GitBundle(
        name="demo",
        repo_url="https://example.com/repo.git",
        sync_path=tmp_path,
        sub_path="workflows",
    )
    assert gb.local.path == (tmp_path / "demo" / "workflows").resolve()


# ─── Bundle plugin discovery snapshot/diff ───────────────────────────────


def test_local_bundle_load_plugins_detects_new_registrations(tmp_path):
    plugins_dir = tmp_path / "bundleA" / "plugins"
    plugins_dir.mkdir(parents=True)
    (plugins_dir / "my_plug.py").write_text(
        textwrap.dedent("""
        from typing import ClassVar
        from beacon import BasePlugin

        class MyBundlePlug(BasePlugin):
            plugin_name: ClassVar[str] = "_bundle_test_plug"
            async def execute(self, context):
                return {}
    """)
    )

    try:
        bundle = LocalBundle(name="bundleA", path=tmp_path / "bundleA")
        registered = bundle.load_plugins()
        assert "_bundle_test_plug" in registered
        assert "_bundle_test_plug" in PLUGINS_REGISTRY
    finally:
        PLUGINS_REGISTRY.pop("_bundle_test_plug", None)


def test_local_bundle_no_plugins_dir_returns_empty(tmp_path):
    bundle = LocalBundle(name="empty", path=tmp_path)
    assert bundle.load_plugins() == []


def test_local_bundle_version_changes_on_file_modification(tmp_path):
    (tmp_path / "dag.yml").write_text("id: x\n")
    b = LocalBundle(name="versioned", path=tmp_path)
    v1 = b.version
    # New bundle instance to bypass cache; modify file mtime.
    import os
    import time

    time.sleep(0.01)
    os.utime(tmp_path / "dag.yml", None)
    (tmp_path / "dag.yml").write_text("id: y\n")
    b2 = LocalBundle(name="versioned", path=tmp_path)
    assert b2.version != v1


# ─── Callback caching ────────────────────────────────────────────────────


def test_callback_instance_is_cached(monkeypatch):
    """OnTaskEvent should resolve and instantiate the hook only once."""
    init_count = {"n": 0}

    from typing import ClassVar

    class CountingHook(Callback):
        hook_name: ClassVar[str] = "_test_counting"

        def __init__(self, **kwargs):
            init_count["n"] += 1

        async def notify(self, event, data):
            return None

    try:
        evt = OnTaskEvent(on_event="success", hook="_test_counting")
        evt._get_resolved()
        evt._get_resolved()
        evt._get_resolved()
        assert init_count["n"] == 1
    finally:
        CALLBACKS_REGISTRY.pop("_test_counting", None)
