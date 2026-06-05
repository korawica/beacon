"""
Example: Plugin Family Pattern for GCS Copy Operations

This demonstrates how to create a shared base class with common configuration
and specialized plugins for each action type (task, sensor, branch, short_circuit).

Key insight: Abstract base classes (with @abstractmethod) are NOT registered
in the plugin registry, allowing them to serve as intermediate bases.
"""

from abc import abstractmethod
from typing import Any

from beacon.core import (
    BasePlugin,
    BaseTaskPlugin,
    BaseSensorPlugin,
    BaseBranchPlugin,
    BaseShortCircuitPlugin,
    Context,
)


class GcsCopyBase(BasePlugin):
    """Abstract base for GCS copy operations.

    This class is NOT registered in PLUGINS_REGISTRY because it has
    an abstract method. It serves as a shared configuration holder.
    """

    source_bucket: str
    dest_bucket: str
    prefix: str = ""

    @abstractmethod
    async def execute(self, context: Context) -> dict[str, Any]:
        """Subclasses must implement."""
        ...

    async def _copy_files(self) -> dict[str, Any]:
        """Shared copy logic used by all concrete plugins."""
        # Simulated copy logic
        print(f"Copying from gs://{self.source_bucket}/{self.prefix}")
        print(f"         to gs://{self.dest_bucket}/{self.prefix}")
        return {
            "source_bucket": self.source_bucket,
            "dest_bucket": self.dest_bucket,
            "prefix": self.prefix,
            "files_copied": 42,
        }

    async def _list_files(self) -> list[str]:
        """List files in source bucket with prefix."""
        # Simulated list logic
        return [f"{self.prefix}/file_{i}.csv" for i in range(10)]


class GcsCopy(GcsCopyBase, BaseTaskPlugin, plugin_name="gcs-copy"):
    """Task: Copy files from source to dest bucket.

    Inherits compatible_actions=("task",) from BaseTaskPlugin.

    Usage:
        ```yaml
        - id: copy-files
          type: task
          uses: gcs-copy
          inputs:
            source_bucket: my-source
            dest_bucket: my-dest
            prefix: data/
        ```
    """

    async def execute(self, context: Context) -> dict[str, Any]:
        return await self._copy_files()


class GcsCopySensor(GcsCopyBase, BaseSensorPlugin, plugin_name="gcs-copy-sensor"):
    """Sensor: Wait for files to exist, then copy.

    Inherits compatible_actions=("sensor",) from BaseSensorPlugin.

    Usage:
        ```yaml
        - id: wait-and-copy
          type: sensor
          uses: gcs-copy-sensor
          inputs:
            source_bucket: my-source
            dest_bucket: my-dest
            prefix: data/
          check_interval: 30
          execution_timeout: 3600
        ```
    """

    async def execute(self, context: Context) -> dict[str, Any]:
        import asyncio

        check_interval = context.get("check_interval", 60)

        while True:
            files = await self._list_files()
            if files:
                result = await self._copy_files()
                result["condition_met"] = True
                result["files_found"] = len(files)
                return result
            await asyncio.sleep(check_interval)


class GcsCopyBranch(GcsCopyBase, BaseBranchPlugin, plugin_name="gcs-copy-branch"):
    """Branch: Copy if enough files exist, choose path.

    Inherits compatible_actions=("branch",) from BaseBranchPlugin.

    Usage:
        ```yaml
        - id: check-and-copy
          type: branch
          uses: gcs-copy-branch
          inputs:
            source_bucket: my-source
            dest_bucket: my-dest
            prefix: data/
            min_files: 10
          success: [process-large-batch]
          failure: [process-small-batch]
        ```
    """

    min_files: int = 1

    async def execute(self, context: Context) -> dict[str, Any]:
        files = await self._list_files()

        if len(files) >= self.min_files:
            result = await self._copy_files()
            return {"branch": context.get("success", []), **result}
        return {"branch": context.get("failure", [])}


class GcsCopyShortCircuit(GcsCopyBase, BaseShortCircuitPlugin, plugin_name="gcs-copy-shortcircuit"):
    """ShortCircuit: Copy if files exist, else skip all downstream.

    Inherits compatible_actions=("short_circuit",) from BaseShortCircuitPlugin.

    Usage:
        ```yaml
        - id: maybe-copy
          type: short_circuit
          uses: gcs-copy-shortcircuit
          inputs:
            source_bucket: my-source
            dest_bucket: my-dest
            prefix: data/
        ```
    """

    async def execute(self, context: Context) -> dict[str, Any]:
        files = await self._list_files()

        if files:
            result = await self._copy_files()
            return {"continue": True, **result}
        return {"continue": False}


# Verification (when run directly)
if __name__ == "__main__":
    from beacon.core.plugin import PLUGINS_REGISTRY

    print("Specialized Base Classes:")
    print(f"  BaseTaskPlugin.actions: {BaseTaskPlugin.compatible_actions}")
    print(f"  BaseSensorPlugin.actions: {BaseSensorPlugin.compatible_actions}")
    print(f"  BaseBranchPlugin.actions: {BaseBranchPlugin.compatible_actions}")
    print(f"  BaseShortCircuitPlugin.actions: {BaseShortCircuitPlugin.compatible_actions}")
    print()

    print("Registered plugins:")
    for name in sorted(PLUGINS_REGISTRY.keys()):
        if "gcs" in name:
            cls = PLUGINS_REGISTRY[name]
            print(f"  {name}: {cls.__name__} (actions: {cls.compatible_actions})")

    print()
    print("GcsCopyBase NOT registered:", "gcs_copy_base" not in PLUGINS_REGISTRY)
    print("GcsCopy registered:", "gcs-copy" in PLUGINS_REGISTRY)
    print("GcsCopySensor registered:", "gcs-copy-sensor" in PLUGINS_REGISTRY)
    print("GcsCopyBranch registered:", "gcs-copy-branch" in PLUGINS_REGISTRY)
    print("GcsCopyShortCircuit registered:", "gcs-copy-shortcircuit" in PLUGINS_REGISTRY)
