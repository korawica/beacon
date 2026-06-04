"""Tests for py_statement inline code and template rendering."""

import asyncio


from beacon import Dag, DagRunner, Task
from beacon.metadata import LocalMetadata
from beacon.providers.standard.plugins.task.python import PythonPlugin


class TestPyStatementInline:
    """Test inline Python code via py_statement."""

    def test_inline_code_basic(self, tmp_path):
        """Execute inline Python code string."""
        dag = Dag(
            id="inline-basic",
            actions=[
                Task(
                    id="inline-task",
                    uses="py",
                    inputs={
                        "py_statement": """
def main():
    return {"status": "done", "count": 42}
""",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(DagRunner(dag, meta=meta).run())
        assert result.state == "success"
        assert result.outputs["inline-task"]["status"] == "done"
        assert result.outputs["inline-task"]["count"] == 42

    def test_inline_code_with_params(self, tmp_path):
        """Inline code can access plugin params (passed as kwargs to function)."""
        dag = Dag(
            id="inline-params",
            actions=[
                Task(
                    id="inline-task",
                    uses="py",
                    inputs={
                        "py_statement": """
def main(source):
    return {"source": source}
""",
                        "py_function": "main",
                        "params": {"source": "test-source"},
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(DagRunner(dag, meta=meta).run())
        assert result.state == "success"
        assert result.outputs["inline-task"]["source"] == "test-source"

    def test_inline_code_with_jinja_template(self, tmp_path):
        """Inline code is rendered with Jinja using DAG params."""
        dag = Dag(
            id="inline-jinja",
            actions=[
                Task(
                    id="inline-task",
                    uses="py",
                    inputs={
                        "py_statement": """
def main():
    source = "{{ params.source }}"
    return {"source": source}
""",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        # Pass DAG-level params which are available in Jinja context
        result = asyncio.run(
            DagRunner(dag, meta=meta).run(params={"source": "rendered-value"})
        )
        assert result.state == "success"
        assert result.outputs["inline-task"]["source"] == "rendered-value"


class TestPyStatementFileRendering:
    """Test Jinja rendering of .py file contents."""

    def test_file_with_jinja_template(self, tmp_path):
        """File contents with Jinja templates are rendered."""
        # Create assets directory and Python file
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        script = assets_dir / "script.py"
        script.write_text("""
def main():
    source = "{{ params.source }}"
    return {"source": source}
""")

        dag = Dag(
            id="file-jinja",
            actions=[
                Task(
                    id="file-task",
                    uses="py",
                    inputs={
                        "py_statement": "script.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        # Pass bundle_root to DagRunner for asset resolution
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run(
                params={"source": "file-rendered"}
            )
        )
        assert result.state == "success"
        assert result.outputs["file-task"]["source"] == "file-rendered"

    def test_file_with_raw_jinja_block(self, tmp_path):
        """Jinja raw blocks preserve literal braces."""
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        script = assets_dir / "script.py"
        script.write_text("""
def main():
    # {% raw %}{{ params.source }}{% endraw %} stays literal
    template = "{% raw %}{{ params.source }}{% endraw %}"
    return {"template": template}
""")

        dag = Dag(
            id="file-raw",
            actions=[
                Task(
                    id="file-task",
                    uses="py",
                    inputs={
                        "py_statement": "script.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run(
                params={"source": "should-not-appear"}
            )
        )
        assert result.state == "success"
        # The raw block should preserve literal braces
        assert result.outputs["file-task"]["template"] == "{{ params.source }}"

    def test_file_without_jinja_unchanged(self, tmp_path):
        """File without Jinja templates is unchanged."""
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        script = assets_dir / "script.py"
        script.write_text("""
def main():
    return {"value": 123}
""")

        dag = Dag(
            id="file-no-jinja",
            actions=[
                Task(
                    id="file-task",
                    uses="py",
                    inputs={
                        "py_statement": "script.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run()
        )
        assert result.state == "success"
        assert result.outputs["file-task"]["value"] == 123


class TestTemplateExt:
    """Test template_ext class variable behavior."""

    def test_py_plugin_has_template_ext(self):
        """PythonPlugin should have template_ext = (".py",)."""
        assert PythonPlugin.template_ext == (".py",)

    def test_base_plugin_default_empty(self):
        """BasePlugin should have empty template_ext by default."""
        from beacon.core import BasePlugin

        assert BasePlugin.template_ext == ()


class TestJinjaAdvancedFeatures:
    """Test advanced Jinja features (extends, include, for loops)."""

    def test_file_with_for_loop(self, tmp_path):
        """Jinja for loops work in template files."""
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        script = assets_dir / "script.py"
        script.write_text("""
def main():
    # Simple for loop example
    parts = []
{% for name in params.names %}
    parts.append("{{ name }}")
{% endfor %}
    return {"names": parts}
""")

        dag = Dag(
            id="for-loop",
            actions=[
                Task(
                    id="for-task",
                    uses="py",
                    inputs={
                        "py_statement": "script.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run(
                params={"names": ["alice", "bob", "charlie"]}
            )
        )
        assert result.state == "success"
        assert result.outputs["for-task"]["names"] == [
            "alice",
            "bob",
            "charlie",
        ]

    def test_file_with_extends(self, tmp_path):
        """Jinja template inheritance with extends works."""
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()

        # Base template
        base = assets_dir / "base.py"
        base.write_text("""
def main():
    result = {% block content %}base{% endblock %}
    return {"result": result}
""")

        # Child template
        child = assets_dir / "child.py"
        child.write_text("""{% extends "base.py" %}
{% block content %}"{{ params.value }}"{% endblock %}
""")

        dag = Dag(
            id="extends",
            actions=[
                Task(
                    id="extends-task",
                    uses="py",
                    inputs={
                        "py_statement": "child.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run(
                params={"value": "child-value"}
            )
        )
        assert result.state == "success"
        assert result.outputs["extends-task"]["result"] == "child-value"

    def test_file_with_include(self, tmp_path):
        """Jinja include works in template files."""
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()

        # Partial template
        partial = assets_dir / "partial.py"
        partial.write_text('partial_value = "{{ params.shared }}"')

        # Main template
        main = assets_dir / "main.py"
        main.write_text("""
{% include "partial.py" %}

def main():
    return {"value": partial_value}
""")

        dag = Dag(
            id="include",
            actions=[
                Task(
                    id="include-task",
                    uses="py",
                    inputs={
                        "py_statement": "main.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run(
                params={"shared": "shared-value"}
            )
        )
        assert result.state == "success"
        assert result.outputs["include-task"]["value"] == "shared-value"

    def test_file_in_subdirectory(self, tmp_path):
        """Template files can be in subdirectories."""
        assets_dir = tmp_path / "assets"
        scripts_dir = assets_dir / "scripts"
        scripts_dir.mkdir(parents=True)

        script = scripts_dir / "transform.py"
        script.write_text("""
def main():
    return {"path": "scripts/transform.py"}
""")

        dag = Dag(
            id="subdir",
            actions=[
                Task(
                    id="subdir-task",
                    uses="py",
                    inputs={
                        "py_statement": "scripts/transform.py",
                        "py_function": "main",
                    },
                ),
            ],
        )
        meta = LocalMetadata(tmp_path / "meta")
        result = asyncio.run(
            DagRunner(dag, meta=meta, bundle_root=tmp_path).run()
        )
        assert result.state == "success"
        assert result.outputs["subdir-task"]["path"] == "scripts/transform.py"
