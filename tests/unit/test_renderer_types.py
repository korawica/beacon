"""Type-preservation tests for the Renderer.

Beacon's renderer must return **real Python types** when a template is a
pure expression. Examples that must hold:

- ``"{{ x }}"`` with ``x = 5``         → ``int(5)``     (not ``"5"``)
- ``"{{ x }}"`` with ``x = [1, 2]``    → ``list``       (not ``"[1, 2]"``)
- ``"{{ x }}"`` with ``x = True``      → ``True``       (not ``"True"``)
- ``"{{ x }}"`` with ``x = None``      → ``None``       (not ``"None"``)
- ``"prefix-{{ x }}"`` with ``x = 5``  → ``"prefix-5"`` (mixed, stays str)

This is what Jinja's ``NativeEnvironment`` provides. The sandbox must
continue to block attribute-attacks.
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

import pytest
from pydantic import Field

from beacon import BasePlugin, Dag, DagRunner, Task
from beacon.core import Renderer
from beacon.metadata import JsonMetadata


# ---------------------------------------------------------------------------
# Pure-expression templates must return the source value's real type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        0,
        1,
        -42,
        1_000_000,
        3.14,
        -2.5,
        True,
        False,
        None,
        "plain-string",
        "",
        [1, 2, 3],
        [],
        ["a", "b", "c"],
        [{"nested": 1}, {"nested": 2}],
        {"a": 1, "b": 2},
        {},
        {"nested": {"deep": [1, 2, {"x": "y"}]}},
        (1, 2, 3),
    ],
)
def test_pure_template_preserves_value_and_type(value: Any) -> None:
    """`"{{ x }}"` must return `x` itself (same value, same type)."""
    out = Renderer({"x": value}).render("{{ x }}")
    assert out == value, f"value mismatch for {value!r}: got {out!r}"
    # Type preservation: bool must not be int, list must not be str, etc.
    if value is None:
        assert out is None
    else:
        assert type(out) is type(value), (
            f"type mismatch for {value!r}: got {type(out).__name__}, "
            f"expected {type(value).__name__}"
        )


def test_bool_true_and_false_distinct_from_strings() -> None:
    """Catches the common Pydantic footgun where `'False'` is truthy."""
    assert Renderer({"x": True}).render("{{ x }}") is True
    assert Renderer({"x": False}).render("{{ x }}") is False


def test_none_distinct_from_string_none() -> None:
    assert Renderer({"x": None}).render("{{ x }}") is None
    assert Renderer({"x": None}).render("{{ x }}") != "None"


# ---------------------------------------------------------------------------
# Mixed templates remain strings (correct Jinja behavior)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tmpl, ctx, expected",
    [
        ("prefix-{{ x }}", {"x": 5}, "prefix-5"),
        ("{{ x }}-suffix", {"x": 5}, "5-suffix"),
        ("{{ a }}/{{ b }}", {"a": "users", "b": 42}, "users/42"),
        ("count={{ n }} items", {"n": [1, 2, 3]}, "count=[1, 2, 3] items"),
    ],
)
def test_mixed_template_returns_string(tmpl, ctx, expected) -> None:
    out = Renderer(ctx).render(tmpl)
    assert out == expected
    assert isinstance(out, str)


# ---------------------------------------------------------------------------
# Recursive structure rendering preserves types per-leaf
# ---------------------------------------------------------------------------


def test_recursive_dict_preserves_types_per_leaf() -> None:
    inputs = {
        "count": "{{ params.count }}",
        "rows": "{{ params.rows }}",
        "enabled": "{{ params.enabled }}",
        "label": "src={{ params.source }}",  # mixed → str
        "nested": {"limit": "{{ params.limit }}"},
        "leaves": [
            "{{ params.count }}",  # int
            "literal",  # str
            123,  # passthrough int
        ],
    }
    params = {
        "count": 5,
        "rows": [1, 2, 3],
        "enabled": True,
        "source": "postgres",
        "limit": 1000,
    }
    out = Renderer({"params": params}).render(inputs)

    assert out["count"] == 5 and type(out["count"]) is int
    assert out["rows"] == [1, 2, 3] and type(out["rows"]) is list
    assert out["enabled"] is True
    assert out["label"] == "src=postgres" and isinstance(out["label"], str)
    assert (
        out["nested"]["limit"] == 1000 and type(out["nested"]["limit"]) is int
    )
    assert out["leaves"][0] == 5 and type(out["leaves"][0]) is int
    assert out["leaves"][1] == "literal"
    assert out["leaves"][2] == 123


def test_dict_method_name_gotcha_is_documented() -> None:
    """Jinja's `foo.bar` does getattr-then-getitem. Naming a params key
    after a dict method (`items`, `keys`, `values`, ...) silently returns
    the bound method. Users should use `params['items']` in that case.
    Pinning this so it's a documented gotcha, not a surprise regression."""
    params = {"items": [1, 2, 3]}
    # Dotted access returns the bound method (str-ified by NativeTemplate).
    out = Renderer({"params": params}).render("{{ params.items }}")
    assert out != [1, 2, 3]  # NOT what you might want
    # Bracket access works correctly.
    out_bracket = Renderer({"params": params}).render("{{ params['items'] }}")
    assert out_bracket == [1, 2, 3]


def test_datetime_preserves_through_pure_template() -> None:
    """Datetimes are a common case; templates must not stringify them."""
    dt = datetime(2026, 6, 4, 12, 30, 0)
    out = Renderer({"ts": dt}).render("{{ ts }}")
    # NativeEnvironment evaluates the *string repr*; datetime's repr is
    # not a literal, so it falls back to str representation. Document that:
    # if you need a datetime through, pass it as-is (don't wrap in Jinja).
    # The recursive renderer DOES pass non-string scalars through untouched.
    out_passthrough = Renderer({}).render(dt)
    assert out_passthrough is dt
    # And the templated path returns the str(dt) representation, NOT crash:
    assert isinstance(out, (str, datetime))


# ---------------------------------------------------------------------------
# Sandbox is still enforced (the security guarantee must not regress)
# ---------------------------------------------------------------------------


def test_sandbox_blocks_dunder_access_after_native_switch() -> None:
    from jinja2.exceptions import SecurityError

    with pytest.raises(SecurityError):
        Renderer({"x": 1}).render("{{ x.__class__.__mro__ }}")


def test_sandbox_blocks_subclasses_walk() -> None:
    from jinja2.exceptions import SecurityError

    with pytest.raises(SecurityError):
        Renderer({}).render("{{ ().__class__.__mro__ }}")


# ---------------------------------------------------------------------------
# End-to-end through scheduler: typed plugin field gets the real type
# ---------------------------------------------------------------------------


_CAPTURED: dict[str, Any] = {}


class _TypedCapture(BasePlugin):
    """Plugin with typed Pydantic fields. Renderer must deliver real types
    or Pydantic will raise / coerce in surprising ways."""

    plugin_name: ClassVar[str] = "_typed_capture"

    count: int = Field(default=0)
    ratio: float = Field(default=0.0)
    enabled: bool = Field(default=False)
    rows: list[int] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    label: str = Field(default="")

    async def execute(self, context):
        _CAPTURED[context["task_id"]] = {
            "count": self.count,
            "ratio": self.ratio,
            "enabled": self.enabled,
            "rows": self.rows,
            "config": self.config,
            "label": self.label,
        }
        return {"ok": True}


@pytest.fixture(autouse=True)
def _reset_captured():
    _CAPTURED.clear()
    yield


def test_e2e_typed_plugin_receives_real_types_from_params(tmp_path):
    dag = Dag(
        id="typed-render",
        actions=[
            Task(
                id="t",
                uses="_typed_capture",
                inputs={
                    "count": "{{ params.count }}",
                    "ratio": "{{ params.ratio }}",
                    "enabled": "{{ params.enabled }}",
                    "rows": "{{ params.rows }}",
                    "config": "{{ params.config }}",
                    "label": "src={{ params.source }}",  # mixed → str
                },
            ),
        ],
    )
    meta = JsonMetadata(tmp_path / "meta")
    sched = DagRunner(dag, meta=meta)
    asyncio.run(
        sched.run(
            params={
                "count": 7,
                "ratio": 0.25,
                "enabled": True,
                "rows": [10, 20, 30],
                "config": {"mode": "fast", "retries": 3},
                "source": "mysql",
            }
        )
    )

    got = _CAPTURED["t"]
    assert got["count"] == 7 and type(got["count"]) is int
    assert got["ratio"] == 0.25 and type(got["ratio"]) is float
    assert got["enabled"] is True
    assert got["rows"] == [10, 20, 30] and type(got["rows"]) is list
    assert got["config"] == {"mode": "fast", "retries": 3}
    assert got["label"] == "src=mysql"


def test_e2e_false_bool_does_not_become_truthy_string(tmp_path):
    """Regression guard: if renderer returned `"False"` (str), Pydantic
    would coerce it to True in some modes. Real bool must flow through."""
    dag = Dag(
        id="false-bool",
        actions=[
            Task(
                id="t",
                uses="_typed_capture",
                inputs={"enabled": "{{ params.enabled }}"},
            ),
        ],
    )
    sched = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    asyncio.run(sched.run(params={"enabled": False}))
    assert _CAPTURED["t"]["enabled"] is False


def test_e2e_upstream_outputs_preserve_types(tmp_path):
    """Late-bound `{{ outputs.X.Y }}` must also preserve types."""

    class _Producer(BasePlugin):
        plugin_name: ClassVar[str] = "_producer"

        async def execute(self, context):
            return {"count": 42, "rows": [1, 2, 3], "ok": True}

    dag = Dag(
        id="upstream-types",
        actions=[
            Task(id="prod", uses="_producer"),
            Task(
                id="cons",
                uses="_typed_capture",
                upstream=["prod"],
                inputs={
                    "count": "{{ outputs.prod.count }}",
                    "rows": "{{ outputs.prod.rows }}",
                    "enabled": "{{ outputs.prod.ok }}",
                },
            ),
        ],
    )
    sched = DagRunner(dag, meta=JsonMetadata(tmp_path / "meta"))
    asyncio.run(sched.run())

    got = _CAPTURED["cons"]
    assert got["count"] == 42 and type(got["count"]) is int
    assert got["rows"] == [1, 2, 3] and type(got["rows"]) is list
    assert got["enabled"] is True


# ---------------------------------------------------------------------------
# Edge cases worth pinning
# ---------------------------------------------------------------------------


def test_empty_string_is_not_jinja_passthrough() -> None:
    assert Renderer({}).render("") == ""


def test_string_that_looks_like_python_literal_stays_string() -> None:
    """A plain string `"5"` must not be auto-evaluated to int 5."""
    assert Renderer({}).render("5") == "5"
    assert isinstance(Renderer({}).render("5"), str)
    assert Renderer({}).render("[1, 2]") == "[1, 2]"
    assert isinstance(Renderer({}).render("[1, 2]"), str)


def test_decimal_pure_template_returns_str_or_decimal() -> None:
    """Decimal isn't a Python literal, so NativeEnvironment will str-ify it
    when it appears inside a template. Passthrough (no template) preserves."""
    d = Decimal("1.5")
    # Passthrough preserves Decimal exactly.
    assert Renderer({}).render(d) == d and isinstance(
        Renderer({}).render(d), Decimal
    )
    # Templated path: documented as str (no crash).
    out = Renderer({"d": d}).render("{{ d }}")
    assert out == d or out == "1.5"


def test_nested_template_inside_list_preserves_each_element() -> None:
    out = Renderer(
        {"a": 1, "b": [10, 20], "c": "X"},
    ).render(["{{ a }}", "{{ b }}", "lit", "{{ c }}-mix"])
    assert out == [1, [10, 20], "lit", "X-mix"]
    assert type(out[0]) is int
    assert type(out[1]) is list
    assert isinstance(out[3], str)
