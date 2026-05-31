from re import Pattern, compile, DOTALL, VERBOSE

JINJA_PATTERN: Pattern[str] = compile(
    r"""
    (
        \{\{.*?}}           # Case for {{ ... }} tags
        |\{%.*?%}           # Case for {% ... %} tags
        |\{#.*?#}           # Case for {# ... #} comments
    )
    """,
    DOTALL | VERBOSE,
)
