"""Surface-area clarity tests for authoritative vs legacy modules."""

from __future__ import annotations

import ast
from pathlib import Path


def _module_constant(module_path: str, name: str) -> str | None:
    tree = ast.parse(Path(module_path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = node.value
                    if isinstance(value, ast.Constant) and isinstance(value.value, str):
                        return value.value
    return None


def test_authoritative_entrypoint_markers_present() -> None:
    assert (
        _module_constant("pipeline/main.py", "MODULE_STATUS")
        == "authoritative_entrypoint"
    )
    assert (
        _module_constant("src/pipeline/engine.py", "MODULE_STATUS")
        == "authoritative_engine"
    )


def test_legacy_markers_present_with_canonical_pointer() -> None:
    legacy_modules = [
        "cli/main.py",
        "src/models/ensemble.py",
        "ingest/parse_ud_strikeouts.py",
        "ingest/park_factors.py",
    ]

    for module_path in legacy_modules:
        assert (
            _module_constant(module_path, "MODULE_STATUS") == "legacy_non_authoritative"
        )

        authoritative_entrypoint = _module_constant(
            module_path, "AUTHORITATIVE_ENTRYPOINT"
        )
        assert authoritative_entrypoint is not None
        assert authoritative_entrypoint.strip() != ""
        assert (
            "pipeline/main.py" in authoritative_entrypoint
            or "src/pipeline/engine.py" in authoritative_entrypoint
        )

        status_note = _module_constant(module_path, "STATUS_NOTE")
        assert status_note is not None
        assert status_note.strip() != ""
