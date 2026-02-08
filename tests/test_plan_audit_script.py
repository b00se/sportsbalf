from __future__ import annotations

from datetime import date

from scripts.audit_plans_doneness import collect_plan_records, render_audit


def test_collect_plan_records_and_render(tmp_path) -> None:
    plans_root = tmp_path / "docs" / "plans"
    planned_dir = plans_root / "planned"
    implemented_dir = plans_root / "implemented"
    planned_dir.mkdir(parents=True)
    implemented_dir.mkdir(parents=True)

    (planned_dir / "foo-plan.md").write_text(
        "# Foo Plan\n\nStatus: Planned\n\nDetails...\n",
        encoding="utf-8",
    )
    (implemented_dir / "bar-plan.md").write_text(
        "# Bar Plan\n\nStatus: Implemented\n\nDone.\n",
        encoding="utf-8",
    )
    (planned_dir / "baz-plan.md").write_text(
        "# Baz Plan\n\nStatus: Implemented\n\nNeeds move.\n",
        encoding="utf-8",
    )

    records = collect_plan_records(plans_root)

    assert len(records) == 3
    by_title = {record.title: record for record in records}
    assert by_title["Foo Plan"].verdict == "Planned"
    assert by_title["Bar Plan"].verdict == "Implemented"
    assert by_title["Baz Plan"].verdict == "Ready to move to implemented"

    report = render_audit(records, date(2026, 2, 8))
    assert "Plan Doneness Auto-Audit (2026-02-08)" in report
    assert "| `" in report
    assert "Ready to move to implemented" in report
