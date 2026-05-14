"""Human-readable formatting for saved MLB slip artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _humanize_stat(stat_id: object) -> str:
    """Return a human-readable stat label.

    Args:
        stat_id: Raw stat identifier from a slip leg.

    Returns:
        Printable stat label.
    """

    stat_text = str(stat_id or "").strip()
    if not stat_text:
        return "stat"
    return stat_text.replace("_", " ")


def _format_decimal(value: object, digits: int = 2) -> str:
    """Return a compact decimal string for numeric values.

    Args:
        value: Value to format.
        digits: Decimal precision before trimming trailing zeroes.

    Returns:
        Compact string representation or ``n/a`` when missing.
    """

    numeric = pd.to_numeric([value], errors="coerce")[0]
    if pd.isna(numeric):
        return "n/a"
    text = f"{float(numeric):.{digits}f}"
    return text.rstrip("0").rstrip(".")


def _format_percent(value: object) -> str:
    """Return a percentage string for a probability value.

    Args:
        value: Probability in ``[0, 1]``.

    Returns:
        Percentage string or ``n/a`` when missing.
    """

    numeric = pd.to_numeric([value], errors="coerce")[0]
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric) * 100:.1f}%"


def _format_signed_decimal(value: object, digits: int = 2) -> str:
    """Return a signed decimal string for numeric values.

    Args:
        value: Value to format.
        digits: Decimal precision.

    Returns:
        Signed decimal string or ``n/a`` when missing.
    """

    numeric = pd.to_numeric([value], errors="coerce")[0]
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric):+.{digits}f}"


def _predicted_value_for_leg(leg: dict[str, Any]) -> object:
    """Return the best available model prediction for a slip leg.

    Args:
        leg: Slip leg payload.

    Returns:
        Matching prediction value when available.
    """

    prediction_column = (
        f"predicted_{str(leg.get('stat_id', '')).strip().lower()}" if leg else ""
    )
    if prediction_column and prediction_column in leg:
        return leg.get(prediction_column)
    return leg.get("predicted_value")


def _format_leg_line(leg: dict[str, Any], index: int) -> str:
    """Return a single human-readable line for a slip leg.

    Args:
        leg: Slip leg payload.
        index: 1-based leg index within the slip.

    Returns:
        Rendered human-readable leg line.
    """

    player = str(leg.get("player") or leg.get("pitcher_name") or "Unknown player")
    team = str(leg.get("team") or leg.get("pitcher_team") or "?")
    opponent = str(leg.get("opponent") or leg.get("upcoming_opponent") or "?")
    stat = _humanize_stat(leg.get("stat_id") or leg.get("market"))
    play = str(leg.get("play") or "").lower()
    line = _format_decimal(leg.get("line"), digits=1)
    predicted = _format_decimal(_predicted_value_for_leg(leg))
    prob = _format_percent(leg.get("prob"))
    ev = _format_signed_decimal(leg.get("ev"))
    return (
        f"{index}. {player} ({team}) vs {opponent}: {stat} {play} {line} "
        f"[model {predicted}, win {prob}, ev {ev}]"
    )


def render_slip_payloads(slip_payloads: dict[str, list[dict[str, Any]]]) -> str:
    """Return a human-readable report for grouped slip payloads.

    Args:
        slip_payloads: Mapping of slip tags to slip lists.

    Returns:
        Multi-line human-readable report.
    """

    lines: list[str] = []
    for tag, slips in slip_payloads.items():
        lines.append(f"{tag.upper()} SLIPS")
        if not slips:
            lines.append("None.")
            lines.append("")
            continue

        for slip_index, slip in enumerate(slips, start=1):
            lines.append(
                " | ".join(
                    [
                        f"Slip {slip_index}",
                        f"{int(slip.get('slip_size', len(slip.get('legs', []))))} legs",
                        f"units {slip.get('units', 1)}",
                        f"win {_format_percent(slip.get('p_win'))}",
                        f"total ev {_format_signed_decimal(slip.get('total_ev'))}",
                    ]
                )
            )
            for leg_index, leg in enumerate(slip.get("legs", []), start=1):
                lines.append(_format_leg_line(leg, leg_index))
            lines.append("")

    return "\n".join(lines).rstrip()


def render_slip_report(
    summary: dict[str, Any], tags: tuple[str, ...] | None = None
) -> str:
    """Return a human-readable report for saved slip files in a summary.

    Args:
        summary: Summary artifact containing ``slip_files``.
        tags: Optional subset of slip tags to print.

    Returns:
        Multi-line human-readable report.

    Raises:
        ValueError: If the summary does not contain requested slip files.
        FileNotFoundError: If a referenced slip artifact is missing.
    """

    slip_files = summary.get("slip_files")
    if not isinstance(slip_files, dict) or not slip_files:
        raise ValueError("Summary file does not contain any slip_files entries.")

    requested_tags = tags or tuple(slip_files.keys())
    slip_payloads: dict[str, list[dict[str, Any]]] = {}
    for tag in requested_tags:
        if tag not in slip_files:
            raise ValueError(f"Requested slip tag '{tag}' not found in summary.")
        path = Path(str(slip_files[tag]))
        if not path.exists():
            raise FileNotFoundError(f"Slip artifact not found: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        slips = payload.get("slips")
        if not isinstance(slips, list):
            raise ValueError(f"Slip artifact {path} does not contain a 'slips' list.")
        slip_payloads[tag] = slips

    return render_slip_payloads(slip_payloads)


def load_summary(summary_path: Path) -> dict[str, Any]:
    """Load a summary artifact from disk.

    Args:
        summary_path: Path to a summary JSON file.

    Returns:
        Parsed JSON payload.
    """

    return json.loads(summary_path.read_text(encoding="utf-8"))
