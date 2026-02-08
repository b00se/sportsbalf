from src.utils.names import (
    from_last_first,
    normalize_person_name,
    resolve_unique_name_match,
)


def test_normalize_person_name_handles_diacritics_and_spacing() -> None:
    assert normalize_person_name("  Rodr\u00edguez,   Yariel  ") == "rodriguez yariel"


def test_from_last_first_converts_display_format() -> None:
    assert from_last_first("Schlittler, Cam") == "Cam Schlittler"
    assert from_last_first("Cam Schlittler") == "Cam Schlittler"


def test_resolve_unique_name_match_supports_first_name_variants() -> None:
    mapping = {
        "cameron schlittler": 693645,
        "mike burrows": 681347,
    }
    assert resolve_unique_name_match("Cam Schlittler", mapping) == 693645
    assert resolve_unique_name_match("Mike Burrows", mapping) == 681347


def test_resolve_unique_name_match_returns_none_when_ambiguous() -> None:
    mapping = {
        "chris martin": 111,
        "cody martin": 222,
    }
    assert resolve_unique_name_match("Cam Martin", mapping) is None
