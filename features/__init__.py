"""Compatibility shim for tests expecting `features.*`.

This package aliases `features.pitcher_enrichment` to
`src.mlb.features.pitcher_enrichment` so tests that monkeypatch using the
short path continue to work.
"""

from __future__ import annotations

import importlib
import sys

# Import the real module
_real_mod = importlib.import_module("src.mlb.features.pitcher_enrichment")

# Expose it under the short alias in sys.modules
sys.modules.setdefault("features.pitcher_enrichment", _real_mod)

# Also make it accessible as an attribute on the package for getattr() lookups
pitcher_enrichment = _real_mod
