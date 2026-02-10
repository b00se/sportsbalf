"""Legacy CLI stub retained for compatibility.

Use `pipeline/main.py` and `src/pipeline/engine.py` as authoritative execution paths.
"""

MODULE_STATUS = "legacy_non_authoritative"
AUTHORITATIVE_ENTRYPOINT = "pipeline/main.py -> src/pipeline/engine.py"
STATUS_NOTE = "Compatibility stub only; do not extend as canonical pipeline entry."

# Entry point CLI stub
if __name__ == "__main__":
    print("sportsbalf CLI running...")
