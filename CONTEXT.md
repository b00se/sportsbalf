# Sportsbalf

Sportsbalf produces model-driven betting analysis artifacts for supported sports and markets. In the MLB Underdog workflow, the system scores live slates and emits reviewable slip artifacts for human action, not account automation.

## Language

**Shadow Run**:
A manual workflow that fetches live lines, scores a slate, and writes artifacts without placing picks.
_Avoid_: Auto-submit, autobet, live execution

**Live Line Snapshot**:
A dated file containing normalized sportsbook lines for one stat on one slate.
_Avoid_: Raw payload, permanent source of truth

**Candidate Leg**:
A single scored over/under prop that is eligible to be combined into a slip.
_Avoid_: Slip, betslip

**Slip Artifact**:
A generated JSON representation of one or more candidate legs proposed for manual review.
_Avoid_: Submitted entry, placed pick

**Manual Submission**:
Human entry of a reviewed slip artifact into Underdog outside this repository.
_Avoid_: Automated submission, account automation

**Proof Run**:
A fast shadow run used to verify that the live workflow produces scored candidates and slip artifacts with the current code and models.
_Avoid_: Freshness run, betting recommendation run

**Freshness Run**:
A shadow run performed only after current-season inputs and models have been refreshed for recommendation-quality output.
_Avoid_: Smoke test, proof run

**Debug Run**:
A targeted live-shadow run used to inspect specific markets or overrides without claiming proof-run validity.
_Avoid_: Proof run, recommendation run

**Sanity Check Failure**:
A proof-run outcome where artifacts may exist but the candidate pool shows signs of pipeline corruption or implausible model behavior.
_Avoid_: Benign warning, acceptable partial failure

**Stat-Mix Gate**:
A hard proof-run check that fails when candidate-leg coverage is implausibly dominated by one stat.
_Avoid_: Normal slate imbalance, diversity preference

**Confidence Gate**:
A hard proof-run check that fails when model probabilities or EV values are implausibly extreme.
_Avoid_: Aggressive but plausible edge, ranking preference

**Slip-Eligible Pool**:
The ranked candidate-leg pool remaining after normalization and EV-based filtering that is actually considered for slip construction.
_Avoid_: Full scored slate, raw provider payload

**Proof-Run Verdict**:
An explicit pass/fail outcome for a proof run derived from runtime completion and sanity-gate results.
_Avoid_: Implicit interpretation, operator guesswork

**Proof-Run Failure Reason**:
A structured explanation attached to a failed proof-run verdict that identifies the blocking class of problem.
_Avoid_: Freeform log spelunking, unlabeled failure

**Proof-Run Evidence**:
The compact summary data emitted with a proof-run verdict so the operator can distinguish healthy output, abstention, and pipeline breakage.
_Avoid_: Raw artifact inspection, vague pass/fail

**No-Play Slate**:
A healthy run outcome where candidates were scored but no slip should be manually submitted because the slate does not justify one under current rules.
_Avoid_: Runtime failure, broken pipeline

## Relationships

- A **Shadow Run** produces one or more **Live Line Snapshots**
- A **Shadow Run** scores **Candidate Legs** from the current slate
- A **Slip Artifact** contains one or more **Candidate Legs**
- A **Manual Submission** may be based on a reviewed **Slip Artifact**
- A **Proof Run** is a kind of **Shadow Run**
- A **Freshness Run** is a kind of **Shadow Run**
- A **Freshness Run** may follow a successful **Proof Run**
- A **Debug Run** is a kind of **Shadow Run**
- A **Sanity Check Failure** makes a **Proof Run** unsuccessful even if **Slip Artifacts** were written
- A **Stat-Mix Gate** is one kind of **Sanity Check Failure**
- A **Confidence Gate** is one kind of **Sanity Check Failure**
- The **Stat-Mix Gate** and **Confidence Gate** inspect the **Slip-Eligible Pool**
- The **Stat-Mix Gate** fails when any one stat exceeds 70% of the **Slip-Eligible Pool**, subject to later tuning
- The **Confidence Gate** evaluates both probability and EV extremes in the **Slip-Eligible Pool**
- The initial probability extreme for the **Confidence Gate** is any leg with probability at or above 0.80 or at or below 0.20, subject to later calibration from historical data
- The initial EV extreme for the **Confidence Gate** is any leg with EV at or above 0.35, subject to later calibration from historical data
- A **Proof Run** should emit an explicit **Proof-Run Verdict**
- A failed **Proof-Run Verdict** should include one or more structured **Proof-Run Failure Reason** values
- Initial **Proof-Run Failure Reason** values should be broad buckets with supporting evidence, not fully granular threshold-specific schemas
- Partial stat failures remain acceptable in a **Proof Run** unless they prevent a non-empty **Slip-Eligible Pool** and at least one valid **Slip Artifact**
- A **No-Play Slate** is distinct from a failed **Proof Run**
- A **No-Play Slate** still counts as a successful **Proof-Run Verdict** when runtime and sanity gates pass
- A **No-Play Slate** should appear as its own explicit outcome in proof-run summaries
- **Proof Run** hardening and **Freshness Run** hardening should be planned separately
- **Proof Run** hardening should extend the existing MLB live shadow CLI rather than introduce a second command
- **Proof-Run Verdict** and proof-run sanity gates should run by default in the existing MLB live shadow CLI
- Every **Proof-Run Verdict** should emit **Proof-Run Evidence** including outcome, failure reasons when present, stat completion/failure summaries, slip-eligible pool size, stat mix, probability extremes, EV extreme, and slip counts
- MLB live shadow runs should default to `mlb.live_underdog.stat_ids` from config, with CLI `--stat-id` values acting as overrides
- Missing required config-backed MLB live stat ids should hard-fail the **Proof-Run Verdict** before runtime scoring begins
- Required MLB live stat-id coverage for a **Proof Run** means all currently supported MLB pitcher-prop markets, not a partial subset
- The MLB live shadow CLI should support explicit **Proof Run** and **Debug Run** modes
- A **Debug Run** may use subset stat-id overrides without claiming full proof-run validity
- **Proof Run** and **Debug Run** should share one summary schema with an explicit mode field

## Example dialogue

> **Dev:** "Does this MLB flow submit slips?"
> **Domain expert:** "No. A **Shadow Run** only creates **Slip Artifacts** for **Manual Submission**."

> **Dev:** "Can I bet after a **Proof Run**?"
> **Domain expert:** "Not yet. A **Proof Run** verifies the workflow; a **Freshness Run** is the bar for recommendation-quality output."

> **Dev:** "The run wrote JSON slips. Did it pass?"
> **Domain expert:** "Not if a **Sanity Check Failure** shows the candidate pool is implausible."

> **Dev:** "What makes a proof run fail even with artifacts?"
> **Domain expert:** "A **Stat-Mix Gate** or **Confidence Gate** can still make the run unsuccessful."

> **Dev:** "Do those checks look at every scored row?"
> **Domain expert:** "No. They inspect the **Slip-Eligible Pool**, because that is what manual submission is based on."

> **Dev:** "What counts as stat dominance for now?"
> **Domain expert:** "If one stat exceeds 70% of the **Slip-Eligible Pool**, the **Stat-Mix Gate** fails."

> **Dev:** "Does the confidence gate look at just probabilities?"
> **Domain expert:** "No. The **Confidence Gate** uses both probability and EV extremes."

> **Dev:** "Are the probability thresholds final?"
> **Domain expert:** "No. They are a sane starting point for the **Confidence Gate** until historical calibration tightens them."

> **Dev:** "What about EV extremes?"
> **Domain expert:** "Treat any slip-eligible leg with EV >= 0.35 as a first-pass **Confidence Gate** failure until historical data supports a better threshold."

> **Dev:** "How do I know the proof run passed?"
> **Domain expert:** "The workflow should emit an explicit **Proof-Run Verdict**, not force manual interpretation."

> **Dev:** "If it fails, how specific should the output be?"
> **Domain expert:** "A failed **Proof-Run Verdict** should include structured **Proof-Run Failure Reason** values so the next fix is obvious."

> **Dev:** "How detailed should those reasons be at first?"
> **Domain expert:** "Use broad **Proof-Run Failure Reason** buckets first, then attach compact evidence."

> **Dev:** "When does a degraded run become a runtime failure?"
> **Domain expert:** "Only when partial failures prevent a non-empty **Slip-Eligible Pool** and at least one valid **Slip Artifact**."

> **Dev:** "What if there are scored candidates but the right move is to bet nothing?"
> **Domain expert:** "That is a **No-Play Slate**, not a failed proof run."

> **Dev:** "Does a no-play slate fail the proof run?"
> **Domain expert:** "No. A **No-Play Slate** still passes the **Proof-Run Verdict** if the workflow and sanity gates are healthy."

> **Dev:** "Should no-play look the same as an ordinary pass?"
> **Domain expert:** "No. A **No-Play Slate** should be explicit in the summary so healthy abstention is distinguishable from accidental emptiness."

> **Dev:** "Should proof-run hardening and freshness work live in one plan?"
> **Domain expert:** "No. **Proof Run** hardening and **Freshness Run** hardening should be separate plans."

> **Dev:** "Should proof-run hardening add a new command?"
> **Domain expert:** "No. It should extend the existing MLB live shadow CLI."

> **Dev:** "Should proof-run checks be opt-in?"
> **Domain expert:** "No. The existing MLB live shadow CLI should run the **Proof-Run Verdict** and sanity gates by default."

> **Dev:** "What should the proof-run summary show?"
> **Domain expert:** "It should emit compact **Proof-Run Evidence** so the operator can see pass, fail, or no-play without opening raw JSON."

> **Dev:** "Where should live Underdog stat ids come from by default?"
> **Domain expert:** "From config. CLI `--stat-id` values should only override config for one-off runs."

> **Dev:** "What if config is missing one of the live stat ids?"
> **Domain expert:** "That should hard-fail the **Proof-Run Verdict** before the run starts."

> **Dev:** "How many MLB live markets are required for a proof run?"
> **Domain expert:** "All currently supported MLB pitcher-prop markets."

> **Dev:** "Why keep CLI stat-id overrides if proof runs need full coverage?"
> **Domain expert:** "Because subset overrides belong to a **Debug Run**, not a **Proof Run**."

> **Dev:** "Should proof and debug runs write different summary formats?"
> **Domain expert:** "No. They should share one summary schema with an explicit mode field."

## Flagged ambiguities

- "submit UD betslips" was ambiguous between **Manual Submission** and automated placement — resolved: this repo only supports **Manual Submission**.
- "prove the model" was ambiguous between workflow validation and recommendation readiness — resolved: **Proof Run** validates workflow, **Freshness Run** gates recommendation-quality use.
- "successful proof run" was ambiguous between artifact creation and trustworthy output — resolved: **Sanity Check Failure** means the **Proof Run** failed even if artifacts were produced.
- "additional checks on correctness" was vague — resolved: the first hard proof-run correctness checks are the **Stat-Mix Gate** and **Confidence Gate**.
- "candidate pool" was ambiguous between all scored rows and actionable rows — resolved: proof-run sanity checks inspect the **Slip-Eligible Pool**.
- "too much stat dominance" was unresolved — resolved: the initial **Stat-Mix Gate** threshold is 70% of the **Slip-Eligible Pool** for any one stat.
- "confidence extremes" was underspecified — resolved: the **Confidence Gate** inspects both probability and EV in the **Slip-Eligible Pool**.
- "extreme probability" had no threshold — resolved: the initial **Confidence Gate** probability bounds are `prob <= 0.20` or `prob >= 0.80`, pending historical calibration.
- "extreme EV" had no threshold — resolved: the initial **Confidence Gate** EV bound is `ev >= 0.35`, pending historical calibration.
- "did the proof run pass?" was too implicit — resolved: the workflow should emit an explicit **Proof-Run Verdict**.
- "failed proof run" was too coarse — resolved: failed proof runs should include structured **Proof-Run Failure Reason** values.
- "how detailed should failure reasons be?" was unresolved — resolved: initial **Proof-Run Failure Reason** values should be broad buckets with compact evidence.
- "when do partial failures become blocking?" was unresolved — resolved: a degraded proof run is acceptable unless it cannot produce a non-empty **Slip-Eligible Pool** and at least one valid **Slip Artifact**.
- "no valid slips" was overloaded between broken workflow and healthy abstention — resolved: a healthy abstention outcome is a **No-Play Slate**, not a proof-run failure.
- "does no action mean failure?" was unresolved — resolved: a healthy **No-Play Slate** is still a successful **Proof-Run Verdict**.
- "how should abstention appear in output?" was unresolved — resolved: **No-Play Slate** should be an explicit summary outcome, not an implied zero-slip pass.
- "should proof and freshness share one plan?" was unresolved — resolved: **Proof Run** hardening and **Freshness Run** hardening should be planned separately.
- "should proof-run hardening use a new command?" was unresolved — resolved: it should extend the existing MLB live shadow CLI.
- "should proof-run checks be opt-in?" was unresolved — resolved: proof-run verdicts and sanity gates should run by default in the existing MLB live shadow CLI.
- "what should the proof-run summary contain?" was unresolved — resolved: every verdict should emit compact **Proof-Run Evidence** with outcome, reasons, completion stats, pool shape, confidence extremes, and slip counts.
- "where should live stat ids come from?" was unresolved — resolved: MLB live shadow runs should default to config-backed stat ids, with CLI values used only as overrides.
- "are missing config stat ids acceptable degradation?" was unresolved — resolved: missing required config-backed MLB live stat ids should hard-fail the **Proof-Run Verdict** before scoring begins.
- "what counts as required live coverage?" was unresolved — resolved: a **Proof Run** requires stat-id coverage for all currently supported MLB pitcher-prop markets.
- "why keep subset stat-id overrides?" was unresolved — resolved: subset overrides are for **Debug Run** mode, while **Proof Run** keeps the full-market coverage contract.
- "should proof and debug summaries diverge?" was unresolved — resolved: they should share one summary schema with an explicit mode field.
