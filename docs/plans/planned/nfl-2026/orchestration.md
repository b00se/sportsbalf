# Codex Orchestration Runbook

## Roles

- Foreman: GPT-5.6 Terra, medium. Raise to high only for model-selection or integration disputes.
- Implementer: GPT-5.6 Luna, medium; high for leakage, probability, or optimizer work.
- Reviewer: a different GPT-5.6 Luna agent, high.
- Phase audit: Terra high. Sol high only after two failed Luna/Terra diagnosis loops.

## Loop

1. Verify Git status, dependencies, and evidence.
2. Assign at most two ready packets with disjoint file ownership.
3. Implementers confirm behavioral tests, implement, and loop on focused failures.
4. Run affected tests, lint changed files, and git diff --check.
5. Commit one task and return objective evidence.
6. A different agent reviews the commit.
7. Return findings to the implementer; repair and rerun gates.
8. Integrate reviewed commits, update evidence, and run the wave gate.
9. Stop. Start the next wave in a fresh task.

## Usage discipline

- Prefer 60-90 minute packets.
- Do not start a packet late in a five-hour window.
- Use focused tests during repairs and the full suite at wave gates.
- Stop after two materially similar failed repairs and launch one diagnosis packet.
- Never relax gates because usage is low.

## Goal versus loop

Use /goal for one foreman wave. The wave has a durable terminal outcome and may need continuations. Do not set a token budget unless the user explicitly chooses one.

Do not use /loop for implementation waves. Recurrence suits scheduled monitoring; these waves already contain bounded test/review repair loops. A recurring loop can consume allowance after reaching a human or dependency gate.

## Human gates

Stop for the controlled Autopilot proof, new credentials or terms, unauthorized destructive Git work, material scope changes, and any upload, entry, or transaction.
