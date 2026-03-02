# Codex Agent Instructions: ARAB-ACMG

This repository uses the Conductor framework under `conductor/` as persistent project state shared across AI agents (Codex and Gemini). Treat Conductor files as the only source of truth for continuity.

## Required Context Before Any Task
Read these files in order:
1. `conductor/index.md`
2. `conductor/workflow.md`
3. `conductor/tracks.md`
4. `conductor/setup_state.json`
5. Active track files:
   - `conductor/tracks/<TRACK>/spec.md`
   - `conductor/tracks/<TRACK>/plan.md`
   - `conductor/tracks/<TRACK>/index.md`

## Task Selection Rules
1. In `conductor/tracks.md`, pick the first track with status `[~]`.
2. If no track is `[~]`, pick the first track with status `[ ]` in sequence.
3. In that track `plan.md`, pick the first task marked `[~]`.
4. If none is `[~]`, pick the first task marked `[ ]`.
5. Do not skip earlier open tasks unless the plan explicitly allows parallel work.

## State Update Protocol
Before implementation:
1. Mark selected task as `[~]` in the track `plan.md`.
2. Ensure the track status in `conductor/tracks.md` is `[~]`.
3. Add a "Started" handoff entry in the track `index.md`.

After implementation:
1. Run relevant tests/verification commands.
2. Commit code changes using a conventional commit message.
3. Attach a git note summary for the commit.
4. Mark task as `[x]` in `plan.md` and append short SHA.
5. Update track status in `conductor/tracks.md`:
   - `[x]` only when all tasks are complete.
   - otherwise `[~]`.
6. Update `conductor/setup_state.json` and keep the key `last_successful_step` for compatibility with Gemini.
7. Add a "Completed" (or "Blocked") handoff entry in the track `index.md`.

## Handoff Entry Format (Track index.md)
Use this exact field set so another agent can resume immediately:
- Timestamp (ISO 8601 with timezone)
- Agent (`Codex` or `Gemini`)
- Task ID (for example `3.2`)
- Status (`Started`, `Completed`, `Blocked`)
- Summary (1-2 lines)
- Files changed
- Verification run + result
- Next exact action

## Collaboration Constraints
- Never depend on conversation memory; always re-read Conductor state files.
- Never rewrite or delete previous handoff entries; append only.
- Prefer small, traceable edits to Conductor files.
- If state files conflict, resolve conflict explicitly in `index.md` with a "Blocked" entry and proposed fix.
