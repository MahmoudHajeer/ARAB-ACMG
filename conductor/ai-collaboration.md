# AI Collaboration Protocol (Gemini + Codex)

This document defines how both agents coordinate work without losing progress.

## Canonical State Files
- `conductor/tracks.md`: Which track is active.
- `conductor/tracks/<TRACK>/plan.md`: Task-level progress (`[ ]`, `[~]`, `[x]`).
- `conductor/setup_state.json`: Latest successful checkpoint (must keep key `last_successful_step`).
- `conductor/tracks/<TRACK>/index.md`: Append-only handoff journal.

## Execution Order
1. Select active track from `conductor/tracks.md` (`[~]` first, otherwise first `[ ]`).
2. Select active task from track `plan.md` (`[~]` first, otherwise first `[ ]`).
3. Mark task `[~]` before writing code.
4. Implement with tests based on `conductor/workflow.md`.
5. Mark task `[x]` after verification and commit.
6. Record checkpoint and next action for handoff.

## Required Handoff Journal Entry
Append each entry in `conductor/tracks/<TRACK>/index.md` with:
- `timestamp`: ISO 8601 timezone-aware value.
- `agent`: `Gemini` or `Codex`.
- `task`: plan task ID (example: `2.3`).
- `status`: `Started` | `Completed` | `Blocked`.
- `summary`: concise description of work performed.
- `files`: changed files (or `none`).
- `verification`: commands run and pass/fail result.
- `next_action`: exact next step another agent should run.

## Resume Rule
When a new session starts, ignore chat history and resume only from:
1. Latest `Completed` or `Blocked` handoff entry in track `index.md`.
2. Current checkbox state in track `plan.md`.
3. `last_successful_step` in `conductor/setup_state.json`.
