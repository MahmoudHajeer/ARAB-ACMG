# Project Workflow: ARAB-ACMG Research

## Guiding Principles

1. **The Plan is the Source of Truth:** All work must be tracked in `plan.md` (within each track).
2. **The Tech Stack is Deliberate:** Changes to the tech stack must be documented in `tech-stack.md` *before* implementation.
3. **Reproducibility First:** All scripts and data analysis steps must be reproducible.
4. **Test-Driven Development (TDD):** Write tests for data transformation and analysis logic.
5. **High Code Coverage:** Aim for >100% code coverage for all Python scripts.
6. **Scientific Rigor:** Ensure all results are validated against known benchmarks or clinical evidence.
7. **Non-Interactive & CI-Aware:** Prefer non-interactive commands. Use `CI=true` for tests and linters.

## Task Workflow

All tasks follow a strict lifecycle:

### Standard Task Workflow

1. **Select Task:** Choose the next available task from the track's `plan.md` in sequential order.

2. **Mark In Progress:** Before beginning work, edit `plan.md` and change the task from `[ ]` to `[~]`.

3. **Write Failing Tests (Red Phase):**
   - Create a test file for the analysis function or data processing step.
   - Write tests that define the expected output for given input data.
   - Run the tests and confirm they fail.

4. **Implement to Pass Tests (Green Phase):**
   - Write the minimum Python code necessary to make the tests pass.
   - **Architectural Style**: Follow a minimalist, pipeline-like structure to make data flow obvious and easy to track.
   - **AI-Signed Comments**: Each significant step must include an AI-signed comment (`# [AI-Agent]: ...`) explaining its purpose and its effect on the data.
   - **Modern Python**: Use Python 3.14+ features whenever possible. Explicitly document these features and their effects in the comments.
   - Run the test suite and confirm all tests pass.

5. **Refactor (Optional but Recommended):**
   - Refactor for clarity and efficiency.
   - Rerun tests to ensure no regressions.

6. **Verify Coverage:** Use `pytest-cov` to ensure the new code is adequately tested.
   ```bash
   pytest --cov=src --cov-report=term-missing
   ```

7. **Document Deviations:** If implementation differs from `tech-stack.md`, update it before proceeding.

8. **Commit Code Changes:**
   - Stage changes.
   - Commit with a message like `feat(analysis): Implement ACMG rule PS1 check`.

9. **Attach Task Summary with Git Notes:**
   - **Step 9.1: Get Commit Hash:** `git log -1 --format="%H"`.
   - **Step 9.2: Draft Note Content:** Summary of the task, changes, and findings.
   - **Step 9.3: Attach Note:** `git notes add -m "<note content>" <commit_hash>`.

10. **Record Task Commit SHA in Plan:**
    - Update `plan.md`, change task to `[x]`, and append the 7-character commit SHA.

11. **Commit Plan Update:** `git commit -m "conductor(plan): Mark task 'PS1 rule implementation' as complete"`.

## Testing Requirements

### Unit Testing
- Every analysis script must have corresponding unit tests.
- Use synthetic or mock data for testing edge cases.
- Test for success (correct classification) and failure (incorrect data format).

### Validation
- Validate Arab-specific frequency filtering against public databases like gnomAD or Greater Middle East (GME) Variome.

## Commit Guidelines

### Message Format
- `feat`: New analysis feature or rule implementation.
- `fix`: Bug fix in analysis scripts.
- `docs`: Documentation updates.
- `data`: Updates to datasets or metadata.
- `refactor`: Analysis code cleanup.
- `test`: Adding or improving tests.
- `chore`: Maintenance tasks.

## Definition of Done

A task is complete when:
1. Analysis logic is implemented.
2. Unit tests are written and passing.
3. Code coverage meets requirements (>80%).
4. Research notes or findings are documented.
5. All linting checks pass.
6. Changes committed and task summary attached as a git note.
