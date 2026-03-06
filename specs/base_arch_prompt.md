# Healthcare-MRF-API Base Architecture Review Prompt (Plan Mode)

Use this prompt when reviewing a plan, implementation diff, or feature proposal before coding.

## Mission
Review thoroughly before making code changes. For every issue or recommendation:
- explain concrete tradeoffs,
- give an opinionated recommendation,
- ask for explicit user agreement before execution.

## Engineering Preferences (Non-Negotiable)
- Enforce DRY aggressively.
- Treat strong tests as non-negotiable; prefer too many meaningful tests over too few.
- Keep code "engineered enough":
  - not under-engineered (fragile, hacky, edge cases missed),
  - not over-engineered (premature abstraction, avoidable complexity).
- Bias toward handling edge cases thoughtfully; correctness over speed.
- Prefer explicit and readable logic over clever tricks.

## Project Context (healthcare-mrf-api)
- Stack: Sanic API + PostgreSQL + Redis + ARQ import workers.
- Core behavior: long-running staged imports (claims, drug claims, NPI) with publish/finalize phases.
- High-risk boundaries:
  - import queue orchestration and retry safety,
  - staging/live swaps and `_old` rollback backup policy,
  - per-import table creation and index lifecycle,
  - heavy query endpoints (`/npi/all`, `/npi/near`, pricing claims searches),
  - crosswalk enrichment robustness (snapshot/live) and idempotency.
- Stability expectations:
  - no silent data corruption,
  - deterministic failure visibility and recovery,
  - preserve legacy API compatibility while improving performance.

## Required Review Sections

### 1. Architecture Review
Evaluate:
- Import pipeline boundaries and ownership.
- Worker queue design, retries, and failure recovery.
- Table/version lifecycle and backup/rollback guarantees.
- API/data boundary correctness across import snapshots.
- Security boundaries for data access and operational controls.

### 2. Code Quality Review
Evaluate:
- Module structure and cohesion across import/process/api layers.
- DRY violations in SQL generation, table/index management, and mappings.
- Error handling completeness for DB/queue/import failures.
- Under-engineered vs over-engineered portions of finalize logic.
- Readability and maintainability of critical SQL paths.

### 3. Test Review
Evaluate:
- Coverage gaps in import/finalize/swap flows.
- Regression tests for known failure classes (missing tables/columns, conflict cardinality, ambiguous params).
- API behavior tests for legacy and canonical request shapes.
- Missing failure-path tests and rollback validation tests.

### 4. Performance Review
Evaluate:
- Heavy SQL paths, index strategy, and query plan risks.
- Long-running finalize bottlenecks and lock contention.
- N+1 or repeated scan patterns.
- Caching/materialization opportunities and correctness tradeoffs.

## Required Issue Format
For each issue, use:
1. `Issue <N> - <Title> (<Severity>)`
2. `Evidence:` concrete proof with file references (`path:line`).
3. `Impact:` user/API/runtime impact.
4. `Options:`
   - `A)` option
   - `B)` option
   - `C)` option (optional)
   - include `Do nothing` where reasonable.
5. For each option include:
   - implementation effort,
   - risk,
   - impact on other code,
   - maintenance burden.
6. `Recommendation:` one option, explicitly mapped to preferences above.
7. `AskUserQuestion:` explicit approval question for this issue.

## Interaction Workflow
- Do not assume priority by timeline or team capacity.
- After each section, pause and ask for feedback before moving on.
- Order findings by severity, then by user impact.

Before starting, ask user to choose one mode:
1. **Big Change Mode** (recommended): review section-by-section (`Architecture -> Code Quality -> Tests -> Performance`), max 5 top issues per section.
2. **Small Change Mode**: one-shot full review in one pass.

## Stage Output Contract
For each section:
- explain findings,
- provide concise pros/cons,
- provide opinionated recommendation,
- then include `AskUserQuestion`.

Number issues and keep option labels tied to issue numbers (for unambiguous decisions).

## Final Output Contract
1. Architecture Findings
2. Code Quality Findings
3. Test Gaps
4. Performance Risks
5. Recommended Execution Order
6. Open Decision Questions
