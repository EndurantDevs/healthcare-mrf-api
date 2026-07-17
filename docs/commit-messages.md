# Commit Message Style

Use commit messages that are easy to scan in `git log`, GitHub history, and
deployment rollbacks. We follow a small Conventional Commits-style subject line
without tying it to release automation.

## Required Subject Format

```text
type(scope): imperative summary
type: imperative summary
type(scope)!: breaking-change summary
```

Rules:

- Keep the first line at 100 characters or less; aim for 72 or less.
- Use an imperative, action-oriented summary: `fix`, `add`, `split`, `remove`,
  `prefer`, `block`.
- Do not end the subject with a period.
- Use the body only when the reason, tradeoff, rollback note, or test evidence
  is not obvious from the diff.
- Keep one logical change per commit when practical.

Allowed types:

- `feat` - user-visible capability
- `fix` - bug or broken behavior
- `refactor` - structure/readability change with no intended behavior change
- `perf` - performance improvement
- `docs` - documentation only
- `test` - tests only or test infrastructure
- `ci` - CI checks and GitHub Actions
- `build` - dependencies, packaging, images, build tooling
- `deploy` - deployment manifests or release plumbing
- `ops` - operational scripts, runbooks, maintenance behavior
- `chore` - narrow housekeeping when no better type fits
- `revert` - explicit rollback

Do not invent adjacent types such as `quality`; use
`refactor(readability): ...` for readability-budget reductions.

Scopes are optional. When used, keep them lowercase and domain-focused, for
example `api`, `ptg`, `routing`, `readability`, `imports`, or `postgres`.

## Examples

Good:

```text
fix(routing): prefer loaded PTG snapshot
feat(imports): add ASR seed-list discovery
refactor(readability): split commit policy checks
ci(commit): gate unclear commit subjects
docs: explain importer smoke workflow
```

Bad:

```text
fix
update stuff
misc changes
fix(API): Handle Timeout.
```

Automation exceptions are allowed for GitHub merge commits, GitHub generated
`Revert "..."` commits, and Dependabot `Bump ...` commits.

## Local Check

```bash
python3 scripts/check_commit_messages.py --message "fix(api): handle upstream timeout"
python3 scripts/check_commit_messages.py --last 3
```

This policy is based on the practical parts of Conventional Commits, common Git
history readability guidance, and the rule that the subject should tell a human
what changed before they open the diff.
