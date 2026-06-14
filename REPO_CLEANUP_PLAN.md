# healthcare-mrf-api — Repository Hygiene & Maintainability Plan

Audience: one engineer, working through tickets sequentially.
Sizing: every task is scoped to ~5 minutes of focused work. Where a single fix is bigger, it has been split.
Convention: each task has **What** (the action) and **Why** (the reason). Bash commands assume the repo root.

Do the sections in order. Section 1 is safe and high-leverage. Section 5+ touches production code and should be reviewed.

---

## Section 1 — Stop tracking junk (do first, lowest risk, highest payoff)

### 1.1 Baseline what is currently tracked

**What:** Run

    git ls-files | xargs -I{} du -k "{}" 2>/dev/null | awk '{s+=$1} END {print s/1024 " MB tracked"}'
    git ls-files | xargs -I{} du -k "{}" 2>/dev/null | sort -rn | head -30

**Why:** Establishes a baseline before deletions so we can verify the repo shrinks as expected. Confirms ~1 GB of tracked content that should not be there (`venv314/`, `restore/venv/`, `support/ptg2_scanner/target/`).

### 1.2 Confirm working tree is clean before mass edits

**What:** `git status` and stash or branch off any in-flight work first:

    git switch -c chore/repo-cleanup

**Why:** `git status` currently shows large uncommitted edits across `ptg.py`, `ptg2_serving.py`, models, and tests. Cleanup must not be entangled with that diff.

### 1.3 Add venv directories to `.gitignore`

**What:** Append to `.gitignore`:

    # Local virtual environments (any name)
    venv*/
    .venv*/
    restore/venv/

**Why:** The current `.gitignore` lists generic Python build dirs but not the named venvs that actually exist in the repo (`venv314/`, `restore/venv/`). Without this rule they will be re-added by anyone who runs `git add .`.

### 1.4 Untrack `venv314/`

**What:**

    git rm -r --cached venv314/

**Why:** 315 MB of site-packages live under `venv314/`. Untracking keeps the directory on disk for the current developer but removes it from the repo going forward.

### 1.5 Untrack `restore/venv/`

**What:**

    git rm -r --cached restore/venv/

**Why:** `restore/` is documented as frozen legacy snapshots, but freezing source ≠ freezing 200 MB of wheels from Python 3.12. Keep the source tree under `restore/`, drop the venv.

### 1.6 Add Rust build artifacts to `.gitignore`

**What:** Append to `.gitignore`:

    # Rust build artifacts
    support/ptg2_scanner/target/

**Why:** `target/` is 418 MB of compiled artifacts. Only `Cargo.toml`, `Cargo.lock`, and `src/` should be tracked.

### 1.7 Untrack `support/ptg2_scanner/target/`

**What:**

    git rm -r --cached support/ptg2_scanner/target/

**Why:** Same reason as 1.5 — these are build outputs, not source.

### 1.8 Remove stale pg_upgrade logs

**What:**

    git rm pg_upgrade_server.log pg_upgrade_utility.log pg_upgrade_internal.log

Add to `.gitignore`:

    pg_upgrade_*.log

**Why:** Three log files from November 2022 sitting at the repo root, unrelated to current operation. They are noise in code search and `git grep`.

### 1.9 Remove the stray `=0.4.2` file

**What:**

    ls -la "=0.4.2"   # confirm it exists and is small
    rm "=0.4.2"

**Why:** This is the file `pip install foo=0.4.2` produces when someone typos `==` as `=` — the shell redirects-ish behavior leaves an empty file named literally `=0.4.2`. It is currently untracked, so just delete from disk.

### 1.10 Add macOS junk to `.gitignore`

**What:** Append:

    .DS_Store
    **/.DS_Store

Then:

    git rm --cached -f $(git ls-files | grep -F .DS_Store) 2>/dev/null || true

**Why:** `.DS_Store` files appear under `support/`, `reports/`, and the repo root. macOS-only metadata, shouldn't be in a server repo.

### 1.11 Commit the cleanup so far

**What:**

    git add .gitignore
    git status   # eyeball what's staged
    git commit -m "chore: stop tracking venvs, build artifacts, and stale logs"

**Why:** Snapshot the safe cleanup before the next decisions, which need product judgment.

---

## Section 2 — Decide what `reports/` is for

### 2.1 List `reports/` contents and triage

**What:**

    git status reports/
    ls -la reports/

Open the README for each subdirectory (if any) and list every artifact in a scratch note: Deloitte demo PDF, market pharmacy JSONs, quality example CSVs, etc.

**Why:** These appear to be one-off deliverables (sales/demo material). Before moving or deleting, an engineer needs to know what the repo owner actually wants to keep.

### 2.2 Open a decision ticket: where does `reports/` belong?

**What:** File a separate ticket (not a code change) with three options for the maintainer to pick:

  a. Keep under `reports/` but `.gitignore` it and store deliverables elsewhere (Drive, S3).
  b. Move to a sibling repo `healthporta-reports`.
  c. Keep tracked but lock down to source-of-truth markdown + small CSVs, no PDFs/JSON dumps.

**Why:** A binary "keep or delete" call shouldn't be made by an engineer alone. Force the question, get an answer, then act.

### 2.3 After decision: implement the policy

**What:** Depending on the answer in 2.2, either:

  - add `reports/` to `.gitignore` and `git rm -r --cached reports/`, or
  - move large binaries out of the repo and commit a `reports/README.md` describing where they now live.

**Why:** Currently `reports/` contains untracked PDFs, TSVs, and JSON. Either commit a policy or remove them.

---

## Section 3 — Reconcile dual test directories

### 3.1 Inventory both test directories

**What:**

    ls __test__/
    ls tests/ | wc -l   # currently 55
    grep -r "^def test_" __test__/ | wc -l

**Why:** `__test__/` is in `.gitignore` but has files (`test_npi_import.py`, `test_npi_section_guard.py`). `tests/` is the real suite per `pytest.ini` (`testpaths = tests`). Confirm which directory actually holds active tests before any move.

### 3.2 Move any unique tests from `__test__/` into `tests/`

**What:** For each file under `__test__/`, check if the test name exists under `tests/`:

    for f in __test__/test_*.py; do
      base=$(basename "$f")
      [ -f "tests/$base" ] || echo "UNIQUE: $f"
    done

Move uniques into `tests/` and run them: `pytest tests/<file> -x`.

**Why:** Don't lose coverage. Two of the unique files (`test_npi_import.py`, `test_npi_section_guard.py`) sound load-bearing.

### 3.3 Delete `__test__/` and remove from `.gitignore`

**What:**

    rm -rf __test__/
    # remove the `__test__/` line from .gitignore

**Why:** Single test directory, single source of truth. Eliminates the README/pytest.ini contradiction.

### 3.4 Update contributor docs to reference `tests/`

**What:** In contributor docs, reference `pytest tests -x` (or just `pytest -x`, since `pytest.ini` already sets `testpaths = tests`).

**Why:** Anyone following stale local notes today is pointed at the wrong directory.

### 3.5 Commit the test-dir reconciliation

**What:** `git add -A && git commit -m "test: consolidate on tests/ as the single test directory"`

**Why:** Discrete, reviewable change.

---

## Section 4 — Resolve uncommitted work on `main`

### 4.1 Audit the current uncommitted diff

**What:**

    git diff --stat HEAD
    git diff HEAD -- api/endpoint/pricing.py | head -100

**Why:** ~1,400 lines uncommitted across `ptg.py`, `ptg2_serving.py`, `db/models.py`, `support/ptg2_scanner/src/main.rs`, and tests. Need to know if this is a feature branch that lost its branch name, or genuinely abandoned work.

### 4.2 Decide: ship, branch, or revert

**What:** Talk to the author of the local edits. Outcome must be one of:

  a. Land it: create a feature branch `feat/ptg2-improvements`, push, open PR.
  b. Park it: stash with a descriptive message, `git stash push -m "ptg2 wip <date>"`.
  c. Drop it: `git checkout -- .` after confirming nothing valuable is lost.

**Why:** Uncommitted changes on `main` are a footgun for every subsequent operation in this plan.

### 4.3 Re-check `git status` is clean

**What:** `git status` returns "working tree clean".

**Why:** Required precondition for migrations / refactor work in Section 6+.

---

## Section 5 — Make Alembic-vs-bootstrap explicit

### 5.1 Add a "Schema sources of truth" section to README

**What:** Under `## Local Development` in `README.md`, add a short subsection:

> **Schema sources of truth.** Tables for issuers, plans, NPI core, and reference data are managed by Alembic (`alembic/versions/`). Canonical tables for claims-pricing, drug-claims, and NPI import outputs are bootstrapped per-import with `checkfirst` create + post-load indexing (see `process/claims_pricing.py`, `process/drug_claims.py`, `process/npi.py`). Run `python main.py manage sync-structure` to reconcile drift.

**Why:** Today a new contributor reading `alembic/versions/` will assume migrations cover the whole schema. They don't. The lack of clarity is also why `manage sync-structure` exists.

### 5.2 Cross-link from contributor docs

**What:** In contributor docs, add a one-line pointer:

> See README "Schema sources of truth" for the alembic vs. per-import bootstrap split.

**Why:** Same audience, different file. Don't make the reader hunt.

### 5.3 Add a docstring to `manage sync-structure`

**What:** In `main.py`, expand the docstring of `sync_structure` from "Ensure database tables, columns, and indexes match the SQLAlchemy models." to mention that it exists precisely because per-import tables bypass Alembic.

**Why:** Future maintainers will understand the command's purpose without needing to read the spec.

---

## Section 6 — Tighten the pylint config (cheap quality win)

### 6.1 Re-enable `too-many-lines` with a generous threshold

**What:** In `.pylintrc`:

  - remove `too-many-lines` from `disable=`
  - under `[FORMAT]`, add `max-module-lines=2000`

**Why:** `db/models.py` (3,915 lines) and `process/ptg.py` (11,060 lines) and `process/provider_quality.py` (4,856) get a pass today because the rule is off. A 2,000-line cap will not break existing files immediately if we add per-file ignores, but it stops *new* files from going down the same road.

### 6.2 Add file-level `# pylint: disable=too-many-lines` to existing offenders

**What:** Add the disable comment at the top of:

  - `db/models.py`
  - `process/ptg.py`
  - `process/provider_quality.py`
  - `process/claims_pricing.py`
  - `process/drug_claims.py`
  - `process/pharmacy_license.py`
  - `process/initial.py`
  - `process/entity_address_unified.py`
  - `process/partd_formulary_network.py`
  - `process/provider_enrichment.py`

**Why:** Lets us turn on the global rule without a massive cleanup blocking it. Each disabled file becomes its own future refactor ticket.

### 6.3 Run pylint and capture the new baseline

**What:**

    pylint --rcfile=.pylintrc db/ api/ process/ 2>&1 | tail -20

**Why:** Confirms no regression from re-enabling the rule and gives us a number to track over time.

### 6.4 Commit the pylint changes

**What:** `git commit -am "lint: re-enable too-many-lines with a 2000-line cap"`

**Why:** Small, isolated, reviewable.

---

## Section 7 — Split `db/models.py` (incremental refactor; do AFTER Section 6 lands)

This is the biggest item; it is split into many small steps because moving 3,915 lines of SQLAlchemy is high-risk for import order and test runs.

### 7.1 Inventory the classes in `db/models.py`

**What:**

    grep -c "^class " db/models.py   # currently 137
    grep -n "^class " db/models.py > /tmp/models_index.txt

**Why:** You can't plan a split without a class list. 137 classes is the number that justifies splitting.

### 7.2 Group classes by domain on paper

**What:** From `models_index.txt`, assign each class to one of these buckets:

  - `db/models/issuer_plan.py` (Issuer, PlanFormulary, plan_* tables)
  - `db/models/pricing.py` (pricing_*, claims pricing)
  - `db/models/prescription.py` (pricing_prescription, drug_claims)
  - `db/models/provider.py` (npi, npi_address, provider_enrichment)
  - `db/models/partd.py` (partd_*)
  - `db/models/quality.py` (provider_quality_*)
  - `db/models/geo.py` (geo_*, places_zcta, lodes)
  - `db/models/pharmacy.py` (pharmacy_license, pharmacy_economics)
  - `db/models/codes.py` (code_catalog, code_crosswalk, nucc)
  - `db/models/system.py` (ImportHistory, ImportLog, run/snapshot tables)

**Why:** Decide grouping before moving any code. Avoids re-shuffling mid-refactor.

### 7.3 Create the package skeleton

**What:**

    mkdir -p db/models
    git mv db/models.py db/models/_legacy.py
    touch db/models/__init__.py
    # In __init__.py, add:  from db.models._legacy import *  # noqa

**Why:** Backwards compatibility. Every other file in the repo imports `from db.models import …`. Re-exporting from `_legacy.py` keeps everything working while we move classes one bucket at a time.

### 7.4 Move one bucket: `system.py` (smallest, lowest risk)

**What:**

  1. Create `db/models/system.py`.
  2. Move `ImportHistory`, `ImportLog`, `PartDImportRun`, `PartDFormularySnapshot` classes into it.
  3. In `db/models/__init__.py`, add `from db.models.system import *`.
  4. Delete the moved classes from `_legacy.py`.
  5. Run `pytest tests -x -q` — expect no failures.

**Why:** Smallest, lowest-coupling group. Validates the pattern before we touch the gnarlier ones.

### 7.5 Move bucket: `codes.py`

**What:** Same procedure as 7.4 for `code_catalog`, `code_crosswalk`, NUCC tables. Run tests.

**Why:** Still low-risk; mostly reference tables.

### 7.6 Move bucket: `geo.py`

**What:** Same procedure for geo_*, places_zcta, lodes. Run tests.

**Why:** Pure lookup tables; no FK entanglement with the heavy hitters.

### 7.7 — 7.13 Continue one bucket per ticket

**What:** Repeat for `issuer_plan.py`, `pricing.py`, `prescription.py`, `provider.py`, `partd.py`, `quality.py`, `pharmacy.py`. One bucket per ticket. Run tests after each.

**Why:** Each move is ~5–15 min and individually revertable. A single 137-class move would be unreviewable.

### 7.14 Delete `_legacy.py` when empty

**What:** Once every class has moved, confirm `_legacy.py` is empty, then `git rm db/models/_legacy.py` and remove the `from _legacy import *` line.

**Why:** No more compatibility shim needed.

### 7.15 Drop the file-level `too-many-lines` disable on `db/models.py`

**What:** It's gone — but make sure no new `db/models.py` is created. Add a sanity check or comment in `db/models/__init__.py`.

**Why:** Prevents accidental regression.

---

## Section 8 — Bus-factor / onboarding

### 8.1 Add a CONTRIBUTING.md

**What:** Create `CONTRIBUTING.md` (~30 lines) covering: branch naming, how to run tests with `--test` mode, the publish/swap rule, the "no parallel `*_finish` workers for ClaimsPricing/DrugClaims" rule, where to add new importers (`process/<name>.py` + register in `process/__init__.py`).

**Why:** Right now this knowledge is scattered across local notes and `specs/base_arch_prompt.md`. A human-facing CONTRIBUTING is the conventional location and will be found by GitHub's contributor UI.

### 8.2 Add a one-screen architecture diagram to `docs/`

**What:** `docs/architecture.md` with a Mermaid diagram: CLI → ARQ queues → Postgres staging → publish/swap → live tables → Sanic API. Reference it from `README.md`.

**Why:** New contributors get the system shape in 60 seconds rather than reading 11k-line files.

### 8.3 Link the spec index from README

**What:** Add to README under "Documentation":

> Engineering specs and design notes: see [`specs/`](./specs/). Start with `specs/base_arch_prompt.md` and `specs/data_source_registry.md`.

**Why:** `specs/` contains the real engineering reasoning. Today it is only discoverable by file browsing.

---

## Section 9 — Verification pass

### 9.1 Confirm repo size dropped

**What:**

    du -sh .git
    git count-objects -vH

Compare to the baseline from 1.1.

**Why:** Sanity-check that all the `git rm --cached` operations had the intended effect. Note: `.git` history still contains the old blobs; cleanup of history is a separate, more invasive operation (BFG / `git filter-repo`) and is *not* in this plan.

### 9.2 Run the full test suite

**What:** `pytest tests -x` (or `pytest -x` since pytest.ini sets testpaths).

**Why:** Catches anything Section 7 broke and confirms `__test__/` removal didn't drop coverage.

### 9.3 Run a smoke importer in `--test` mode

**What:** `python main.py start npi --test` (or whichever is fastest).

**Why:** Section 5 and Section 7 touch DB-adjacent code. A real importer round-trip is the strongest signal nothing regressed.

### 9.4 Push the cleanup branch and open a PR

**What:** Push `chore/repo-cleanup`, open PR, request review.

**Why:** Cleanup ships as a stack of small commits, each individually revertable. Reviewer sees the trajectory rather than one mega-diff.

---

## Out of scope (call out in the PR description)

- Rewriting git history to actually shrink the `.git` directory (`git filter-repo` to expunge the old venv/blobs). This requires a force-push and coordination with every clone holder. Mention it in the PR; do not do it in this stack.
- Splitting `process/ptg.py` (11k lines) and `process/provider_quality.py` (4.8k lines). Same pattern as Section 7 but each is its own multi-day project.
- Migrating per-import bootstrapped tables to Alembic. Deliberate architectural choice; revisit only if the team decides to.

---

## Suggested ticket sequence (copy into your tracker)

1. Section 1 — Stop tracking junk (11 sub-tickets, ~1 hour total)
2. Section 4 — Resolve uncommitted work (must finish before Section 5+)
3. Section 3 — Reconcile test directories
4. Section 2 — `reports/` decision + implementation
5. Section 5 — Alembic clarity docs
6. Section 6 — Pylint config
7. Section 8 — CONTRIBUTING + architecture doc
8. Section 7 — `db/models.py` split (multi-day, do last)
9. Section 9 — Verification + PR
