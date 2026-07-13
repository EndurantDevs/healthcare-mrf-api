# In-Network Provider Specialty Filtering for Group Plans

**Status:** Proposed · **Date:** 2026-06-24 · **Owner:** TBD
**Scope:** healthcare-mrf-api engine, serving, and enumeration endpoints plus implementation-neutral API consumers
**Related:** the `group_plan_providers` enumeration endpoint (`api/endpoint/pricing.py`) and the PTG2 serving search (`api/ptg2_serving.py`).

This spec is self-contained: a coding agent can pick it up cold. Every "current behavior" claim cites public service code or a reproducible development result.

---

## 1. Problem & motivation

When a member of a group plan asks "find me a covered doctor for X near me, and what will it cost," the system can return a provider of the **wrong specialty** with a real negotiated price. In a production-like minimal-layout case, a routine flu visit surfaced:

- a provider whose NPPES primary taxonomy was `207P00000X` = **Emergency Medicine**
- a provider whose taxonomy was `363A00000X` = **Physician Assistant** (no specialty)

Neither is a primary-care provider. Proposing them for a flu is wrong and erodes trust. The system must never hand a member an anesthesiologist, ER physician, or allergist for a primary-care need.

## 2. Background — why the plan data alone cannot answer "who is a PCP"

Transparency-in-Coverage (TiC) in-network files — the source of a group plan's coverage — contain only **NPI + TIN + negotiated rate per billing code**. They carry **no specialty**. Verified against an imported dense minimal-layout snapshot:

- serving table columns: `serving_content_hash_128, plan_id, procedure_global_id_128, reported_code_system, reported_code, provider_set_global_id_128, provider_count, price_set_global_id_128, source_trace_set_hash` — procedures + rates, **no taxonomy**.
- provider membership table columns: `provider_group_global_id_128, npi` — **just the NPI**.
- `provider_group_location_table` (the one PTG2 table that *can* carry `taxonomy_codes`/`specialties`) is **null** for this snapshot.
- negotiated-search response items expose `npi, address, price_summary, prices, procedure_code` — **no specialty field**.

**Specialty exists only in NPPES** (`mrf.npi` / `mrf.npi_taxonomy` + `mrf.nucc_taxonomy`). Therefore the only correct way to constrain a plan's in-network providers to a specialty is to **join each in-network NPI to NPPES taxonomy and filter there**. There is no shortcut inside plan data.

## 3. Current behavior (what's broken) — evidence

1. **The serving-search `specialty` filter is a no-op for minimal-layout snapshots.** In `api/ptg2_serving.py` the specialty predicate (`nucc.display_name LIKE :specialty_like`, ~L1601–1610 and L1654–1658) is only emitted inside two branches: one gated on `serving_tables.provider_group_location_table` (~L1583) and one built `FROM {provider_set_component_table} … JOIN {provider_group_member_table}` (~L1633–1660). The affected layout has **neither** `provider_group_location_table` **nor** `provider_set_component_table` (only `provider_group_member_table`), so no specialty predicate is applied. A synthetic regression request for `plan_id=TESTPLAN001` must return a strict subset when `specialty=Family%20Medicine` is added.
2. **`classification` is ignored by the serving search** — only `specialty` is read (`api/ptg2_serving.py` ~L1551). External callers may send `classification`/`taxonomy_codes`, but the serving engine drops them.
3. **The group-plan enumeration endpoint has no specialty filter at all** — `api/endpoint/pricing.py::group_plan_providers` enumerates every in-network NPI with no taxonomy constraint.
4. **Some automation clients cannot scope provider enumeration by specialty** because they do not expose the parameter.
5. (Secondary) Client wrappers must preserve plan-scoped pricing query parameters and return the same data as direct HTTP requests.

## 4. Goals / non-goals

**Goals**
- A member-facing search/enumeration can be constrained to a clinical specialty (or a curated "primary care" group), and **every returned provider's NPPES taxonomy matches**, for **all** snapshot storage layouts (including the minimal `provider_group_member`-only layout).
- Specialty filtering composes with the existing plan-scoped negotiated-rate search, so results are: in-network ∧ right specialty ∧ with negotiated price ∧ near the member.
- Honor `specialty` (free text → NUCC), `classification` (NUCC classification), and `taxonomy_codes` (exact NUCC codes).

**Non-goals**
- Importing specialty into plan/MRF data (it isn't in TiC; out of scope).
- Provider quality/cost ranking changes.
- Fixing malformed 9-digit NPIs in legacy source data (tracked separately).

## 5. Design overview

Add a **single, layout-independent specialty predicate** that filters by joining the candidate NPIs to NPPES (`mrf.npi_taxonomy` → `mrf.nucc_taxonomy`). Apply it in two engine paths:

- **A. Serving/pricing search** (`api/ptg2_serving.py`): when neither `provider_group_location_table` nor `provider_set_component_table` is available, filter on `provider_group_member.npi` directly via NPPES. When they *are* available, keep the existing (more selective) path but ensure it uses the same NUCC matching rules.
- **B. Group-plan enumeration** (`api/endpoint/pricing.py::group_plan_providers`): add a taxonomy join + filter on the enumerated DISTINCT NPIs.

Expose the parameters through the public HTTP contract. Specialty resolution (free text/classification -> NUCC code set) is shared logic so all entry points agree.

## 6. Specialty resolution & PCP taxonomy set (the critical correctness detail)

NUCC taxonomy is `Grouping / Classification / Specialization`. **Do not match `nucc.display_name LIKE '%family medicine%'` or `classification = 'Internal Medicine'` alone** — `classification = 'Internal Medicine'` also matches **Cardiology** (`207RC0000X`, classification "Internal Medicine", specialization "Cardiovascular Disease"). That is exactly how a cardiologist/ER doc leaks in.

Resolution rules:
- `taxonomy_codes` → use the exact codes verbatim.
- `classification` → match `nucc.classification = :classification` **AND** (`nucc.specialization IS NULL OR :include_subspecialties`) so "Internal Medicine" means general internists, not subspecialists, unless explicitly opted in.
- `specialty` (free text) → resolve through a curated map to a code set (reuse/extend the terminology/NUCC lookup already used by `resolve_specialty` / `nucc_taxonomy`), never raw LIKE on display_name.
- Define a named **`primary_care` group** = the PCP taxonomy allowlist:
  - `207Q00000X` Family Medicine, `207R00000X` Internal Medicine (general), `208D00000X` General Practice,
  - `208000000X` Pediatrics (general), `207QA0000X`/geriatric general-practice variants,
  - Nurse Practitioner primary care `363LA2200X`, `363LF0000X`, `363LP2300X`,
  - Physician Assistant `363A00000X` **only** when paired with a primary-care setting (PA is specialty-agnostic — treat as PCP only with an explicit opt-in or when its practice location/claims indicate primary care),
  - Urgent care `261QU0200X` (clinic) for acute primary-care-equivalent visits.
  The allowlist lives in one place (e.g. `api/specialty_groups.py` or extend `nucc_taxonomy` metadata) and is unit-tested.

## 7. API contract changes

### 7.1 healthcare-mrf-api — serving search (`api/ptg2_serving.py`)
- Read `specialty`, `classification`, `taxonomy_codes` (currently only `specialty`).
- Resolve them to a NUCC code set via the shared helper (§6).
- Add a filter branch for the **minimal layout** (`provider_group_member_table` present, `provider_group_location_table` and `provider_set_component_table` absent):
  ```sql
  AND EXISTS (
    SELECT 1 FROM mrf.npi_taxonomy nt
    WHERE nt.npi = pgm.npi
      AND nt.healthcare_provider_taxonomy_code = ANY(:taxonomy_code_set)
  )
  ```
  applied where the in-network NPIs are produced for include_providers expansion.
- Keep existing location/set_component branches but route them through the same code-set matching.

### 7.2 healthcare-mrf-api — enumeration endpoint (`api/endpoint/pricing.py::group_plan_providers`)
- New query params: `specialty`, `classification`, `taxonomy_codes` (CSV), optional `include_subspecialties` (bool, default false).
- When any is set, constrain the DISTINCT-NPI query:
  ```sql
  SELECT DISTINCT pgm.npi
    FROM {group_member_table} pgm
    JOIN mrf.npi_taxonomy nt ON nt.npi = pgm.npi
   WHERE pgm.npi > :cursor_npi
     AND nt.healthcare_provider_taxonomy_code = ANY(:taxonomy_code_set)
   ORDER BY pgm.npi LIMIT :limit
  ```
  (keyset pagination preserved; `npi_taxonomy.npi` is indexed.)
- Response unchanged except an echo of the resolved `taxonomy_code_set` for transparency.

### 7.3 External gateways
- Add `specialty`, `classification`, `taxonomy_codes`, `include_subspecialties` to the **allowed params** of `pricing_group_plan_providers` (the pricing-search route already forwards `specialty`/`classification`/`taxonomy_codes`; verify they reach the serving engine unchanged).

### 7.4 Automation clients
- Provider-enumeration clients should expose `specialty` / `classification` and forward them to the public endpoint.
- Procedure-search clients should forward `specialty`/`classification`/`taxonomy_codes` together with plan parameters and `include_providers`.
- Tool descriptions: for a member search, **default to specialty-scoped** — instruct the agent to pass the member's needed specialty (or `specialty=primary care`) so an off-specialty provider is never proposed.
- Add parity tests proving wrapped and direct calls return the same plan-scoped data.

## 8. Acceptance criteria

1. `GET /api/v1/pricing/group-plan-providers?plan_id=TESTPLAN001&market_type=group&specialty=Family%20Medicine` returns NPIs whose NPPES **primary** taxonomy is in the Family-Medicine/General-Practice set — verified by re-querying `getProviderByNpi` for every returned NPI. No Emergency Medicine, no Cardiology, no bare PA.
2. `searchPricingProvidersByProcedure?plan_id=TESTPLAN001&market_type=group&code=99214&include_providers=true&specialty=Family%20Medicine` returns **only** family-medicine in-network providers near the location, each with the negotiated rate. The result set is a **strict subset** of the unfiltered call (proving the filter is applied, not ignored).
3. `classification=Internal%20Medicine` (without `include_subspecialties=true`) returns general internists and **excludes** cardiologists (`207RC0000X`) and other IM subspecialists.
4. Works for both synthetic minimal-layout and fuller-layout snapshots.
5. An automation-client call for `plan_id=TESTPLAN001` and `specialty="primary care"` returns only PCPs.

## 9. Testing

- **Unit:** specialty resolver (§6) — `"family medicine"`/`"primary care"`/`classification=Internal Medicine` → expected code sets; assert cardiology code is excluded from the IM general set. (`healthcare-mrf-api/tests/`)
- **Engine/integration:** seed a tiny snapshot with provider_group_member NPIs of mixed taxonomies; assert the specialty filter returns only matching NPIs in both minimal and location/set_component layouts. (`tests/test_npi_api*` / a new `tests/test_group_plan_provider_specialty.py`)
- **API consumers:** add a "covered PCP near member with negotiated price" scenario that asserts every returned NPI's taxonomy matches.
- **Regression:** a test that fails if a specialty filter is silently dropped (compare filtered vs unfiltered count and taxonomy of results).

## 10. Rollout & migration

- No schema migration; uses existing `mrf.npi_taxonomy`. Confirm an index on `npi_taxonomy.npi` exists (it does — created by the NPI loader).
- Deploy order: public engine and endpoint first, then external gateway parameter passthrough and automation-client updates. Each component remains independently deployable.
- Backward compatible: all new params optional; absent = today's behavior.

## 11. Risks & mitigations

- **Display-name matching leaks subspecialties** → match on NUCC code sets / `classification` + `specialization IS NULL`, never free-text LIKE (§6). This is the central correctness control.
- **PA / NP ambiguity** (specialty-agnostic) → exclude bare PA from `primary_care` unless opted in; document the choice.
- **Empty results when a plan's in-network set has no provider of the requested specialty** → return an explicit "no in-network <specialty> provider found near you; nearest in-network options / widen radius" rather than silently returning the wrong specialty.
- **Performance** of the NPPES join on multi-million-NPI plans → keyset pagination + indexed `npi`; the join is per-page, not whole-set.

## 12. Out of scope / future

- Surfacing per-NPI negotiated rate for an arbitrary specialty-correct provider (today the rate is per provider-set; if a specialty-correct in-network provider isn't in a priced set, fall back to the plan's set-level negotiated rate for the code with a clear caveat).
- A combined one-call client operation (`find_covered_providers(plan, specialty, procedure, location)`) that returns specialty-correct, in-network, priced providers is a natural follow-up once the filter lands.
