# Massachusetts physician education and training

The `massachusetts-borim-profile` managed importer reads the official
[Massachusetts physician profiles](https://findmydoctor.mass.gov/). Its coverage
is a frozen NPPES cohort of individual providers in the exact NUCC Allopathic &
Osteopathic Physicians grouping with numeric Massachusetts licenses. The state
search is capped, so this cohort must not be described as every Massachusetts
physician or a statewide census. Excluded license formats remain in the cohort
artifact for reporting.

Medical-school names, graduation dates, and postgraduate training retain their
source precision and provenance. Training is separate from clinical experience.
Missing dates do not establish current enrollment or completion. A license or
NPI/name conflict retains the record without publishing it for a guessed NPI.
Pending-review profiles and sections hidden by the public profile are withheld.
See the board's [profile terms and definitions](https://www.mass.gov/info-details/borim-profiles-terms-and-definitions).

## Run and verify

Use the managed import API on the intended engine node. This importer requires
a managed run identity; standalone CLI execution is disabled. A bounded run uses
`{"max_providers": 100}` and completes without advancing public data. A full run
uses `{}` and requests every eligible license in its frozen cohort. The worker
fetches sequentially at no more than two requests per second; allow several
hours for a full run. HTTP errors, oversized responses, changed retained bytes,
and invalid source schemas fail the run.

Artifacts are retained under `/work/massachusetts-borim/<profile-run-id>` on the
worker's persistent import-workdir volume. `HLTHPRT_MA_BORIM_ARTIFACT_ROOT` can
select another persistent root. The worker defaults to a 512Mi memory request,
4Gi limit, and a 24-hour ARQ timeout; explicitly configured worker resource
profiles retain precedence.

A failed run less than seven days old can be resumed with a new managed
run using `{"resume_from": "<profile-run-id>"}` and the original
`max_providers` value, if any. Resume verifies the sealed cohort and every reused
response, creates a new artifact directory, and requires the same publication
predecessor. Never reuse an execution ID or remove another active run to force a
resume.

Publication requires one retained record per requested license, no transport or
integrity failures, and a full cohort. At least half of requested licenses must
resolve to full-license profiles. The first publication additionally requires
10,000 matched providers with public facts; later publications must preserve at
least 80% of both the incumbent matched-provider and received-profile counts.
These guards have no partial-publication or volume-drop override.

The source pointer, source completion, and exact managed run attempt succeed
in one transaction. Source retention preserves current/previous publications,
the latest run, active runs and their resume dependencies, and recent failures;
audit run rows remain. Florida retention must be source-scoped before this
importer is enabled.

After a full publication, verify the exact worker source/image, terminal control
and source run, pointer, and retained counts. Verify public school and training
facts with their `massachusetts-borim` generation through both provider-profile
API and MCP responses, including evidence pagination, preserved Florida/CMS
facts, and corroboration versus distinct or conflicting education records.
