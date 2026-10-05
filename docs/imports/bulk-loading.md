# Bulk loading and snapshot publication

This is the required design for new or changed high-volume import paths, not a
claim that every existing importer already implements it. Existing per-row
database validation must be replaced when migrating those paths; increasing a
COPY batch while retaining the same per-row work is not completion.
This applies throughout staging, admission, retained-family copying, graph and
output materialization, and legacy import paths, not only initial SOURCE loading.

## Loading and validation

- COPY bounded, large row/byte batches into isolated candidate storage using
  the declared model/schema. Keep only essential load-time indexes and
  constraints. Do not load a replacement into the snapshot serving API reads.
- Replace per-record database queries, authority lookups, counter updates and
  row-based custom validation triggers with set-based batch/snapshot checks.
  Do not introduce new row-based custom triggers on high-volume import relations.
  This includes custom UPDATE guards and accounting, not just INSERT guards.
- Refresh native planner statistics on the isolated bulk-loaded candidate before
  its indexed set checks. Index presence alone does not prove an efficient plan;
  inspect representative plans and include sampled analysis in pipeline timings.
  Do not analyze or rebuild unrelated live/history storage for a candidate.
- Preserve the validation rules: types, canonical bytes and hashes, key
  uniqueness/collisions, source ordinals, counts/bytes, parent-child references,
  complete-family rejection, provenance and source-bundle consistency. Parsing,
  normalization and row-local type/hash computation still visit each record;
  they must not cause a database round trip or authority lock per record.
- Check execution ownership, fence, cancellation and deadline once per bounded
  write transaction, with a fresh check after waits and before commit. Persist
  the batch and its durable cursor atomically. A stale or failed batch rolls
  back; uncertain commit results use the existing durable-prefix resume path.
  Coalescing SOURCE packs must also flush within the live lease window; row and
  byte caps alone are not a heartbeat. Keep the immutable build deadline fixed.
- Finish serving projections, deferred indexes and relationship validation before
  publication. Never drop or rebuild shared live/history indexes to speed up a
  candidate. Every deferred invariant needs equivalent completed proof.
- Validate immutable snapshot relationships with indexed, set-based anti-joins
  on the isolated candidate instead of per-record foreign-key checks. Check the
  complete parent/child, revision, family, projection and winner identities,
  including their dataset, schema and producer scope; an ID existing elsewhere
  is not sufficient. Keep essential native PK/UNIQUE/NOT NULL/CHECK constraints.
  Low-volume ownership, lease and batch-control constraints are a separate
  boundary, not a reason to retain foreign keys on every snapshot record.
- Close candidate writes before complete snapshot validation and keep them
  closed through publication. Failed checks leave the incumbent untouched;
  successful checks are invalid if the candidate can change afterward. Reuse
  candidate-only indexes for these checks and serving, and verify native query
  plans do not scan unrelated snapshots or history.
- Migrate every affected writer before retiring its old validation triggers.
  Restrict canonical writes to protected batch/promotion entry points; workers
  must not retain direct write access that bypasses their checks.
  Temporary COPY transport grants end with the last authorized batch in their
  transaction, not only when the entire candidate eventually freezes.
  Do not use caller-controlled bypass flags or disable/re-enable triggers as a
  substitute for validation: reenabling a trigger does not validate skipped rows.

Custom imports use generic, migration-managed storage with explicit dataset,
build and generation identities, not per-client tables or arbitrary runtime DDL.
The storage layout must provide candidate isolation and the intended index
lifecycle; an atomic publication pointer alone does not eliminate loading costs.

Use a fixed, model-defined table family in an engine-generated namespace. Keep
canonical row shapes and stable logical/entity IDs; do not add a snapshot column
to existing composite payload types or allocate replacement identity dictionaries.
Register exact relation OIDs against the existing producer identity, then bind
that family immutably to its generation. Prepare processing indexes only when
their phase needs them, and typed serving indexes after candidate writes close.
Neither readers nor validation may resolve a family from caller-supplied names.
Do not copy redundant UNIQUE indexes whose columns contain the primary key:
without snapshot FKs the primary key already proves their uniqueness. Preserve
genuine natural-key, content, position, membership and context uniqueness.

## Interchangeable snapshots

One declared model/schema can have multiple complete snapshots. Keep snapshot A
serving while B loads, validates and finishes its indexes. Publish B with one
atomic transition only after complete verification. Use the importer's existing
table-family rotation or generation-pointer mechanism; do not rename globally
shared tables for one dataset.

Each API request pins one snapshot and its schema/selection identity through
counting, pagination and root/child hydration. In-flight reads finish against A;
new reads may use B after the switch. Retain A for exact rollback. Candidate
failure must leave A and its publication identity unchanged, with no partially
published or empty-data interval.

## Verification

Compare the old and new complete pipeline on identical representative data and
native hardware. Include mapping, COPY, set validation, promotion, index and
relationship checks, admission, graph/output construction, verification and cutover;
report WAL, index/history/capture storage and cleanup as well as elapsed time.
COPY-only timings do not establish end-to-end capacity.

Test canceled/stale/expired writers, failure in the last batch, interrupted
validation/indexing, uncertain commit/resume, duplicate and invalid-child
semantics, concurrent reads during loading/cutover, competing publication and
exact rollback. Local measurements do not replace source-bound deployment and
API behavior proof.
