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
- Preserve the validation rules: types, canonical bytes and hashes, key
  uniqueness/collisions, source ordinals, counts/bytes, parent-child references,
  complete-family rejection, provenance and source-bundle consistency. Parsing,
  normalization and row-local type/hash computation still visit each record;
  they must not cause a database round trip or authority lock per record.
- Check execution ownership, fence, cancellation and deadline once per bounded
  write transaction, with a fresh check after waits and before commit. Persist
  the batch and its durable cursor atomically. A stale or failed batch rolls
  back; uncertain commit results use the existing durable-prefix resume path.
- Finish serving projections, deferred indexes and constraint validation before
  publication. Never drop or rebuild shared live/history indexes to speed up a
  candidate. Native integrity constraints are not permission to retain repeated
  custom validation; every deferred invariant needs equivalent completed proof.
- Preserve native PK/UNIQUE/FK/NOT NULL/CHECK guarantees. Complete permitted
  deferred native validation before publication; do not weaken shared canonical
  constraints.
- Migrate every affected writer before retiring its old validation triggers.
  Restrict canonical writes to protected batch/promotion entry points; workers
  must not retain direct write access that bypasses their checks.
  Do not use caller-controlled bypass flags or disable/re-enable triggers as a
  substitute for validation: reenabling a trigger does not validate skipped rows.

Custom imports use generic, migration-managed storage with explicit dataset,
build and generation identities, not per-client tables or arbitrary runtime DDL.
The storage layout must provide candidate isolation and the intended index
lifecycle; an atomic publication pointer alone does not eliminate loading costs.

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
constraint work, admission, graph/output construction, verification and cutover;
report WAL, index/history/capture storage and cleanup as well as elapsed time.
COPY-only timings do not establish end-to-end capacity.

Test canceled/stale/expired writers, failure in the last batch, interrupted
validation/indexing, uncertain commit/resume, duplicate and invalid-child
semantics, concurrent reads during loading/cutover, competing publication and
exact rollback. Local measurements do not replace source-bound deployment and
API behavior proof.
