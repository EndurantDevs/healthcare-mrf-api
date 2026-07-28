# Frozen multipart PTG dispatch

This runbook defines the operator contract for immutable multipart PTG rate-file
dispatch. It is a separate V4 control-plane path. It does not replace or modify
the retained strict-V3 import and review contracts.

## Admission envelope

A protected request supplies these four fields together or supplies none of
them:

- `frozen_rate_file_set_contract`
- `frozen_rate_files`
- `frozen_rate_file_set_sha256`
- `frozen_rate_file_count`

The contract value is `ptg_frozen_rate_file_set_v1`. A protected control
request also carries the same nonempty `source_file_import_id` and `import_id`
in its outer and parameter envelopes. Any partial tuple or identity difference
is rejected before queue admission.

The file array contains between 2 and 128 exact descriptors. Each descriptor
has only the v1 fields:

`source_type`, `canonical_url`, `content_length`, `etag`, `last_modified`,
`raw_sha256`, `logical_sha256`, `logical_hash_deferred`,
`engine_source_identity_hash`, `engine_source_file_version_id`, and `ordinal`.

Every descriptor must have `source_type: "in_network"` and use one shared,
query-free HTTPS origin. Frozen multipart does not accept `allowed_amounts`;
those coordinates continue through the existing direct scalar allowed-amount
path outside this protected envelope. In Release 1, all 14 multipart
coordinates are in-network; the two allowed-amount coordinates remain direct
scalar imports. Ordinals are exactly `1..N`; URLs, raw hashes, engine
identities, and engine file-version identities are unique.
Engine identities are lowercase hexadecimal strings of exactly 16, 32, or 64
characters, matching the supported checksum64, BLAKE2-128, and SHA-256 modes.
The healthcare generator emits source-identity/file-version widths of `16/16`,
`32/32`, and `64/32` for those modes; the frozen validator also accepts a
64-character file-version identity for cross-service compatibility.
Each file has a strong ETag or Last-Modified validator. The canonical envelope
is at most 256 KiB, and its supplied SHA-256 must match the canonical bytes.

The sum of declared `content_length` values is checked before work is queued.
The default aggregate ceiling is 512 GiB and can be tightened with
`HLTHPRT_PTG2_FROZEN_TOTAL_MAX_BYTES`; the value must be a positive integer.
The protected path is mutually exclusive with scalar file URLs, TOC discovery,
URL filters, and a conflicting `max_files` value.

## Durable source-file binding

Admission inserts or compares one row in
`mrf.ptg2_frozen_source_file_binding`, keyed by
`source_file_import_id`. The row binds that external identity to:

- the exact internal run ID `ptg2:<source_file_import_id>`;
- the frozen set contract, digest, and count;
- source key and first-of-month import date;
- canonical plan IDs and market types; and
- the canonical binding payload and its digest.

An advisory transaction lock serializes the insert-or-compare operation.
Exact replay is allowed; any changed coordinate is a terminal contract
failure. The control plane writes this binding before its normal lifecycle row.
The worker rechecks it before executing, and direct engine entry binds it after
schema readiness and the source lock but before snapshot lookup.

Database triggers reject `UPDATE`, `DELETE`, and `TRUNCATE`, including
replication-role bypasses. The migration downgrade takes an exclusive lock and
refuses to remove a nonempty binding table. Operators must not edit or clear a
binding to force a retry; changed source evidence requires a new admitted
source-file identity.

## Acquisition and retained artifacts

Every non-reused protected artifact is acquired with an exact full-body GET.
Range requests and partial-prefix resume are disabled for that path. Before
each retry, incomplete body and range-sidecar state are discarded, so every
attempt starts at byte zero. The successful final GET must provide a 2xx
response plus final URL and response metadata; the complete body length and
raw digest must match the frozen descriptor.

HEAD is advisory. A retained, sealed local artifact may be accepted when the
origin does not support HEAD, but any explicit live URL, validator, or length
returned by the origin must still match. Retained and fresh artifacts are
checked against canonical URL, byte count, raw SHA-256, and the logical
SHA-256 when it was not deferred.

A contract mismatch may discard only attempt-private incomplete staging.
Validation never unlinks a published, retained, shared, aliased, or
pre-existing CAS object. Normal failed-import cleanup may remove unpublished
attempt tables and private copy artifacts, but it preserves the immutable
source-file binding and any candidate or published state whose ownership is
not conclusively safe to remove.

## Candidate revalidation and activation

Publication records an ordinal-complete proof for every file. Candidate audit
then recomputes the set and proof digests and requires all of the following:

- the manifest binding exactly equals the immutable database binding;
- the candidate run ID is the binding's exact internal run ID;
- proof descriptors and source-file-version rows exactly match all frozen
  descriptors;
- database source keys are the dense physical-identity set `0..N-1`;
- database source version IDs and raw container digests are unique and exactly
  equal the frozen descriptor sets; and
- file, version, raw-hash, and database-source cardinalities remain exact and
  unambiguous;
- `raw_byte_count` equals the descriptor `content_length` in both the proof and
  source-version row; and
- both rows carry the same nonempty implemented `verification_mode`:
  `downloaded`, `strong_etag_length`, `length_last_modified`, or
  `verified_local_sha256`.

Any validation or mismatch exception becomes
`ptg_frozen_rate_file_contract_failed` with `retryable: false`. Candidate audit
turns the same evidence failure into a release-gate error, so the candidate is
not activated.

## Progress semantics

Download progress begins as file-count work. After the complete batch is
validated, processing progress is monotonic and weighted by each unique
artifact's measured compressed byte count, not by equal file slices.
Duplicate physical input contributes zero additional weight. Progress events
identify the one-based file index, total file count, safe file label, file
weight, and whether it is the dominant file. `files_completed` advances only
after successful processing; counters are aggregated across files.

For the protected path, the safe label is only
`frozen-part-<ordinal>-of-<count>-<opaque-digest-prefix>`. Download logs, screen
output, live progress, acquisition errors, and public run responses do not
render URLs, validators, content hashes, or engine source identities. Public
run responses expose only the protected marker and part count; the complete
descriptor tuple remains internal to admission, the worker, and candidate
audit.

The scan stage occupies its assigned run interval and cannot reach its endpoint
until all files complete. A very large part can therefore dominate the
percentage and ETA without making smaller parts appear to represent equal
work.

## Storage attribution and measurement

Frozen multipart is a control and evidence projection over the normal shared
PTG publication path. It owns zero serving, rate, provider-graph,
snapshot-map, logical-snapshot, or CAS-block bytes. It does retain the exact
compressed source artifacts required by the sealed snapshot. Those files are
not attributed to the control projection, but they are real snapshot storage
and must be measured once by the whole-snapshot canary.

The feature retains its immutable binding rows and the ordinary candidate-audit
metadata separately. `measure_frozen_binding_storage()` reports
`ptg_frozen_rate_storage_attribution_v2`, six explicit zero owned-payload
fields, the binding row count, and
`pg_total_relation_size(mrf.ptg2_frozen_source_file_binding)`. Candidate-audit
metadata continues through the existing candidate-audit storage gate.

For each release canary, record both:

1. this control-metadata measurement; and
2. the whole-snapshot/shared-layout measurement, including every referenced
   compressed artifact and the PostgreSQL artifact-manifest/blob relations.

The first is retained evidence overhead. The second is the only source of
serving, graph, map, rate, snapshot, raw-artifact, and CAS payload bytes. The
whole-snapshot gate reconciles each descriptor to one source-version row and
one artifact-manifest row, requires the content-addressed file below
`HLTHPRT_PTG2_ARTIFACT_DIR`, and reports both exact file length and allocated
filesystem bytes. The compressed acquisition total must match the manifest
storage budget. The import does not blanket-gzip or recompress source data.

Run the retained-artifact storage gate inside the exact deployed healthcare
container or a disposable pod using the same image and mounted artifact PVC.
It must see the same `HLTHPRT_PTG2_ARTIFACT_DIR` as the importer. A workstation
or DB-only execution without that mounted volume is not acceptable evidence
and fails closed. Record the deployed source SHA, image digest, pod/PVC
identity, snapshot/import identity, and canonical evidence digest with the
canary result.

## Operator response

For a terminal frozen-contract failure:

1. Keep the immutable binding row and retained artifacts intact.
2. Compare the admitted canonical set digest, file count, source identity, and
   first mismatch reported by the worker or candidate audit.
3. Confirm whether the origin changed its full-body bytes, validator, URL, or
   declared length.
4. Replay only when the request is byte-for-byte and coordinate-for-coordinate
   identical. Otherwise admit a new source-file identity and frozen envelope.
5. Do not downgrade the binding migration while any binding row exists.
