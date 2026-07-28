# Legacy PTG orphan sweeper

The legacy PTG orphan sweeper is a versioned maintenance command for dynamic
PTG table families that predate shared-block snapshot storage. It complements
normal snapshot removal and shared-block garbage collection; it does not
replace either path.

The command is dry-run by default:

```bash
python -m process.ptg_parts.ptg2_legacy_orphan_sweeper \
  --schema mrf \
  --control-schema <control-plane-schema>
```

Omit `--schema` to use the shared PTG schema resolver. If
`HLTHPRT_DB_SCHEMA` and `DB_SCHEMA` disagree, both dry-run and apply refuse
before opening a database transaction. Supplying `--schema` is an intentional
reviewed override. The lifecycle-control schema must be supplied with
`--control-schema` or `HLTHPRT_PTG_CONTROL_SCHEMA`; there is no
environment-specific built-in default.

Dry-run prints an aggregate JSON summary and a `plan_digest`. It does not print
source, plan, payer, employer, snapshot, import, or relation identities.
`catalog_suffixes`, `scanned_suffixes`, and `unscanned_suffixes` show progress
through large legacy catalogs. Each plan construction scans legacy-shaped
relation identities once under a separate hard ceiling, then loads catalog
detail by exact indexed OID for windows bounded by both 100 suffixes and 5,000
unique relations until the requested cleanup batch is full. The in-memory
suffix-to-relation index is built once; neither PostgreSQL nor that inventory
is rescanned for each window. Dry-run constructs one plan. The first apply
constructs it once before and once after acquiring exact root locks so drift
fails closed; an exact audit replay does not rebuild the catalog. A later
invocation starts from the remaining lexical catalog after the prior batch is
removed.

Applying a plan requires the exact reviewed digest and an auditable actor:

```bash
python -m process.ptg_parts.ptg2_legacy_orphan_sweeper \
  --schema mrf \
  --control-schema <control-plane-schema> \
  --apply \
  --expected-plan-digest <64-lowercase-hex> \
  --actor <maintenance-actor> \
  --max-suffixes <dry-run-value> \
  --max-tables <dry-run-value> \
  --max-relations <dry-run-value> \
  --max-bytes <dry-run-value>
```

## Safety contract

The sweeper:

- recognizes only the frozen legacy PTG relation prefixes followed by an exact
  lowercase 32-hex import suffix;
- validates schema, owner, OID, relation kind, persistence, columns, indexes,
  sequences, inheritance, user-trigger definitions/enabled state, and generic
  PostgreSQL dependencies before probing rows;
- binds every required lifecycle-authority OID and column shape, the exact
  present/absent state of optional transient stage relations (plus their OID
  and shape when present), and the immutable audit trigger into the reviewed
  plan;
- blocks building, running, validated-without-owner, active/unknown placement,
  current/previous pointer, route, release, pin, shared-binding, attempt-fence,
  cross-owner, serving-row, allowed-amount, candidate-audit, and stage residue;
- preserves raw manifest and run identities while matching candidates, so a
  manifest/run mismatch or a control/mirror snapshot claim against another raw
  snapshot owner cannot authorize deletion;
- filters snapshot, run, job, mirror, control, and placement ownership reads to
  candidate identities and fails closed at fixed catalog/ownership row
  ceilings before destructive planning;
- permits nonempty tables only with exact terminal snapshot/import ownership;
- permits ownerless relations only when every allowed root table is proven
  empty;
- serializes with the shared PTG lifecycle lock and rechecks the complete plan
  after acquiring exact locks on every present authority table; application
  setup for both optional stage relations acquires the same lifecycle lock
  before DDL, keeps required `CREATE` fail-fast, and isolates each tolerated
  `ALTER` or index statement in its own savepoint, while the final recheck
  still binds optional absence or exact OID/schema/shape when present;
- caps the read-only relation/suffix inventory and every indexed catalog
  detail window separately from the lower transaction ceilings for suffixes,
  root tables, dependent relations, and bytes; every ceiling is hard and
  non-overridable;
- skips and explicitly classifies an individually oversized lexical family so
  it cannot starve smaller later families; a family above either the
  5,000-relation inspection ceiling or 10 GiB transaction ceiling requires a
  separately designed, reviewed large-family cleanup path and is never split
  or dropped by this command;
- deletes only exact owned snapshot scope/source and retained-artifact
  metadata, then drops exact root tables without `CASCADE`;
- writes one immutable audit row in the same transaction; a failure rolls back
  metadata, audit, and every table drop;
- treats an exact applied digest as an idempotent replay only after validating
  the audit proof and all removal postconditions.

The audit table rejects `UPDATE`, `DELETE`, and `TRUNCATE`, including when
`session_replication_role=replica`. Its migration takes an
`ACCESS EXCLUSIVE` lock before checking emptiness and refuses downgrade after
the first cleanup record exists.

## Operating sequence

1. Confirm the migration is applied and no import, audit, publication, route,
   release, cleanup, Alembic migration, or manual DDL operation is in flight.
   Alembic and manual DDL are a trusted operator boundary: they must never run
   concurrently with either dry-run review or apply.
2. Run dry-run with conservative limits and retain its aggregate output,
   including all four values under `limits`.
3. Review `blocked_reason_counts`, selected counts, and the exact
   `plan_digest`. Block reasons are neutral aggregates; identities remain out
   of the command output.
4. Apply that digest once with the exact same four `--max-*` values. Any
   catalog, lifecycle, or limit drift fails closed.
5. Replay the same command only to prove idempotency.
6. Re-run dry-run and storage inventory. Continue in bounded batches.

This command does not remove shared V3/V4 layouts or CAS blocks. Use exact
snapshot removal for those layouts and the separately bounded block-GC sweep
after its configured grace period. Do not run either cleanup family while a
replacement import or activation is in progress.
