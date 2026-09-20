# Static TIGER native publication

The explicit `tiger` replacement family owns only `tiger.zip_state` and
`tiger.zcta5`, in that generation order, plus the sequence owned by `zcta5.gid`.
It does not belong to `geo-census`, which updates ZIP-level Census profiles and
does not publish these two relations.

The model contract preserves the composite primary keys `(zip, stusps)` and
`(zcta5ce, statefp)`, the integer `gid` owned sequence, double-precision area
fields, and native `geometry(MultiPolygon,4269)` with its GiST index. PostGIS must
already be installed. Archive preparation creates independent model-complete
tables, indexes and owned sequences; it copies native values and rebases sequence
state from the frozen rows. It never copies a default pointing to the source
sequence or coerces geometry to text.

Generation authority remains in the configured application schema's
`reference_family_result_generation` table. Only the `tiger` importer routes its
ledger there while addressing serving relations in the fixed `tiger` schema.
Migration adds a generation-less row without changing TIGER ownership, grants,
tables, or installed data. Before explicit bootstrap, automatic generation
capture is unavailable; legacy tables are not silently adopted.

## Explicit bootstrap or replacement

A trusted publisher may explicitly review and republish installed static data:

1. Use `prepare_reference_family_archive_source` for `tiger`, with explicit
   reviewed source metadata, and persist the exact prepared ownership.
2. Export a custom PostgreSQL archive with `export_prepared_reference_family_archive`.
   Restore data into `precreate_reference_family_restore` tables and indexes.
3. Freeze the stage under the protected publisher owner, apply the approved
   serving read grants, capture the incumbent OIDs, and produce the existing
   `prepare_reference_family_activation` validation receipt.
4. In one caller-owned transaction, invoke
   `process.tiger_result_generation.publish_tiger_generation` with manual
   cutover authority and no adopted source generation. Persist its completion
   receipt in that same transaction and report success only after commit.

The callable performs the validated real table/schema swap, retains the exact
predecessor, and advances a new local generation bound to the new serving OIDs.
Its `tiger-local-publication.v1` receipt includes package and dataset identity,
activation/predecessor details, and the recorded generation. Failed activation
or a failed enclosing transaction leaves the predecessor/current generation
unchanged. A consumed stage cannot be published again; orchestration must retain
the completion receipt instead of retrying a committed publication blindly.

This operation still requires the protected relation owner for table/index and
sequence DDL. Application-role ownership of the ledger is not publication
permission, and no TIGER CREATE or ownership grants are added. Future automatic
exports must capture the recorded serving generation in the existing source
metadata callback under the same snapshot and family locks. Destination
controller admission, retention relocation, and committed rollback orchestration
are separate integrations; this source contract does not enable them.
