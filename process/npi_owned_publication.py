# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private NPI output heaps and ownership-sealed, manual native adoption."""

from dataclasses import dataclass
from datetime import date
from uuid import uuid4

from sqlalchemy import text

from process import mrf_address_publication as canonical
from process import npi_result_archive as archive
from process.npi_canonical_publication import validate_npi_canonical_publication_receipt
from process.npi_result_generation import (
    current_npi_relation_oids,
    read_npi_result_generation_authority,
    validate_npi_result_generation_authority,
)


async def precreate_npi_output(session, *, dataset_id):
    """Use native models without generating identifiers supplied by NPPES."""
    ownership = await archive.precreate_npi_restore(session, dataset_id=dataset_id, canonical=True)
    for name, _oid, table, column in ownership.sequence_oids:
        if (name, table, column) != ("npi_npi_seq", "npi", "npi"):
            raise archive.NpiResultArchiveError("NPI output sequence differs")
        schema = archive._quoted(ownership.schema_name)
        await session.execute(text(f'ALTER TABLE {schema}."npi" ALTER COLUMN "npi" DROP DEFAULT'))
        await session.execute(text(f'DROP SEQUENCE {schema}."npi_npi_seq" RESTRICT'))
    return await archive.capture_npi_stage_ownership(session, dataset_id=dataset_id, canonical=True)


async def require_npi_output_catalog(session, ownership, *, executable_roles, protected_owner_oid, expected_owner_oid):
    """Require the completed native model before ledger intake or frozen reads."""
    return await _is_output_catalog_valid(
        session, ownership, executable_roles, protected_owner_oid, expected_owner_oid, completed=True
    )


async def require_npi_output_heap_catalog(session, ownership, *, executable_roles, protected_owner_oid, builder_oid):
    """Require the exact empty-compiler heap shape before controller index completion."""
    return await _is_output_catalog_valid(
        session, ownership, executable_roles, protected_owner_oid, builder_oid, completed=False
    )


async def _require_output_owners(session, ownership, protected_owner_oid, expected_owner_oid):
    """Retain the protected namespace and the current phase's exact heap owner."""
    schema_owner = await session.scalar(
        text(
            "SELECT n.nspowner=CAST(:owner AS oid) AND NOT (r.rolcanlogin OR r.rolsuper OR r.rolcreaterole "
            "OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls) FROM pg_catalog.pg_namespace n "
            "JOIN pg_catalog.pg_roles r ON r.oid=n.nspowner WHERE n.oid=CAST(:schema AS oid)"
        ),
        {"owner": protected_owner_oid, "schema": ownership.schema_oid},
    )
    same_owner = await session.scalar(
        text(
            "SELECT bool_and(relowner=CAST(:owner AS oid)) FROM pg_catalog.pg_class WHERE oid=ANY(CAST(:oids AS oid[]))"
        ),
        {"owner": expected_owner_oid, "oids": [oid for _, oid in ownership.relation_oids]},
    )
    if schema_owner is not True or same_owner is not True:
        raise archive.NpiResultArchiveError("NPI output catalog owner differs")


async def _is_output_catalog_valid(
    session, ownership, executable_roles, protected_owner_oid, expected_owner_oid, *, completed
):
    """Authenticate exact native model shape before privileged output reads.

    An empty compiler-owned family supplies the independent model catalog. Its
    lifetime is one savepoint and it never becomes a producer or serving output.
    """
    archive._require_transaction(session)
    if (
        not isinstance(ownership, archive.NpiStageOwnership)
        or tuple(name for name, _ in ownership.relation_oids)
        != tuple(sorted(archive.npi_archive_names(canonical=True)))
        or ownership.sequence_oids
        or ownership.freeze_function_oid is not None
        or ownership.freeze_trigger_oids
        or ownership.freeze_catalog_versions
    ):
        raise archive.NpiResultArchiveError("NPI output catalog ownership differs")
    await archive.verify_npi_stage_ownership(session, ownership)
    await _require_output_owners(session, ownership, protected_owner_oid, expected_owner_oid)
    enum_oid = await canonical._canonical_enum_oid(session)
    oids = [oid for _, oid in ownership.relation_oids]
    await canonical.require_native_read_catalog(
        session, oids, allowed_type_oids=(enum_oid,), executable_roles=executable_roles
    )
    unsafe = await session.scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[]))) "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=ANY(CAST(:oids AS oid[]))) "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=ANY(CAST(:oids AS oid[])) "
            "AND (contype NOT IN ('p','u','n','c') OR NOT convalidated "
            "OR NOT coalesce((to_jsonb(pg_constraint)->>'conenforced')::boolean,true)))"
        ),
        {"oids": oids},
    )
    if unsafe is not False:
        raise archive.NpiResultArchiveError("NPI output executable catalog differs")
    observed_by_table = {
        name: await archive._npi_schema_identity(session, oid, ownership.schema_name, name)
        for name, oid in ownership.relation_oids
    }
    async with session.begin_nested() as compiler_scope:
        baseline = await precreate_npi_output(session, dataset_id=uuid4())
        if completed:
            await archive.complete_npi_restore(session, baseline)
        expected_by_table = {
            name: await archive._npi_schema_identity(session, oid, baseline.schema_name, name)
            for name, oid in baseline.relation_oids
        }
        await compiler_scope.rollback()
    if observed_by_table != expected_by_table:
        raise archive.NpiResultArchiveError("NPI output native model catalog differs")
    return True


async def _same_transaction(session, callback, *arguments):
    transaction = session.get_transaction()
    if transaction is None or not transaction.is_active:
        raise archive.NpiResultArchiveError("NPI publication transaction is unavailable")
    transaction_id = await session.scalar(text("SELECT pg_catalog.pg_current_xact_id()::text"))
    result = await callback(session, *arguments)
    if (
        session.get_transaction() is not transaction
        or not transaction.is_active
        or await session.scalar(text("SELECT pg_catalog.pg_current_xact_id()::text")) != transaction_id
    ):
        raise archive.NpiResultArchiveError("NPI publication transaction changed")
    return result


async def _require_inert_owned_stage(session, ownership, owner_oid):
    await archive._verify_stage_owner(session, ownership, owner_oid)
    protected_owner = await session.scalar(
        text(
            "SELECT NOT (rolcanlogin OR rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls) "
            "FROM pg_catalog.pg_roles WHERE oid=CAST(:owner AS oid)"
        ),
        {"owner": owner_oid},
    )
    unsafe = await session.scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger "
            "WHERE tgrelid=ANY(CAST(:oids AS oid[])) AND NOT tgisinternal) "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint "
            "WHERE conrelid=ANY(CAST(:oids AS oid[])) AND contype='f') "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=ANY(CAST(:oids AS oid[])))"
        ),
        {"oids": [oid for _name, oid in ownership.relation_oids]},
    )
    if protected_owner is not True or unsafe is not False:
        raise archive.NpiResultArchiveError("NPI output custody differs")


async def _publish_generation(session, incumbent, previous, publication):
    """Advance real local history only after the output relations have been adopted."""
    schema = archive._quoted(incumbent.schema_name)
    oids = await current_npi_relation_oids(session, schema_name=incumbent.schema_name)
    authority_row = (
        (
            await session.execute(
                text(
                    f"UPDATE {schema}.npi_result_generation SET local_generation=local_generation+1,"
                    "origin_lineage_id=local_lineage_id,origin_generation=local_generation+1,"
                    "published_at=clock_timestamp(),relation_oids=CAST(:oids AS bigint[]),"
                    "canonical_publication_ref=:publication_ref,canonical_publication_generation=:publication_generation,"
                    "canonical_chain_ref=:chain_ref,canonical_import_date=CAST(:import_date AS date) "
                    "WHERE singleton IS TRUE AND local_lineage_id=CAST(:lineage AS uuid) "
                    "AND local_generation=:generation AND local_generation < 9223372036854775807 "
                    "RETURNING singleton,local_lineage_id,local_generation,origin_lineage_id,origin_generation,"
                    "published_at,relation_oids,canonical_publication_ref,canonical_publication_generation,"
                    "canonical_chain_ref,canonical_import_date"
                ),
                {
                    "oids": list(oids),
                    "lineage": previous.local_lineage_id,
                    "generation": previous.local_generation,
                    "publication_ref": publication.publication_ref,
                    "publication_generation": publication.publication_generation,
                    "chain_ref": publication.chain_ref,
                    "import_date": date.fromisoformat(publication.import_date),
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if authority_row is None:
        raise archive.NpiResultArchiveError("NPI local publication generation changed")
    return validate_npi_result_generation_authority(authority_row)


@dataclass(frozen=True)
class NpiOwnershipPublication:
    """Required constructor-bound local capabilities; incoming JSON grants none."""

    cutover: archive.NpiCutoverAuthority
    verify_custody: object
    attach_canonical: object
    on_activated: object
    npi_alias_authority: object | None = None


def _activation_contract(ownership, manifest, incumbent, validation_receipt, authority):
    """Keep ownership-only manual output distinct from every legacy archive path."""
    if type(authority) is not NpiOwnershipPublication or not all(
        callable(callback)
        for callback in (authority.verify_custody, authority.attach_canonical, authority.on_activated)
    ):
        raise archive.NpiResultArchiveError("NPI accepted publication capabilities are unavailable")
    cutover = authority.cutover
    if not isinstance(ownership, archive.NpiStageOwnership) or not isinstance(incumbent, archive.NpiIncumbent):
        raise archive.NpiResultArchiveError("NPI activation ownership is invalid")
    if (
        not isinstance(cutover, archive.NpiCutoverAuthority)
        or cutover.authority != "manual"
        or cutover.expected_stage_owner_oid != cutover.sealed_owner_oid
        or ownership.sequence_oids
        or ownership.freeze_function_oid is not None
        or ownership.freeze_trigger_oids
        or ownership.freeze_catalog_versions
    ):
        raise archive.NpiResultArchiveError("NPI ownership-only activation authority differs")
    validated = archive.validate_npi_result_manifest(manifest)
    validation = archive.validate_npi_validation_receipt(validation_receipt)
    names = archive.npi_archive_names(canonical=True)
    if (
        validated.contract not in archive.MODEL_CONTRACTS
        or validated.capture_authority != "legacy-manual"
        or validated.source_serving_generation is not None
        or validated.canonical_provenance is not None
        or tuple(name for name, _oid in incumbent.relation_oids) != names
        or tuple(name for name, _oid in ownership.relation_oids) != tuple(sorted(names))
    ):
        raise archive.NpiResultArchiveError("NPI direct output contract differs")
    archive._validate_cutover_bindings(ownership, validated, validation, cutover)
    return validated, validation, names


async def activate_ownership_sealed_npi_stage(
    session, *, ownership, manifest, incumbent, validation_receipt, authority
):
    """Adopt prepared output only with accepted custody and canonical capabilities.

    This path never performs a legacy live canonical merge or installs hooks.
    The caller retains the transaction, incumbent CAS and original native run.
    """
    archive._require_transaction(session)
    validated, validation, names = _activation_contract(ownership, manifest, incumbent, validation_receipt, authority)
    await archive._alias_arguments(session, validated, authority.npi_alias_authority)
    cutover = authority.cutover
    await archive._lock_and_verify_activation(session, ownership, incumbent)
    await _require_inert_owned_stage(session, ownership, cutover.sealed_owner_oid)
    await _same_transaction(session, authority.verify_custody, ownership, validated, incumbent, validation, cutover)
    previous = await read_npi_result_generation_authority(session, schema_name=incumbent.schema_name, lock=True)
    await archive._preserve_npi_input_reads(session, ownership, incumbent, cutover.sealed_owner_oid)
    predecessor = await archive._rotate_relations(session, ownership, incumbent)
    pairs = await archive._relation_pairs(session, incumbent.schema_name, names)
    if tuple(sorted(pairs)) != ownership.relation_oids or any(oid is None for _name, oid in pairs):
        raise archive.NpiResultArchiveError("NPI adopted output identity differs")
    canonical = validate_npi_canonical_publication_receipt(
        await _same_transaction(session, authority.attach_canonical, ownership, validated, incumbent, validation)
    )
    counts_by_table = {table.table_name: table.row_count for table in validation.tables}
    if canonical.relation_oids != tuple(
        dict(pairs)[name] for name in archive.RELATION_NAMES
    ) or canonical.row_counts != tuple(counts_by_table[name] for name in archive.RELATION_NAMES):
        raise archive.NpiResultArchiveError("NPI canonical attachment output differs")
    generation = await _publish_generation(session, incumbent, previous, canonical)
    receipt = archive.NpiActivationReceipt(
        pairs, incumbent.relation_oids, predecessor, validation.tables, generation, canonical._asdict()
    )
    await _same_transaction(session, authority.on_activated, receipt)
    return receipt
