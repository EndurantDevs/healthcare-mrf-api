# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected native attachment, persistent retention and request-local pinning."""

import base64
import re
from uuid import UUID

from sqlalchemy import text

from api.ptg_membership_contract import CONTRACT, SPEC, digest, logical_source_identity
from process import reference_family_archive as native
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def attachment_name(identity):
    """Derive a native identifier containing the complete identity digest."""
    return _attachment_for_digest(digest(identity))


def _attachment_for_digest(key):
    return "pm_" + base64.b32encode(bytes.fromhex(key)).decode("ascii").lower().rstrip("=")


async def protected_owner(session):
    """Keep the legacy entrypoint without observing roles or catalog ownership."""
    return None


async def lock_identity(session, identity):
    """Serialize builds and retirement for exactly one source/code identity."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"), {"key": CONTRACT + ":" + digest(identity)}
    )


def _pin(identity):
    return {"owner_type": CONTRACT, "owner_id": digest(identity), "snapshot_id": identity["snapshot_id"]}


async def source_pin(session, identity, *, create=False):
    """Verify or create the durable source-snapshot retention reference."""
    from api.ptg2_serving import PTG2_SCHEMA

    schema = native._quoted(PTG2_SCHEMA)
    params = _pin(identity)
    if create:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_snapshot_pin (owner_type,owner_id,snapshot_id,reason) "
                "VALUES (:owner_type,:owner_id,:snapshot_id,:reason) ON CONFLICT DO NOTHING"
            ),
            params | {"reason": "active immutable provider membership projection"},
        )
    exists = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {schema}.ptg2_snapshot_pin "
            "WHERE owner_type=:owner_type AND owner_id=:owner_id AND snapshot_id=:snapshot_id)"
        ),
        params,
    )
    if exists is not True:
        raise PTG2ManifestArtifactError("membership index source retention pin is missing")
    return params


async def _active_view(session, identity, owner=None):
    name = attachment_name(identity)
    row = (
        await session.execute(
            text(
                "SELECT c.relkind='v',c.relpersistence='p' FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='hp_snapshot_retention' AND c.relname=:name"
            ),
            {"name": name},
        )
    ).first()
    if row is None:
        return None
    if tuple(row) != (True, True):
        raise PTG2ManifestArtifactError("membership active attachment layout differs")
    return "hp_snapshot_retention." + name


async def read_model_family_ownership(session, spec, dataset_id):
    """Name one installed model stage's tables for a read, without a namespace census."""

    native._require_transaction(session)
    native._require_model_family_spec(spec)
    schema_name = native.reference_family_stage_schema(dataset_id)
    relation_rows = (
        await session.execute(
            text(
                "SELECT namespace.oid, relation.relname, relation.oid FROM pg_catalog.pg_namespace AS namespace "
                "JOIN pg_catalog.pg_class AS relation ON relation.relnamespace=namespace.oid "
                "WHERE namespace.nspname=:schema_name AND relation.relname=ANY(CAST(:table_names AS text[])) "
                "AND relation.relkind='r'"
            ),
            {"schema_name": schema_name, "table_names": sorted(spec.table_names)},
        )
    ).all()
    if len(relation_rows) != len(spec.table_names):
        raise native.ReferenceFamilyArchiveError("reference family owned relation is missing")
    return native.ReferenceFamilyStageOwnership(
        spec.importer_id,
        dataset_id,
        schema_name,
        int(relation_rows[0][0]),
        tuple(sorted((str(name), int(oid)) for _schema_oid, name, oid in relation_rows)),
    )


async def attachment_storage(session, view, spec, *, membership_names=()):
    """Resolve the complete dependency family by its declared qualified names."""
    dependencies = (
        await session.execute(
            text(
                "SELECT DISTINCT n.nspname,c.relname FROM pg_rewrite r "
                "JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid "
                "AND d.refclassid='pg_class'::regclass JOIN pg_class c ON c.oid=d.refobjid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE r.ev_class=to_regclass(:view) AND c.oid<>r.ev_class"
            ),
            {"view": view},
        )
    ).all()
    actual_membership_names = {name for schema, name in dependencies if schema == "hp_snapshot_retention"}
    stored_pairs = {(schema, name) for schema, name in dependencies if schema != "hp_snapshot_retention"}
    schemas = {schema for schema, _name in stored_pairs}
    if (
        actual_membership_names != set(membership_names)
        or len(schemas) != 1
        or {name for _schema, name in stored_pairs} != set(spec.table_names)
    ):
        raise PTG2ManifestArtifactError("membership active attachment dependencies differ")
    schema = schemas.pop()
    prefix = "reference_family_archive_"
    try:
        if not schema.startswith(prefix):
            raise ValueError
        dataset = UUID(schema[len(prefix) :])
    except ValueError as error:
        raise PTG2ManifestArtifactError("membership index namespace differs") from error
    await native._lock_family(session, schema, spec.table_names, "ACCESS SHARE", nowait=True)
    ownership = await read_model_family_ownership(session, spec, dataset)
    if ownership.schema_name != schema or set(ownership.relation_names) != set(spec.table_names):
        raise PTG2ManifestArtifactError("membership index storage names changed")
    from api.plan_pricing_code_storage import has_locked_plain_heaps

    relations = [f"{native._quoted(schema)}.{native._quoted(name)}" for name in sorted(spec.table_names)]
    if not await has_locked_plain_heaps(session, relations):
        raise PTG2ManifestArtifactError("membership index storage is not plain")
    return ownership


async def _attachment_storage(session, view, owner=None):
    return await attachment_storage(session, view, SPEC)


async def freeze_attachment(session, view, reader_principals):
    """Close the one named attachment through the existing administrative freeze."""
    from process.provider_directory_generic_source_provision import _freeze_snapshot_relation

    namespace, name = view.split(".", 1)
    if namespace != "hp_snapshot_retention" or re.fullmatch(r"(?:pm|pc)_[a-z2-7]{52}", name) is None:
        raise PTG2ManifestArtifactError("projection attachment name is invalid")
    owner, readers = native.model_family_roles()
    driver = await native._native_model_copy_driver(session, ("execute", "fetch", "fetchval"))
    await _freeze_snapshot_relation(
        driver,
        namespace,
        {"relname": name, "relkind": "v"},
        native._quoted(owner),
        tuple(native._quoted(role) for role in readers),
    )
    for principal in reader_principals:
        await session.execute(
            text(f"GRANT SELECT ON {native._quoted(namespace)}.{native._quoted(name)} TO {principal}")
        )


async def _sealed_header(session, schema, identity):
    header = (
        (
            await session.execute(
                text(f'SELECT identity,content_digest,counts FROM "{schema}".ptg_membership_manifest WHERE singleton=1')
            )
        )
        .mappings()
        .one()
    )
    if (
        logical_source_identity(header["identity"]) != logical_source_identity(identity)
        or re.fullmatch("[0-9a-f]{64}", header["content_digest"] or "") is None
    ):
        raise PTG2ManifestArtifactError("membership index sealed identity differs")
    coverage_rows = [
        dict(coverage_row)
        for coverage_row in (
            await session.execute(
                text(
                    f'SELECT code_key,occurrence_count,set_count,set_digest FROM "{schema}".ptg_membership_code_coverage ORDER BY code_key'
                )
            )
        ).mappings()
    ]
    if coverage_rows != header["counts"].get("code_coverage"):
        raise PTG2ManifestArtifactError("membership index code coverage differs")
    return dict(header)


async def locate(session, identity):
    """Resolve the named attachment, complete data family and retention pin."""
    view = await _active_view(session, identity)
    if view is None:
        return None
    ownership = await _attachment_storage(session, view)
    header = await _sealed_header(session, ownership.schema_name, identity)
    pin = await source_pin(session, header["identity"])
    return ownership, header, None, view, pin


async def seal(session, ownership, identity, *, reader_principals):
    """A native dependency view and source pin retain the immutable family."""
    schema = native._quoted(ownership.schema_name)
    await native.seal_model_family_storage(session, ownership)
    for principal in reader_principals:
        await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {principal}"))
        await session.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {principal}"))
    name = native._quoted(attachment_name(identity))
    dependencies = ",".join(
        f"(SELECT count(*) FROM {schema}.{native._quoted(table)} WHERE false) AS dependency_{index}"
        for index, table in enumerate(SPEC.table_names)
        if table != "ptg_membership_manifest"
    )
    await session.execute(
        text(
            f"CREATE VIEW hp_snapshot_retention.{name} AS SELECT identity,content_digest,{dependencies} "
            f"FROM {schema}.ptg_membership_manifest WHERE singleton=1"
        )
    )
    await freeze_attachment(session, "hp_snapshot_retention." + attachment_name(identity), reader_principals)
    await source_pin(session, identity, create=True)


def receipt(found):
    """Expose the sealed identity, accounting and retained storage coordinates."""
    ownership, header, _legacy_owner, _view, pin = found
    return dict(
        logical_source_identity(header["identity"]),
        state="ready",
        projection_id=digest(logical_source_identity(header["identity"])),
        content_digest=header["content_digest"],
        counts=header["counts"],
        code_coverage=header["counts"]["code_coverage"],
        active_attachment={"schema_name": "hp_snapshot_retention", "name": attachment_name(header["identity"])},
        source_pin=pin,
        storage={
            "dataset_id": str(ownership.dataset_id),
            "schema_name": ownership.schema_name,
            "relation_names": sorted(ownership.relation_names),
        },
    )


async def _require_no_references(session, identity):
    from api.ptg2_serving import PTG2_SCHEMA
    from process.ptg_parts.source_snapshot_control import _TransactionExecutor
    from process.ptg_parts.source_snapshot_references import load_snapshot_reference_rows

    async def load_reference_rows(
        reference_schema, snapshot_id, *, table, selected_fields, reference_field="snapshot_id", order_fields=None
    ):
        """Read exact source-reference rows inside the held lifecycle fence."""
        return (
            (
                await session.execute(
                    text(
                        f"SELECT {selected_fields} FROM {native._quoted(reference_schema)}.{native._quoted(table)} "
                        f"WHERE {native._quoted(reference_field)}=:snapshot_id ORDER BY {order_fields or selected_fields}"
                    ),
                    {"snapshot_id": snapshot_id},
                )
            )
            .mappings()
            .all()
        )

    references = await load_snapshot_reference_rows(
        PTG2_SCHEMA, identity["snapshot_id"], load_reference_rows, _TransactionExecutor(session)
    )
    expected = _pin(identity)
    for kind, reference_rows in references.items():
        if kind == "pin":
            reference_rows = await _external_pins(session, identity, expected, reference_rows)
        if reference_rows:
            raise PTG2ManifestArtifactError("membership index source still has retention references")
    return expected


async def _external_pins(session, identity, expected, pin_rows):
    external_pins = []
    for pin_row in pin_rows:
        if pin_row["owner_type"] != CONTRACT:
            external_pins.append(pin_row)
        elif pin_row["owner_id"] != expected["owner_id"]:
            await _require_sibling_pin(session, identity, pin_row["owner_id"])
    return external_pins


async def _require_sibling_pin(session, identity, key):
    if re.fullmatch("[0-9a-f]{64}", key or "") is None:
        raise PTG2ManifestArtifactError("membership sibling retention pin is invalid")
    sibling = await session.scalar(
        text(f"SELECT identity FROM hp_snapshot_retention.{native._quoted(_attachment_for_digest(key))}")
    )
    if (
        not isinstance(sibling, dict)
        or digest(sibling) != key
        or sibling.get("snapshot_id") != identity["snapshot_id"]
        or sibling.get("source_key") != identity["source_key"]
        or await locate(session, sibling) is None
    ):
        raise PTG2ManifestArtifactError("membership sibling retention pin is invalid")


async def retire_unreferenced(session, identity):
    """Explicit retirement; active, rollback, release and external pins veto it."""
    from api.ptg2_serving import PTG2_SCHEMA
    from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock

    await native.protected_publisher_owner(session)
    await acquire_ptg2_lifecycle_lock(session)
    await lock_identity(session, identity)
    found = await locate(session, identity)
    has_attachment = found is not None
    if not has_attachment:
        return has_attachment
    schema = native._quoted(PTG2_SCHEMA)
    await session.execute(
        text(f"SELECT snapshot_id FROM {schema}.ptg2_snapshot WHERE snapshot_id=:snapshot_id FOR UPDATE"),
        {"snapshot_id": identity["snapshot_id"]},
    )

    expected = await _require_no_references(session, identity)
    ownership = found[0]
    # Native DROP waits only briefly for pinned readers, then rolls back the whole retirement.
    await native._lock_family(session, ownership.schema_name, SPEC.table_names, "ACCESS SHARE", nowait=True)
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text(f"DROP VIEW hp_snapshot_retention.{native._quoted(attachment_name(identity))} RESTRICT"))
    await session.execute(
        text(
            f"DELETE FROM {schema}.ptg2_snapshot_pin "
            "WHERE owner_type=:owner_type AND owner_id=:owner_id AND snapshot_id=:snapshot_id"
        ),
        expected,
    )
    await native.cleanup_model_family_stage(session, SPEC, ownership)
    return has_attachment
