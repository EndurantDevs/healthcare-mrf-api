# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected native attachment, persistent retention and request-local pinning."""

import base64
import re
from uuid import UUID

from sqlalchemy import text

from api.ptg_membership_contract import CONTRACT, SPEC, digest
from process import reference_family_archive as native
from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation, _seal_published_relation
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_physical_binding import _require_closed_local_custody


def attachment_name(identity):
    """Derive a native identifier containing the complete identity digest."""
    return _attachment_for_digest(digest(identity))


def _attachment_for_digest(key):
    return "pm_" + base64.b32encode(bytes.fromhex(key)).decode("ascii").lower().rstrip("=")


async def protected_owner(session):
    """Resolve the retention namespace owner and reject unsafe role custody."""
    row = (
        await session.execute(
            text(
                "SELECT owner.oid, NOT owner.rolcanlogin AND NOT owner.rolsuper "
                "AND NOT owner.rolcreatedb AND NOT owner.rolcreaterole AND NOT owner.rolreplication "
                "AND NOT owner.rolbypassrls AS safe FROM pg_namespace n "
                "JOIN pg_roles owner ON owner.oid=n.nspowner WHERE n.nspname='hp_snapshot_retention'"
            )
        )
    ).first()
    if row is not None and not row[1]:
        raise PTG2ManifestArtifactError("membership protected namespace owner differs")
    return None if row is None else int(row[0])


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


async def _active_view(session, identity, owner):
    name = attachment_name(identity)
    row = (
        await session.execute(
            text(
                "SELECT c.oid,c.relowner,c.relkind='v',c.relpersistence='p' FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='hp_snapshot_retention' AND c.relname=:name"
            ),
            {"name": name},
        )
    ).first()
    if row is None:
        return None
    view_oid, view_owner, kind, persistence = row
    if view_owner != owner or kind is not True or persistence is not True:
        raise PTG2ManifestArtifactError("membership active attachment custody differs")
    await _require_no_untrusted_mutation(session, [view_oid], owner)
    return view_oid


async def _attachment_storage(session, view_oid, owner):
    dependencies = (
        await session.execute(
            text(
                "SELECT DISTINCT c.oid,n.oid,n.nspname,c.relname FROM pg_rewrite r "
                "JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid "
                "AND d.refclassid='pg_class'::regclass JOIN pg_class c ON c.oid=d.refobjid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE r.ev_class=:view AND c.oid<>:view"
            ),
            {"view": view_oid},
        )
    ).all()
    schemas = {(int(dependency[1]), dependency[2]) for dependency in dependencies}
    if len(schemas) != 1 or {dependency[3] for dependency in dependencies} != set(SPEC.table_names):
        raise PTG2ManifestArtifactError("membership active attachment dependencies differ")
    schema_oid, schema = schemas.pop()
    prefix = "reference_family_archive_"
    try:
        if not schema.startswith(prefix):
            raise ValueError
        dataset = UUID(schema[len(prefix) :])
    except ValueError as error:
        raise PTG2ManifestArtifactError("membership index namespace differs") from error
    await native._lock_family(session, schema, SPEC.table_names, "ACCESS SHARE", nowait=True)
    ownership = await native.capture_model_family_stage_ownership(session, SPEC, dataset)
    if ownership.schema_oid != schema_oid or set(ownership.relation_oids) != {
        (dependency[3], int(dependency[0])) for dependency in dependencies
    }:
        raise PTG2ManifestArtifactError("membership index storage identity changed")
    await _require_closed_local_custody(session, ownership, owner)
    return ownership


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
    if header["identity"] != identity or re.fullmatch("[0-9a-f]{64}", header["content_digest"] or "") is None:
        raise PTG2ManifestArtifactError("membership index sealed identity differs")
    coverage_rows = [
        dict(row)
        for row in (
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
    """Only a protected native view authorizes an active index; comments do not."""
    owner = await protected_owner(session)
    if owner is None:
        return None
    view_oid = await _active_view(session, identity, owner)
    if view_oid is None:
        return None
    ownership = await _attachment_storage(session, view_oid, owner)
    header = await _sealed_header(session, ownership.schema_name, identity)
    pin = await source_pin(session, identity)
    return ownership, header, owner, view_oid, pin


async def seal(session, ownership, identity, *, reader_principals):
    """A native dependency view and source FK pin retain this exact immutable family."""
    owner = await native.protected_publisher_owner(session)
    owner_name = await session.scalar(
        text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:owner"), {"owner": owner}
    )
    schema = native._quoted(ownership.schema_name)
    for principal in reader_principals:
        await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {principal}"))
        await session.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {principal}"))
    await session.execute(text(f"ALTER SCHEMA {schema} OWNER TO {owner_name}"))
    # The creating heap owner remains observable to the native ACL closer.
    for _name, oid in ownership.relation_oids:
        await _seal_published_relation(session, oid, owner)
    await native.seal_model_family_storage(session, ownership, owner)
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
    view_oid = await session.scalar(
        text("SELECT to_regclass(:relation)::oid"), {"relation": "hp_snapshot_retention." + attachment_name(identity)}
    )
    await _seal_published_relation(session, view_oid, owner)
    for principal in reader_principals:
        await session.execute(text(f"GRANT SELECT ON hp_snapshot_retention.{name} TO {principal}"))
    await source_pin(session, identity, create=True)


def receipt(found):
    """Expose the sealed identity, accounting and retained storage coordinates."""
    ownership, header, owner, view_oid, pin = found
    return dict(
        header["identity"],
        state="ready",
        projection_id=digest(header["identity"]),
        content_digest=header["content_digest"],
        counts=header["counts"],
        code_coverage=header["counts"]["code_coverage"],
        active_attachment={"relation_oid": int(view_oid), "name": attachment_name(header["identity"])},
        source_pin=pin,
        storage={
            "dataset_id": str(ownership.dataset_id),
            "schema_oid": ownership.schema_oid,
            "relation_oids": [list(pair) for pair in ownership.relation_oids],
            "owner_oid": owner,
        },
    )


async def _require_no_references(session, schema, identity):
    from api.ptg2_serving import PTG2_SCHEMA
    from process.ptg_parts.source_snapshot_references import load_snapshot_reference_rows

    async def load_reference_rows(
        _schema, snapshot_id, *, table, selected_fields, reference_field="snapshot_id", order_fields=None
    ):
        """Read exact source-reference rows inside the held lifecycle fence."""
        return (
            (
                await session.execute(
                    text(
                        f"SELECT {selected_fields} FROM {schema}.{native._quoted(table)} "
                        f"WHERE {native._quoted(reference_field)}=:snapshot_id ORDER BY {order_fields or selected_fields}"
                    ),
                    {"snapshot_id": snapshot_id},
                )
            )
            .mappings()
            .all()
        )

    references = await load_snapshot_reference_rows(PTG2_SCHEMA, identity["snapshot_id"], load_reference_rows)
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

    expected = await _require_no_references(session, schema, identity)
    ownership = found[0]
    # A concurrent reader already holds ACCESS SHARE on the family; NOWAIT makes
    # this retirement roll back without detaching its view or pin under that reader.
    await native._lock_family(session, ownership.schema_name, SPEC.table_names, "ACCESS EXCLUSIVE", nowait=True)
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
