# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected native publication and request-local retention for code capabilities."""

import base64
import re
from uuid import UUID

from sqlalchemy import text

from api import ptg_membership_storage as attachments
from api.plan_pricing_code_contract import CONTRACT, SPEC, ZIP_CONTRACT, identity_digest, logical_identity
from process import reference_family_archive as native
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def attachment_name(identity):
    """Name the exact release/procedure attachment independently of provider refresh."""
    anchor_by_field = {
        key: identity[key]
        for key in (
            "contract",
            "build_revision",
            "plan_release_id",
            "serving_revision_id",
            "binding_set_digest",
            "code_system",
            "code",
        )
    }
    if identity["contract"] == ZIP_CONTRACT:
        anchor_by_field["scope"] = {key: identity["scope"][key] for key in ("kind", "zip5", "zip_radius_miles")}
    return "pc_" + base64.b32encode(bytes.fromhex(identity_digest(anchor_by_field))).decode("ascii").lower().rstrip("=")


async def lock_identity(session, identity):
    """Serialize preparation and retirement for this immutable attachment."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": identity["contract"] + ":" + attachment_name(identity)},
    )


def _pins(identity):
    return tuple(
        {"owner_type": identity["contract"], "owner_id": identity_digest(identity), "snapshot_id": snapshot}
        for snapshot in sorted({source["source_identity"]["snapshot_id"] for source in identity["sources"]})
    )


async def source_pins(session, identity, *, create=False):
    """Require or create every immutable source pin owned by this capability."""
    from api.ptg2_serving import PTG2_SCHEMA

    schema = native._quoted(PTG2_SCHEMA)
    for pin in _pins(identity):
        if create:
            await session.execute(
                text(
                    f"INSERT INTO {schema}.ptg2_snapshot_pin (owner_type,owner_id,snapshot_id,reason) VALUES (:owner_type,:owner_id,:snapshot_id,'active immutable code pricing capability') ON CONFLICT DO NOTHING"
                ),
                pin,
            )
        present = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {schema}.ptg2_snapshot_pin WHERE owner_type=:owner_type AND owner_id=:owner_id AND snapshot_id=:snapshot_id)"
            ),
            pin,
        )
        if present is not True:
            raise PTG2ManifestArtifactError("code pricing source retention pin is missing")
    return _pins(identity)


def _membership_views(identity):
    """Retain only validated named membership dependencies, including old receipt editions."""
    names = set()
    for source in identity["sources"]:
        attachment = source["membership"]["active_attachment"]
        name = attachment["name"]
        if (
            re.fullmatch(r"pm_[a-z2-7]{52}", name or "") is None
            or attachment.get("schema_name", "hp_snapshot_retention") != "hp_snapshot_retention"
        ):
            raise PTG2ManifestArtifactError("code pricing membership dependency identity is invalid")
        names.add(name)
    return tuple(sorted(names))


async def _storage(session, view, identity):
    """Reuse the common locked named storage/dependency contract."""
    return await attachments.attachment_storage(session, view, SPEC, membership_names=_membership_views(identity))


_FAMILY_SQL = """SELECT DISTINCT n.nspname,c.relname FROM pg_rewrite r
 JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid AND d.refclassid='pg_class'::regclass
 JOIN pg_class c ON c.oid=d.refobjid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE r.ev_class=to_regclass(:view) AND c.oid<>r.ev_class AND n.nspname<>'hp_snapshot_retention'"""
_PLAIN_HEAPS_SQL = """SELECT count(*) FROM pg_catalog.pg_class c
 WHERE c.oid=ANY(CAST(:relations AS pg_catalog.regclass[])::pg_catalog.oid[])
 AND c.relkind='r' AND c.relpersistence='p'
 AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
 AND NOT (c.relrowsecurity OR c.relforcerowsecurity OR c.relispartition)
 AND NOT pg_catalog.row_security_active(c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid)"""


async def has_locked_plain_heaps(session, relations):
    """Share locks last until the read ends, so row security or rules cannot be added meanwhile.

    pg_class is read through the transaction snapshot, which can predate the lock under repeatable read;
    row_security_active reads the current catalog, so a row filter committed before the lock is refused too.
    """
    await session.execute(text(f"LOCK TABLE {','.join(relations)} IN ACCESS SHARE MODE"))
    return await session.scalar(text(_PLAIN_HEAPS_SQL), {"relations": list(relations)}) == len(relations)


async def _locate_by_name(session, identity):
    """Serve the stored capability the request anchor names; nothing is recomputed."""
    from api.ptg2_serving import PTG2_SCHEMA

    view = "hp_snapshot_retention." + attachment_name(identity)
    if await session.scalar(text("SELECT to_regclass(:view) IS NOT NULL"), {"view": view}) is not True:
        return None
    dependencies = (await session.execute(text(_FAMILY_SQL), {"view": view})).all()
    schema = dependencies[0][0] if dependencies else ""
    if re.fullmatch("reference_family_archive_[0-9a-f]{32}", schema) is None or set(map(tuple, dependencies)) != {
        (schema, name) for name in SPEC.table_names
    }:
        raise PTG2ManifestArtifactError("code pricing attachment dependencies differ")
    relations = [native._quoted(schema) + "." + native._quoted(name) for name in SPEC.table_names]
    if not await has_locked_plain_heaps(session, (*relations, native._quoted(PTG2_SCHEMA) + ".ptg2_snapshot_pin")):
        raise PTG2ManifestArtifactError("code pricing storage is not a plain heap")
    manifest = f"SELECT identity,content_digest,counts FROM ONLY {native._quoted(schema)}.plan_pricing_code_manifest WHERE singleton=1"
    header_by_field = dict((await session.execute(text(manifest))).mappings().one())
    try:
        is_same_anchor = attachment_name(header_by_field["identity"]) == attachment_name(identity)
    except (KeyError, TypeError) as error:
        raise PTG2ManifestArtifactError("code pricing sealed identity differs") from error
    if not is_same_anchor or re.fullmatch("[0-9a-f]{64}", header_by_field["content_digest"] or "") is None:
        raise PTG2ManifestArtifactError("code pricing sealed identity differs")
    await source_pins(session, header_by_field["identity"])
    return schema, header_by_field


async def locate(session, identity, *, allow_stale_provider=False, api_read=False):
    """Importers prove the closed native view; API reads serve the stored capability by name."""
    if type(api_read) is not bool:
        raise PTG2ManifestArtifactError("capability API read mode is invalid")
    if api_read:
        return await _locate_by_name(session, identity)
    view = "hp_snapshot_retention." + attachment_name(identity)
    if await session.scalar(text("SELECT to_regclass(:view) IS NOT NULL"), {"view": view}) is not True:
        return None
    ownership = await _storage(session, view, identity)
    header = (
        (
            await session.execute(
                text(
                    f"SELECT identity,content_digest,counts FROM {native._quoted(ownership.schema_name)}.plan_pricing_code_manifest WHERE singleton=1"
                )
            )
        )
        .mappings()
        .one()
    )
    if re.fullmatch("[0-9a-f]{64}", header["content_digest"] or "") is None:
        raise PTG2ManifestArtifactError("code pricing sealed identity differs")
    if logical_identity(header["identity"]) != logical_identity(identity):
        old, new = logical_identity(header["identity"]), logical_identity(identity)
        if identity["contract"] == ZIP_CONTRACT and old.get("provider_signature") != new.get("provider_signature"):
            old["scope"] = {key: scope_value for key, scope_value in old.get("scope", {}).items() if key != "geo_cells"}
            new["scope"] = {key: scope_value for key, scope_value in new["scope"].items() if key != "geo_cells"}
        old.pop("provider_signature", None)
        new.pop("provider_signature", None)
        if allow_stale_provider and old == new:
            await source_pins(session, header["identity"])
            return None
        raise PTG2ManifestArtifactError("code pricing sealed identity differs")
    await source_pins(session, header["identity"])
    return ownership, dict(header), None, view


async def seal(session, ownership, identity, *, reader_principals):
    """Atomically seal completed indexes/data and retain every source and membership family."""
    schema = native._quoted(ownership.schema_name)
    await native.seal_model_family_storage(session, ownership)
    for principal in reader_principals:
        await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {principal}"))
        await session.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {principal}"))
    dependencies = [
        f"(SELECT count(*) FROM {schema}.{native._quoted(name)} WHERE false) AS own_{index}"
        for index, name in enumerate(SPEC.table_names)
        if name != "plan_pricing_code_manifest"
    ]
    dependencies.extend(
        f"(SELECT count(*) FROM hp_snapshot_retention.{native._quoted(name)} WHERE false) AS membership_{index}"
        for index, name in enumerate(sorted(_membership_views(identity)))
    )
    name = native._quoted(attachment_name(identity))
    await session.execute(
        text(
            f"CREATE OR REPLACE VIEW hp_snapshot_retention.{name} AS SELECT identity,content_digest,{','.join(dependencies)} FROM {schema}.plan_pricing_code_manifest WHERE singleton=1"
        )
    )
    await attachments.freeze_attachment(
        session, "hp_snapshot_retention." + attachment_name(identity), reader_principals
    )
    await source_pins(session, identity, create=True)


def receipt(found):
    """Expose logical identity, accounting, and portable named storage coordinates."""
    ownership, header, _legacy_owner, _view = found
    identity = logical_identity(header["identity"])
    return {
        "contract": identity["contract"],
        "projection_id": identity_digest(identity),
        "state": "ready",
        "identity": identity,
        "content_digest": header["content_digest"],
        "counts": header["counts"],
        "active_attachment": {"schema_name": "hp_snapshot_retention", "name": attachment_name(identity)},
        "storage": {
            "dataset_id": str(ownership.dataset_id),
            "schema_name": ownership.schema_name,
            "relation_names": list(ownership.relation_names),
        },
    }


async def _unreferenced_source_schema(session, identity):
    from api.ptg2_serving import PTG2_SCHEMA
    from api.ptg_membership_contract import CONTRACT as MEMBERSHIP_CONTRACT
    from process.ptg_parts.source_snapshot_control import _TransactionExecutor
    from process.ptg_parts.source_snapshot_references import load_snapshot_reference_rows

    source_schema = native._quoted(PTG2_SCHEMA)
    await source_pins(session, identity)

    async def load(
        reference_schema, snapshot_id, *, table, selected_fields, reference_field="snapshot_id", order_fields=None
    ):
        """Read one ordered source-reference family within the locked snapshot."""
        return (
            (
                await session.execute(
                    text(
                        f"SELECT {selected_fields} FROM {native._quoted(reference_schema)}.{native._quoted(table)} WHERE {native._quoted(reference_field)}=:snapshot ORDER BY {order_fields or selected_fields}"
                    ),
                    {"snapshot": snapshot_id},
                )
            )
            .mappings()
            .all()
        )

    for pin in _pins(identity):
        await session.execute(
            text(f"SELECT snapshot_id FROM {source_schema}.ptg2_snapshot WHERE snapshot_id=:snapshot_id FOR UPDATE"),
            {"snapshot_id": pin["snapshot_id"]},
        )
        references = await load_snapshot_reference_rows(
            PTG2_SCHEMA, pin["snapshot_id"], load, _TransactionExecutor(session)
        )
        # Other derived families retain their own source data, not this family.
        references["pin"] = [
            reference_row
            for reference_row in references["pin"]
            if reference_row["owner_type"] not in {CONTRACT, ZIP_CONTRACT, MEMBERSHIP_CONTRACT}
        ]
        if any(references.values()):
            raise PTG2ManifestArtifactError("code pricing source still has retention references")
    return source_schema


async def _retirement_storage(session, ready):
    """Resolve the retained declared family by name before any cleanup mutation."""
    ownership = await native.capture_model_family_stage_ownership(session, SPEC, UUID(ready["storage"]["dataset_id"]))
    expected = ready["storage"]
    expected_schema = expected.get("schema_name", "reference_family_archive_" + ownership.dataset_id.hex)
    names = expected.get("relation_names")
    if names is None:
        names = [entry[0] for entry in expected.get("relation_oids", ())]
    if ownership.schema_name != expected_schema or tuple(ownership.relation_names) != tuple(names):
        raise PTG2ManifestArtifactError("code pricing retirement storage identity changed")
    return ownership


async def retire_unreferenced(session, ready):
    """Retire only a captured unreferenced generation; source/rollback/read pins veto it."""
    from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock

    identity = ready["identity"]
    if identity.get("contract") not in {CONTRACT, ZIP_CONTRACT} or ready.get("projection_id") != identity_digest(
        identity
    ):
        raise PTG2ManifestArtifactError("code pricing retirement identity differs")
    await acquire_ptg2_lifecycle_lock(session)
    await lock_identity(session, identity)
    ownership = await _retirement_storage(session, ready)
    schema = native._quoted(ownership.schema_name)
    header = (
        (
            await session.execute(
                text(f"SELECT identity,content_digest FROM {schema}.plan_pricing_code_manifest WHERE singleton=1")
            )
        )
        .mappings()
        .one()
    )
    has_matching_seal = (
        logical_identity(header["identity"]) == logical_identity(identity)
        and header["content_digest"] == ready["content_digest"]
    )
    if not has_matching_seal:
        raise PTG2ManifestArtifactError("code pricing retirement sealed identity differs")
    stored_identity = header["identity"]
    source_schema = await _unreferenced_source_schema(session, stored_identity)
    await native._lock_family(session, ownership.schema_name, SPEC.table_names, "ACCESS SHARE", nowait=True)
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    alias = "hp_snapshot_retention." + attachment_name(identity)
    if await session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": alias}) is True:
        dependencies = (await session.execute(text(_FAMILY_SQL), {"view": alias})).all()
        if any(namespace == ownership.schema_name for namespace, _name in dependencies):
            await session.execute(
                text(f"DROP VIEW hp_snapshot_retention.{native._quoted(attachment_name(identity))} RESTRICT")
            )
    for pin in _pins(stored_identity):
        await session.execute(
            text(
                f"DELETE FROM {source_schema}.ptg2_snapshot_pin WHERE owner_type=:owner_type AND owner_id=:owner_id AND snapshot_id=:snapshot_id"
            ),
            pin,
        )
    await native.cleanup_model_family_stage(session, SPEC, ownership)
    return has_matching_seal
