# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected native publication and request-local retention for code capabilities."""

import base64
import re
from uuid import UUID

from sqlalchemy import text

from api.plan_pricing_code_contract import CONTRACT, SPEC, identity_digest
from api.ptg_membership_storage import protected_owner
from process import reference_family_archive as native
from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation, _seal_published_relation
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_physical_binding import _require_closed_local_custody


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
    return "pc_" + base64.b32encode(bytes.fromhex(identity_digest(anchor_by_field))).decode("ascii").lower().rstrip("=")


async def lock_identity(session, identity):
    """Serialize preparation and retirement for this immutable attachment."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": CONTRACT + ":" + attachment_name(identity)},
    )


def _pins(identity):
    return tuple(
        {"owner_type": CONTRACT, "owner_id": identity_digest(identity), "snapshot_id": snapshot}
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
    view_oid_by_name = {}
    for source in identity["sources"]:
        attachment = source["membership"]["active_attachment"]
        name, oid = attachment["name"], attachment["relation_oid"]
        if re.fullmatch(r"pm_[a-z2-7]{52}", name or "") is None or type(oid) is not int or not 0 < oid < 2**32:
            raise PTG2ManifestArtifactError("code pricing membership dependency identity is invalid")
        if name in view_oid_by_name and view_oid_by_name[name] != oid:
            raise PTG2ManifestArtifactError("code pricing membership dependency identity collides")
        view_oid_by_name[name] = oid
    return view_oid_by_name


async def _storage(session, view_oid, owner, identity):
    dependencies = (
        await session.execute(
            text(
                "SELECT DISTINCT c.oid,n.oid,n.nspname,c.relname FROM pg_rewrite r JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid AND d.refclassid='pg_class'::regclass JOIN pg_class c ON c.oid=d.refobjid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE r.ev_class=:view AND c.oid<>:view"
            ),
            {"view": view_oid},
        )
    ).all()
    membership_oid_by_name = {
        dependency_row[3]: int(dependency_row[0])
        for dependency_row in dependencies
        if dependency_row[2] == "hp_snapshot_retention"
    }
    storage_relations = [
        dependency_row for dependency_row in dependencies if dependency_row[2] != "hp_snapshot_retention"
    ]
    if membership_oid_by_name != _membership_views(identity) or {
        dependency_row[3] for dependency_row in storage_relations
    } != set(SPEC.table_names):
        raise PTG2ManifestArtifactError("code pricing attachment dependencies differ")
    schemas = {(int(dependency_row[1]), dependency_row[2]) for dependency_row in storage_relations}
    if len(schemas) != 1:
        raise PTG2ManifestArtifactError("code pricing storage family differs")
    schema_oid, schema = schemas.pop()
    prefix = "reference_family_archive_"
    try:
        if not schema.startswith(prefix):
            raise ValueError
        dataset = UUID(schema[len(prefix) :])
    except ValueError as error:
        raise PTG2ManifestArtifactError("code pricing namespace differs") from error
    await native._lock_family(session, schema, SPEC.table_names, "ACCESS SHARE", nowait=True)
    ownership = await native.capture_model_family_stage_ownership(session, SPEC, dataset)
    if ownership.schema_oid != schema_oid or set(ownership.relation_oids) != {
        (dependency_row[3], int(dependency_row[0])) for dependency_row in storage_relations
    }:
        raise PTG2ManifestArtifactError("code pricing storage identity changed")
    await _require_closed_local_custody(session, ownership, owner)
    return ownership


async def locate(session, identity, *, allow_stale_provider=False):
    """An exact closed native view, never a comment or caller receipt, authorizes reads."""
    owner = await protected_owner(session)
    if owner is None:
        return None
    attachment_record = (
        await session.execute(
            text(
                "SELECT c.oid,c.relowner,c.relkind='v',c.relpersistence='p' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='hp_snapshot_retention' AND c.relname=:name"
            ),
            {"name": attachment_name(identity)},
        )
    ).first()
    if attachment_record is None:
        return None
    if attachment_record[1] != owner or attachment_record[2:] != (True, True):
        raise PTG2ManifestArtifactError("code pricing attachment custody differs")
    view_oid = int(attachment_record[0])
    await _require_no_untrusted_mutation(session, [view_oid], owner)
    ownership = await _storage(session, view_oid, owner, identity)
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
    if header["identity"] != identity:
        old, new = dict(header["identity"]), dict(identity)
        old.pop("provider_signature", None)
        new.pop("provider_signature", None)
        if allow_stale_provider and old == new:
            await source_pins(session, header["identity"])
            return None
        raise PTG2ManifestArtifactError("code pricing sealed identity differs")
    await source_pins(session, identity)
    return ownership, dict(header), owner, view_oid


async def seal(session, ownership, identity, *, reader_principals):
    """Atomically seal completed indexes/data and retain every source and membership family."""
    owner = await native.protected_publisher_owner(session)
    owner_name = await session.scalar(
        text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:owner"), {"owner": owner}
    )
    schema = native._quoted(ownership.schema_name)
    for principal in reader_principals:
        await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {principal}"))
        await session.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {principal}"))
    await session.execute(text(f"ALTER SCHEMA {schema} OWNER TO {owner_name}"))
    for _name, oid in ownership.relation_oids:
        await _seal_published_relation(session, oid, owner)
    await native.seal_model_family_storage(session, ownership, owner)
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
    oid = await session.scalar(
        text("SELECT to_regclass(:name)::oid"), {"name": "hp_snapshot_retention." + attachment_name(identity)}
    )
    await _seal_published_relation(session, oid, owner)
    for principal in reader_principals:
        await session.execute(text(f"GRANT SELECT ON hp_snapshot_retention.{name} TO {principal}"))
    await source_pins(session, identity, create=True)


def receipt(found):
    """Expose the authenticated ready identity and captured native storage coordinates."""
    ownership, header, owner, oid = found
    return {
        "contract": CONTRACT,
        "projection_id": identity_digest(header["identity"]),
        "state": "ready",
        "identity": header["identity"],
        "content_digest": header["content_digest"],
        "counts": header["counts"],
        "active_attachment": {"name": attachment_name(header["identity"]), "relation_oid": oid},
        "storage": {
            "dataset_id": str(ownership.dataset_id),
            "schema_oid": ownership.schema_oid,
            "relation_oids": [list(row) for row in ownership.relation_oids],
            "owner_oid": owner,
        },
    }


async def _unreferenced_source_schema(session, identity):
    from api.ptg2_serving import PTG2_SCHEMA
    from api.ptg_membership_contract import CONTRACT as MEMBERSHIP_CONTRACT
    from process.ptg_parts.source_snapshot_references import load_snapshot_reference_rows

    source_schema = native._quoted(PTG2_SCHEMA)
    await source_pins(session, identity)

    async def load(_schema, snapshot_id, *, table, selected_fields, reference_field="snapshot_id", order_fields=None):
        """Read one ordered source-reference family within the locked snapshot."""
        return (
            (
                await session.execute(
                    text(
                        f"SELECT {selected_fields} FROM {source_schema}.{native._quoted(table)} WHERE {native._quoted(reference_field)}=:snapshot ORDER BY {order_fields or selected_fields}"
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
        references = await load_snapshot_reference_rows(PTG2_SCHEMA, pin["snapshot_id"], load)
        # Other derived families retain their own source data, not this family.
        references["pin"] = [
            reference_row
            for reference_row in references["pin"]
            if reference_row["owner_type"] not in {CONTRACT, MEMBERSHIP_CONTRACT}
        ]
        if any(references.values()):
            raise PTG2ManifestArtifactError("code pricing source still has retention references")
    return source_schema


async def retire_unreferenced(session, ready):
    """Retire only a captured unreferenced generation; source/rollback/read pins veto it."""
    from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock

    identity = ready["identity"]
    if identity.get("contract") != CONTRACT or ready.get("projection_id") != identity_digest(identity):
        raise PTG2ManifestArtifactError("code pricing retirement identity differs")
    owner = await native.protected_publisher_owner(session)
    await acquire_ptg2_lifecycle_lock(session)
    await lock_identity(session, identity)
    ownership = await native.capture_model_family_stage_ownership(session, SPEC, UUID(ready["storage"]["dataset_id"]))
    if (
        ownership.schema_oid != ready["storage"]["schema_oid"]
        or [list(reference_row) for reference_row in ownership.relation_oids] != ready["storage"]["relation_oids"]
        or owner != ready["storage"]["owner_oid"]
    ):
        raise PTG2ManifestArtifactError("code pricing retirement storage identity changed")
    await _require_closed_local_custody(session, ownership, owner)
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
    has_matching_seal = header["identity"] == identity and header["content_digest"] == ready["content_digest"]
    if not has_matching_seal:
        raise PTG2ManifestArtifactError("code pricing retirement sealed identity differs")
    source_schema = await _unreferenced_source_schema(session, identity)
    await native._lock_family(session, ownership.schema_name, SPEC.table_names, "ACCESS EXCLUSIVE", nowait=True)
    alias = "hp_snapshot_retention." + attachment_name(identity)
    oid = await session.scalar(text("SELECT to_regclass(:name)::oid"), {"name": alias})
    if oid is not None:
        dependency = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_rewrite r JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid AND d.refclassid='pg_class'::regclass JOIN pg_class c ON c.oid=d.refobjid WHERE r.ev_class=:view AND c.relnamespace=:schema)"
            ),
            {"view": oid, "schema": ownership.schema_oid},
        )
        if dependency:
            await _require_no_untrusted_mutation(session, [oid], owner)
            await session.execute(
                text(f"DROP VIEW hp_snapshot_retention.{native._quoted(attachment_name(identity))} RESTRICT")
            )
    for pin in _pins(identity):
        await session.execute(
            text(
                f"DELETE FROM {source_schema}.ptg2_snapshot_pin WHERE owner_type=:owner_type AND owner_id=:owner_id AND snapshot_id=:snapshot_id"
            ),
            pin,
        )
    await native.cleanup_model_family_stage(session, SPEC, ownership)
    return has_matching_seal
