# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read named capabilities and maintain their native builder lifecycle."""

import base64
import copy
import re
from contextlib import asynccontextmanager, nullcontext
from uuid import UUID

from sqlalchemy import text

from api.network_read_execution import qualified_relation_sql
from api.ptg_membership_contract import CONTRACT, SPEC, digest
from process import reference_family_archive as native
from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation, _seal_published_relation
from process.network_compact_layout import CompactAddressRelation
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_physical_binding import _require_closed_local_custody

_HEX = re.compile(r"[0-9a-f]{64}\Z")


def _api_identity(identity):
    simple_fields = {"source_key", "snapshot_id", "code_system", "code"}
    full_fields = simple_fields | {"contract", "physical_binding", "graph_root", "finalizer", "source_storage"}
    if (
        type(identity) is not dict
        or set(identity) not in (simple_fields, full_fields, simple_fields - {"source_key"})
        or type(identity.get("snapshot_id")) is not str
    ):
        raise PTG2ManifestArtifactError("membership read identity is invalid")
    if set(identity) == full_fields and identity["contract"] != CONTRACT:
        raise PTG2ManifestArtifactError("membership read contract differs")
    return identity


def _logical_source_identity(identity, *, reference=None):
    """Project only the supported saved source inventory to qualified names."""
    try:
        return _decode_source_identity(identity, reference=reference)
    except (KeyError, TypeError, ValueError) as error:
        raise PTG2ManifestArtifactError("membership saved source identity is invalid") from error


def _named_source_binding(binding):
    """Decode either saved binding format to its validated named inventory."""
    from dataclasses import asdict

    from process.ptg_parts.ptg2_physical_binding import PTG2PhysicalBinding

    common_fields = {
        "contract",
        "snapshot_id",
        "destination_layout_key",
        "payload_snapshot_id",
        "payload_snapshot_key",
        "dataset_id",
    }
    legacy_fields = {"schema_oid", "owner_oid", "relation_oids", "sequence_oids"}
    named_fields = {"schema_name", "relations", "sequences"}
    if type(binding) is not dict or set(binding) not in (common_fields | legacy_fields, common_fields | named_fields):
        raise PTG2ManifestArtifactError("membership saved source binding differs")
    binding_by_field = {name: binding[name] for name in common_fields}
    binding_by_field["dataset_id"] = UUID(binding_by_field["dataset_id"])
    if set(binding) == common_fields | legacy_fields:
        schema = native.reference_family_stage_schema(binding_by_field["dataset_id"])
        for key in ("schema_oid", "owner_oid"):
            if type(binding[key]) is not int or not 0 < binding[key] < 2**32:
                raise PTG2ManifestArtifactError("membership saved source binding differs")
        relations = binding["relation_oids"]
        sequences = binding["sequence_oids"]
        if (
            type(relations) is not list
            or type(sequences) is not list
            or any(
                type(entry) is not list or len(entry) != 2 or type(entry[1]) is not int or not 0 < entry[1] < 2**32
                for entry in relations
            )
            or any(
                type(entry) is not list or len(entry) != 4 or type(entry[1]) is not int or not 0 < entry[1] < 2**32
                for entry in sequences
            )
        ):
            raise PTG2ManifestArtifactError("membership saved source inventory differs")
        binding_by_field.update(
            schema_name=schema,
            relations=tuple((schema, entry[0]) for entry in relations),
            sequences=tuple((schema, entry[0], entry[2], entry[3]) for entry in sequences),
        )
    else:
        binding_by_field.update(
            schema_name=binding["schema_name"],
            relations=tuple(map(tuple, binding["relations"])),
            sequences=tuple(map(tuple, binding["sequences"])),
        )
    projected = PTG2PhysicalBinding(**binding_by_field)
    binding_by_field = asdict(projected) | {"dataset_id": str(projected.dataset_id)}
    for key in ("relations", "sequences"):
        binding_by_field[key] = list(map(list, binding_by_field[key]))
    return binding_by_field


def _named_source_storage(storage, schema, *, reference):
    """Decode a saved source storage receipt without changing its logical scope."""
    legacy_fields = {"database_oid", "schema_oid", "relation_oids", "attestation_digest"}
    named_fields = {"schema_name", "relations"}
    if type(storage) is not dict or set(storage) not in (legacy_fields, named_fields):
        raise PTG2ManifestArtifactError("membership saved source storage differs")
    if set(storage) == legacy_fields:
        expected = None if reference is None else reference.get("source_storage")
        schema = schema or (expected.get("schema_name") if type(expected) is dict else None)
        if (
            schema is None
            or not _HEX.fullmatch(storage["attestation_digest"] or "")
            or any(
                type(storage[key]) is not int or not 0 < storage[key] < 2**32 for key in ("database_oid", "schema_oid")
            )
            or type(storage["relation_oids"]) is not list
            or any(
                type(entry) is not list or len(entry) != 2 or type(entry[1]) is not int or not 0 < entry[1] < 2**32
                for entry in storage["relation_oids"]
            )
        ):
            raise PTG2ManifestArtifactError("membership saved source storage differs")
        relations = [[schema, entry[0]] for entry in storage["relation_oids"]]
    else:
        schema, relations = storage["schema_name"], storage["relations"]
    if (
        type(relations) is not list
        or any(type(entry) is not list or len(entry) != 2 for entry in relations)
        or len({tuple(entry) for entry in relations}) != len(relations)
    ):
        raise PTG2ManifestArtifactError("membership source named inventory differs")
    for namespace, name in relations:
        qualified_relation_sql(namespace, name)
        if namespace != schema:
            raise PTG2ManifestArtifactError("membership source named inventory differs")
    return {"schema_name": schema, "relations": sorted(relations)}


def _decode_source_identity(identity, *, reference):
    """Decode a saved source identity while preserving its validated named inputs."""
    identity = copy.deepcopy(_api_identity(identity))
    binding = identity.get("physical_binding")
    schema = None
    if binding is not None:
        identity["physical_binding"] = _named_source_binding(binding)
        schema = identity["physical_binding"]["schema_name"]
    storage = identity.get("source_storage")
    if storage is not None:
        identity["source_storage"] = _named_source_storage(storage, schema, reference=reference)
    return identity


async def _named_dependencies(session, name):
    """Read the qualified inputs of the existing attachment view, without identity census."""
    return (
        (
            await _read_metadata(
                session,
                text(
                    "SELECT DISTINCT source.nspname AS schema_name,dependency.relname AS table_name "
                    "FROM pg_catalog.pg_class attachment JOIN pg_catalog.pg_namespace home ON home.oid=attachment.relnamespace "
                    "JOIN pg_catalog.pg_rewrite rule ON rule.ev_class=attachment.oid "
                    "JOIN pg_catalog.pg_depend link ON link.classid='pg_catalog.pg_rewrite'::regclass "
                    "AND link.objid=rule.oid AND link.refclassid='pg_catalog.pg_class'::regclass "
                    "JOIN pg_catalog.pg_class dependency ON dependency.oid=link.refobjid "
                    "JOIN pg_catalog.pg_namespace source ON source.oid=dependency.relnamespace "
                    "WHERE home.nspname='hp_snapshot_retention' AND attachment.relname=:name "
                    "AND (source.nspname,dependency.relname)<>('hp_snapshot_retention',:name) "
                    "ORDER BY source.nspname,dependency.relname"
                ),
                {"name": name},
                api_read=True,
            )
        )
        .mappings()
        .all()
    )


async def _named_family(session, name, spec, manifest, *, memberships=()):
    rows = await _named_dependencies(session, name)
    coordinates = [(row["schema_name"], row["table_name"]) for row in rows]
    expected_memberships = {("hp_snapshot_retention", value) for value in memberships}
    actual_memberships = {pair for pair in coordinates if pair[0] == "hp_snapshot_retention"}
    family_relations = [pair for pair in coordinates if pair[0] != "hp_snapshot_retention"]
    schemas = {schema for schema, _name in family_relations}
    if (
        len(coordinates) != len(set(coordinates))
        or actual_memberships != expected_memberships
        or len(schemas) != 1
        or {table for _schema, table in family_relations} != set(spec.table_names)
    ):
        raise PTG2ManifestArtifactError("capability named attachment inventory differs")
    schema = schemas.pop()
    try:
        dataset = UUID(schema.removeprefix("reference_family_archive_"))
        if schema != native.reference_family_stage_schema(dataset):
            raise ValueError
        for namespace, table in coordinates:
            qualified_relation_sql(namespace, table)
    except (TypeError, ValueError) as error:
        raise PTG2ManifestArtifactError("capability named archive namespace differs") from error
    return CompactAddressRelation(schema, manifest)


def _named_storage_receipt(relation, spec):
    return {
        "dataset_id": str(UUID(relation.schema_name.removeprefix("reference_family_archive_"))),
        "schema_name": relation.schema_name,
        "relations": [[relation.schema_name, name] for name in spec.table_names],
    }


def attachment_name(identity):
    """Derive a native identifier containing the complete identity digest."""
    return _attachment_for_digest(digest(identity))


def _attachment_for_digest(key):
    return "pm_" + base64.b32encode(bytes.fromhex(key)).decode("ascii").lower().rstrip("=")


async def _api_read_snapshot(session):
    """Require an active read-only repeatable SQLAlchemy transaction."""
    transaction = session.get_transaction()
    if session.in_transaction() is not True or transaction is None:
        raise PTG2ManifestArtifactError("capability API read snapshot is unavailable")
    modes = (
        await _read_metadata(
            session,
            text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"),
            api_read=True,
        )
    ).one()
    if tuple(modes) not in (("repeatable read", "on"), ("serializable", "on")):
        raise PTG2ManifestArtifactError("capability API read snapshot is unavailable")
    return transaction


async def _read_metadata(session, statement, parameters=None, *, api_read=False):
    transaction = session.get_transaction() if api_read else None
    if api_read and (transaction is None or session.in_transaction() is not True):
        raise PTG2ManifestArtifactError("capability API read snapshot is unavailable")
    result = await session.execute(statement) if parameters is None else await session.execute(statement, parameters)
    if api_read and (session.get_transaction() is not transaction or session.in_transaction() is not True):
        result.close()
        raise PTG2ManifestArtifactError("capability API read transaction changed")
    return result


@asynccontextmanager
async def _api_storage_read(session, transaction):
    """Keep capability reads within the selected logical transaction."""
    if session.get_transaction() is not transaction or session.in_transaction() is not True:
        raise PTG2ManifestArtifactError("capability API read transaction changed")
    yield
    if session.get_transaction() is not transaction or session.in_transaction() is not True:
        raise PTG2ManifestArtifactError("capability API read transaction changed")


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


async def _api_source_pin(session, identity, *, projection_id=None):
    """Read the exact retention predicate inside the authenticated API scope."""
    from api.ptg2_serving import PTG2_SCHEMA

    pin = (
        _pin(identity)
        if projection_id is None
        else {"owner_type": CONTRACT, "owner_id": projection_id, "snapshot_id": identity["snapshot_id"]}
    )
    if not _HEX.fullmatch(pin["owner_id"] or ""):
        raise PTG2ManifestArtifactError("membership source retention edition differs")
    present = (
        await _read_metadata(
            session,
            text(
                f"SELECT EXISTS(SELECT 1 FROM ONLY {native._quoted(PTG2_SCHEMA)}.ptg2_snapshot_pin "
                "WHERE owner_type=:owner_type AND owner_id=:owner_id AND snapshot_id=:snapshot_id)"
            ),
            pin,
            api_read=True,
        )
    ).scalar_one()
    if present is not True:
        raise PTG2ManifestArtifactError("membership index source retention pin is missing")
    return pin


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


async def _attachment_storage(session, view_oid, owner, *, api_read=False):
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
    if api_read is True:
        await _api_read_snapshot(session)
    else:
        await native._lock_family(session, schema, SPEC.table_names, "ACCESS SHARE", nowait=True)
    ownership = await native.capture_model_family_stage_ownership(session, SPEC, dataset)
    if ownership.schema_oid != schema_oid or set(ownership.relation_oids) != {
        (dependency[3], int(dependency[0])) for dependency in dependencies
    }:
        raise PTG2ManifestArtifactError("membership index storage identity changed")
    await _require_closed_local_custody(session, ownership, owner)
    return ownership


async def _sealed_header(session, schema, identity, *, api_read=False):
    header = (
        (
            await _read_metadata(
                session,
                text(
                    f"SELECT identity,content_digest,counts FROM {'ONLY ' if api_read else ''}{qualified_relation_sql(schema, 'ptg_membership_manifest')} WHERE singleton=1"
                ),
                api_read=api_read,
            )
        )
        .mappings()
        .one()
    )
    actual = _logical_source_identity(header["identity"], reference=identity) if api_read else header["identity"]
    expected = _logical_source_identity(identity) if api_read else identity
    if actual != expected or re.fullmatch("[0-9a-f]{64}", header["content_digest"] or "") is None:
        raise PTG2ManifestArtifactError("membership index sealed identity differs")
    coverage_rows = [
        dict(coverage_row)
        for coverage_row in (
            await _read_metadata(
                session,
                text(
                    f'SELECT code_key,occurrence_count,set_count,set_digest FROM {"ONLY " if api_read else ""}"{schema}".ptg_membership_code_coverage ORDER BY code_key'
                ),
                api_read=api_read,
            )
        ).mappings()
    ]
    if coverage_rows != header["counts"].get("code_coverage"):
        raise PTG2ManifestArtifactError("membership index code coverage differs")
    return dict(header)


async def _has_named_attachment(session, name):
    return (
        await _read_metadata(
            session,
            text("SELECT pg_catalog.to_regclass(:relation) IS NOT NULL"),
            {"relation": qualified_relation_sql("hp_snapshot_retention", name)},
            api_read=True,
        )
    ).scalar_one() is True


async def _locate_named(session, identity):
    identity = _logical_source_identity(identity)
    direct = attachment_name(identity)
    if await _has_named_attachment(session, direct):
        names = (direct,)
    else:
        from api.ptg2_serving import PTG2_SCHEMA

        pins = (
            (
                await _read_metadata(
                    session,
                    text(
                        f"SELECT owner_id FROM ONLY {qualified_relation_sql(PTG2_SCHEMA, 'ptg2_snapshot_pin')} "
                        "WHERE owner_type=:owner_type AND snapshot_id=:snapshot_id ORDER BY owner_id"
                    ),
                    {"owner_type": CONTRACT, "snapshot_id": identity["snapshot_id"]},
                    api_read=True,
                )
            )
            .mappings()
            .all()
        )
        keys = [pin_row["owner_id"] for pin_row in pins]
        if len(keys) != len(set(keys)) or any(type(key) is not str or _HEX.fullmatch(key) is None for key in keys):
            raise PTG2ManifestArtifactError("membership source retention editions differ")
        names = tuple(_attachment_for_digest(key) for key in keys)
    matches = []
    for name in names:
        if name != direct and not await _has_named_attachment(session, name):
            continue
        relation = await _named_family(session, name, SPEC, "ptg_membership_manifest")
        selector = (
            await _read_metadata(
                session,
                text(
                    f"SELECT identity FROM ONLY {qualified_relation_sql(relation.schema_name, relation.table_name)} "
                    "WHERE singleton=1 AND identity->>'source_key'=:source_key AND identity->>'snapshot_id'=:snapshot_id "
                    "AND identity->>'code_system'=:code_system AND identity->>'code'=:code"
                ),
                {key: identity[key] for key in ("source_key", "snapshot_id", "code_system", "code")},
                api_read=True,
            )
        ).first()
        if selector is None:
            if name == direct:
                raise PTG2ManifestArtifactError("membership attachment source scope differs")
            continue
        header_by_field = await _sealed_header(session, relation.schema_name, identity, api_read=True)
        # Decode the existing identifier only after complete logical identity and coverage agreement.
        projection = digest(header_by_field["identity"])
        if _attachment_for_digest(projection) != name:
            raise PTG2ManifestArtifactError("membership attachment edition differs")
        pin = await _api_source_pin(session, identity, projection_id=projection)
        header_by_field = {**header_by_field, "identity": identity}
        matches.append((relation, header_by_field, None, CompactAddressRelation("hp_snapshot_retention", name), pin))
    if len(matches) > 1:
        raise PTG2ManifestArtifactError("membership attachment logical edition is ambiguous")
    return matches[0] if matches else None


async def locate(session, identity, *, api_read=False):
    """Locate the exact attached family and validate its complete logical scope."""
    if type(api_read) is not bool:
        raise PTG2ManifestArtifactError("capability API read mode is invalid")
    transaction = await _api_read_snapshot(session) if api_read else None
    if api_read:
        async with _api_storage_read(session, transaction):
            return await _locate_named(session, identity)
    owner = await protected_owner(session)
    if owner is None:
        return None
    view_oid = await _active_view(session, identity, owner)
    if view_oid is None:
        return None
    ownership = await _attachment_storage(session, view_oid, owner, **({"api_read": True} if api_read else {}))
    async with _api_storage_read(session, transaction) if api_read else nullcontext():
        header = await _sealed_header(
            session, ownership.schema_name, identity, **({"api_read": True} if api_read else {})
        )
        pin = await (_api_source_pin if api_read else source_pin)(session, identity)
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
    if type(ownership) is CompactAddressRelation:
        return dict(
            header["identity"],
            state="ready",
            projection_id=pin["owner_id"],
            content_digest=header["content_digest"],
            counts=header["counts"],
            code_coverage=header["counts"]["code_coverage"],
            active_attachment={"schema_name": view_oid.schema_name, "name": view_oid.table_name},
            source_pin=pin,
            storage=_named_storage_receipt(ownership, SPEC),
        )
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
