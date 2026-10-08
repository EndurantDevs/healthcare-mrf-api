# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Internal complete-family identity; portable metadata is never custody authority."""

from dataclasses import dataclass
from datetime import UTC
from uuid import UUID

from process.ptg_parts.db_tables import _quote_ident

PHYSICAL_BINDING_CONTRACT = "ptg.snapshot-local-physical-binding.v1"
SERVING_SCOPE_CONTRACT = "ptg.snapshot-local-serving-scope.v1"
PREPARED_LOCAL_READ_VIEW_SHA256 = "b5c5a00338fcb6ddcd09dbe1ef24b73a43d759c9f61ab3ca34bd42c93ea0cb26"
INSTALLED_LOCAL_READ_VIEW_SHA256 = "0525c69ffa3b9815a9e278fe76fab967013fcb0f200c450167ac92e611a76cb3"
_SOURCE_FIELDS = (
    "source_key",
    "source_type",
    "identity_kind",
    "identity_sha256",
    "raw_container_sha256",
    "logical_json_sha256",
    "logical_hash_deferred",
    "source_trace_set_hash",
)
_PREPARATION = "hp_snapshot_retention.reference_preparation"
_INITIAL_PREPARATION_COLUMNS = (
    "operation_id",
    "lease_token",
    "attempt",
    "package_id",
    "node_id",
    "importer_id",
    "dataset_key",
    "profile_contract",
    "stage_schema",
    "stage_schema_oid",
    "builder_oid",
    "inventory_sha256",
)


class PTG2PhysicalBindingError(RuntimeError):
    """A local physical family is incomplete, mismatched or not admitted."""


async def _local_preparation_owner(session):
    """Authenticate the installed control catalog, preserving only initial builder INSERTs."""
    return await _require_local_preparation_catalog(session, lock_tables=True)


async def local_preparation_catalog_owner(session):
    """Authenticate the same protected native owner without granting or reading its tables."""
    return await _require_local_preparation_catalog(session, lock_tables=False)


async def _require_local_preparation_catalog(session, *, lock_tables):
    """The publisher locks controls; view-only readers inspect their identical native catalog."""
    from sqlalchemy import text

    from process.reference_family_archive import _require_transaction

    _require_transaction(session)
    owner_oid = None
    for table in (_PREPARATION, _PREPARATION + "_relation", _PREPARATION + "_sequence"):
        if lock_tables:
            await session.execute(text(f"LOCK TABLE ONLY {table} IN ACCESS SHARE MODE NOWAIT"))
        proof_by_field = (
            (
                await session.execute(
                    text(
                        "SELECT c.relowner::bigint AS owner_oid,c.relkind='r' AND c.relpersistence='p' "
                        "AND NOT c.relispartition AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
                        "AND c.relowner=n.nspowner AND NOT owner.rolcanlogin AND NOT owner.rolsuper "
                        "AND NOT owner.rolcreaterole AND NOT owner.rolcreatedb AND NOT owner.rolreplication "
                        "AND NOT owner.rolbypassrls AND current_user=session_user "
                        "AND reader.rolcanlogin AND NOT reader.rolsuper AND NOT reader.rolcreaterole AND NOT reader.rolcreatedb "
                        "AND NOT reader.rolreplication AND NOT reader.rolbypassrls "
                        "AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a "
                        "WHERE a.grantee<>n.nspowner AND a.privilege_type='CREATE') "
                        "AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
                        "WHERE a.grantee<>c.relowner AND (a.is_grantable OR NOT (a.privilege_type='SELECT' "
                        "OR (:child AND a.privilege_type='INSERT')))) "
                        "AND NOT EXISTS(SELECT 1 FROM pg_attribute col,LATERAL aclexplode(col.attacl) a "
                        "WHERE col.attrelid=c.oid AND a.grantee<>c.relowner AND (a.is_grantable "
                        "OR NOT (a.privilege_type='SELECT' OR (a.privilege_type='INSERT' "
                        "AND (:child OR col.attname=ANY(CAST(:initial_columns AS text[]))))))) "
                        "AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid) "
                        "AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
                        "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) AS protected "
                        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "JOIN pg_roles owner ON owner.oid=c.relowner JOIN pg_roles reader ON reader.rolname=current_user "
                        "WHERE n.nspname=split_part(:table,'.',1) AND c.relname=split_part(:table,'.',2)"
                    ),
                    {
                        "table": table,
                        "child": table != _PREPARATION,
                        "initial_columns": list(_INITIAL_PREPARATION_COLUMNS),
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            not proof_by_field
            or not proof_by_field["protected"]
            or (owner_oid is not None and proof_by_field["owner_oid"] != owner_oid)
        ):
            raise PTG2PhysicalBindingError("PTG local preparation control catalog is not protected")
        owner_oid = proof_by_field["owner_oid"]
    return owner_oid


def _local_read_view_columns(is_prepared):
    """The fixed native projections expose no operation lease, request or role configuration."""
    names = [
        "contract",
        "destination_snapshot_id",
        "package_id",
        "manifest_sha256",
        "artifact_sha256",
        "validation_sha256",
        "inventory_sha256",
        "owner_oid",
        "native_validation",
        "relation_inventory",
        "sequence_inventory",
    ]
    if not is_prepared:
        names.extend(
            (
                "generation_id",
                "publication_fence",
                "publication_sha256",
                "registration_sha256",
                "generation_inventory",
                "native_publication",
            )
        )
    return {
        name: "bigint"
        if name == "owner_oid"
        else "jsonb"
        if name.endswith("inventory") or name in {"native_validation", "native_publication"}
        else "text"
        for name in names
    }


async def require_local_physical_read_view(session, *, is_prepared):
    """Attest and lock a fixed read-only projection; unqualified definitions always refuse."""
    from sqlalchemy import text

    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    _qualified_local_read_view_sha(is_prepared)
    owner_oid = await local_preparation_catalog_owner(session)
    ordinary_reader = await session.scalar(
        text(
            "SELECT NOT pg_has_role(current_user,CAST(:owner_oid AS oid),'MEMBER') "
            "AND NOT has_schema_privilege(current_user,to_regnamespace(:schema_name),'CREATE')"
        ),
        {"owner_oid": owner_oid, "schema_name": resolve_ptg2_schema()},
    )
    if ordinary_reader is not True:
        raise PTG2PhysicalBindingError("PTG local reader privileges differ")
    return await _require_local_physical_view_catalog(session, is_prepared=is_prepared, owner_oid=owner_oid)


async def require_local_physical_publisher_view(session, *, is_prepared):
    """Authenticate actual publisher inheritance separately, never by a reader privilege flag."""
    from sqlalchemy import text

    _qualified_local_read_view_sha(is_prepared)
    owner_oid = await _local_preparation_owner(session)
    if (
        await session.scalar(
            text("SELECT pg_has_role(current_user,CAST(:owner_oid AS oid),'USAGE')"), {"owner_oid": owner_oid}
        )
        is not True
    ):
        raise PTG2PhysicalBindingError("PTG local publisher privileges differ")
    return await _require_local_physical_view_catalog(session, is_prepared=is_prepared, owner_oid=owner_oid)


async def require_local_binding_publisher(session):
    """Require installed native controls before the caller-owned LOCAL publication cut."""
    from process.ptg_parts.result_archive_candidate_validation import require_local_publication_controls

    await require_local_physical_publisher_view(session, is_prepared=True)
    await require_local_physical_publisher_view(session, is_prepared=False)
    await require_local_publication_controls(session)


def _qualified_local_read_view_sha(is_prepared):
    """Native qualification is source-bound, never a caller-supplied digest."""
    import re

    expected_sha = PREPARED_LOCAL_READ_VIEW_SHA256 if is_prepared else INSTALLED_LOCAL_READ_VIEW_SHA256
    if not isinstance(expected_sha, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None:
        raise PTG2PhysicalBindingError("PTG local read interface is not qualified")
    return expected_sha


async def _require_local_physical_view_catalog(session, *, is_prepared, owner_oid):
    """The two fixed role-specific entrypoints share one exact native catalog proof."""
    import hashlib
    import json

    from sqlalchemy import text

    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    expected_sha = _qualified_local_read_view_sha(is_prepared)
    view_name = "ptg2_prepared_physical_binding" if is_prepared else "ptg2_installed_physical_binding"
    schema_name = resolve_ptg2_schema()
    query = text(
        "SELECT c.oid::bigint AS oid,pg_get_viewdef(c.oid,false) AS definition,"
        "c.relkind='v' AND c.relpersistence='p' AND c.relowner=:owner_oid "
        "AND (SELECT array_agg(option ORDER BY option) FROM unnest(c.reloptions) option) "
        "= ARRAY['security_barrier=true','security_invoker=false']::text[] "
        "AND has_schema_privilege(current_user,n.oid,'USAGE') AND has_table_privilege(current_user,c.oid,'SELECT') "
        "AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
        "WHERE a.grantee<>c.relowner AND (a.grantee=0 OR a.is_grantable OR a.privilege_type<>'SELECT')) "
        "AND NOT EXISTS(SELECT 1 FROM pg_attribute col,LATERAL aclexplode(col.attacl) a "
        "WHERE col.attrelid=c.oid AND a.grantee<>c.relowner AND (a.grantee=0 OR a.is_grantable OR a.privilege_type<>'SELECT')) "
        "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) AS read_only,"
        "(SELECT jsonb_object_agg(a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod)) "
        "FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relname=:view"
    )
    parameters_by_name = {"owner_oid": owner_oid, "schema": schema_name, "view": view_name}
    first_oid = None
    for lock_held in (False, True):
        if lock_held:
            await session.execute(
                text(
                    f"LOCK TABLE ONLY {_quote_ident(schema_name)}.{_quote_ident(view_name)} IN ACCESS SHARE MODE NOWAIT"
                )
            )
        proof = await _local_read_view_proof(session, query, parameters_by_name)
        if (
            proof is None
            or not proof["read_only"]
            or hashlib.sha256(proof["definition"].encode()).hexdigest() != expected_sha
        ):
            raise PTG2PhysicalBindingError("PTG local read interface catalog differs")
        columns_by_name = json.loads(proof["columns"]) if isinstance(proof["columns"], str) else proof["columns"]
        if columns_by_name != _local_read_view_columns(is_prepared) or (lock_held and proof["oid"] != first_oid):
            raise PTG2PhysicalBindingError("PTG local read interface identity differs")
        first_oid = proof["oid"]
    return owner_oid


async def _local_read_view_proof(session, query, parameters_by_name):
    """Use the qualified deparser path only inside the owning transaction, then restore it."""
    from sqlalchemy import text

    original_path = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
    try:
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog, pg_temp',true)"))
        return (await session.execute(query, parameters_by_name)).mappings().one_or_none()
    finally:
        await session.execute(
            text("SELECT pg_catalog.set_config('search_path',:original_path,true)"), {"original_path": original_path}
        )


async def require_frozen_local_preparation(session, *, operation, ownership):
    """Rejoin real frozen custody and current lease before called receiver validation."""
    from sqlalchemy import text

    owner_oid = await _local_preparation_owner(session)
    proof_by_field = (
        (
            await session.execute(
                text(
                    f"SELECT p.*,pg_has_role(current_user,CAST(:owner_oid AS oid),'USAGE') AS publisher "
                    f"FROM {_PREPARATION} p WHERE p.operation_id=CAST(:operation_id AS uuid) "
                    "FOR SHARE OF p NOWAIT"
                ),
                {"operation_id": operation["operation_id"], "owner_oid": owner_oid},
            )
        )
        .mappings()
        .one_or_none()
    )
    coordinates = ("lease_token", "attempt", "package_id", "node_id", "importer_id", "dataset_key")
    if (
        not proof_by_field
        or not proof_by_field["publisher"]
        or proof_by_field["state"] not in {"frozen", "validated"}
        or proof_by_field["profile_contract"] != "ptg_result.postgres.v2"
        or proof_by_field["frozen_owner_oid"] != owner_oid
        or proof_by_field["stage_schema"] != ownership.schema_name
        or proof_by_field["stage_schema_oid"] != ownership.schema_oid
        or any(str(proof_by_field[field]) != str(operation[field]) for field in coordinates)
    ):
        raise PTG2PhysicalBindingError("PTG local frozen preparation custody differs")
    await verify_local_data_family(session, ownership)
    await _require_local_preparation_inventory(session, proof_by_field, ownership)
    await _require_closed_local_custody(session, ownership, owner_oid)
    return owner_oid


async def _require_local_preparation_inventory(session, preparation_by_field, ownership):
    """Bind every initial child row to the publisher-owned header's complete inventory digest."""
    from sqlalchemy import text

    parameters_by_name = {"operation_id": preparation_by_field["operation_id"]}
    relations = (
        (
            await session.execute(
                text(
                    f"SELECT ordinal,table_name,relation_oid FROM {_PREPARATION}_relation "
                    "WHERE operation_id=:operation_id ORDER BY ordinal"
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )
    sequences = (
        (
            await session.execute(
                text(
                    f"SELECT ordinal,sequence_name,sequence_oid,owner_table,owner_column FROM {_PREPARATION}_sequence "
                    "WHERE operation_id=:operation_id ORDER BY ordinal"
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )
    _require_local_inventory(ownership, relations, sequences, preparation_by_field["inventory_sha256"])


def _require_local_inventory(ownership, relations, sequences, inventory_sha256):
    """Compare every ordered native child and its canonical inventory encoding."""
    expected_relations = [
        {"ordinal": ordinal, "table_name": name, "relation_oid": oid}
        for ordinal, (name, oid) in enumerate(ownership.relation_oids)
    ]
    expected_sequences = [
        {"ordinal": ordinal, "sequence_name": name, "sequence_oid": oid, "owner_table": table, "owner_column": column}
        for ordinal, (name, oid, table, column) in enumerate(ownership.sequence_oids)
    ]
    if [dict(relation) for relation in relations] != expected_relations or [
        dict(sequence) for sequence in sequences
    ] != expected_sequences:
        raise PTG2PhysicalBindingError("PTG local protected inventory differs")
    inventory_by_field = {
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relations": [
            {field: relation[field] for field in ("table_name", "relation_oid")} for relation in expected_relations
        ],
        "sequences": [
            {field: sequence[field] for field in ("sequence_name", "sequence_oid", "owner_table", "owner_column")}
            for sequence in expected_sequences
        ],
    }
    if _native_metadata_digest(inventory_by_field) != inventory_sha256:
        raise PTG2PhysicalBindingError("PTG local protected inventory digest differs")


async def _require_closed_local_custody(session, ownership, owner_oid):
    """Require actual native ownership and closed writes, not comments or inventory JSON."""
    from sqlalchemy import text

    object_oids = [oid for _name, oid in ownership.relation_oids]
    sequence_oids = [oid for _name, oid, _table, _column in ownership.sequence_oids]
    proof_by_field = (
        (
            await session.execute(
                text(
                    _closed_local_custody_query(
                        owner_parameter=":owner_oid",
                        schema_parameter=":schema_oid",
                        heaps_parameter=":heaps",
                        sequences_parameter=":sequences",
                        objects_parameter=":objects",
                    )
                ),
                {
                    "objects": object_oids + sequence_oids,
                    "heaps": object_oids,
                    "sequences": sequence_oids,
                    "owner_oid": owner_oid,
                    "schema_oid": ownership.schema_oid,
                },
            )
        )
        .mappings()
        .one()
    )
    _require_closed_local_custody_proof(ownership, proof_by_field)


def _closed_local_custody_query(
    *, owner_parameter, schema_parameter, heaps_parameter, sequences_parameter, objects_parameter
):
    """Compile the same fixed catalog proof for native Session and driver callers."""
    return (
        f"SELECT count(*) AS object_count,bool_and(c.relowner={owner_parameter} AND n.nspowner={owner_parameter} "
        f"AND c.relnamespace={schema_parameter} AND NOT c.relispartition AND c.relpersistence='p' "
        "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
        f"AND ((c.oid=ANY(CAST({heaps_parameter} AS oid[])) AND c.relkind='r') "
        f"OR (c.oid=ANY(CAST({sequences_parameter} AS oid[])) AND c.relkind='S')) "
        "AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a "
        "WHERE a.grantee<>n.nspowner AND a.privilege_type='CREATE') "
        "AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's' ELSE 'r' END::\"char\",c.relowner))) a "
        "WHERE a.grantee<>c.relowner AND (a.is_grantable OR c.relkind='S' OR a.privilege_type<>'SELECT')) "
        "AND NOT EXISTS(SELECT 1 FROM pg_attribute col,LATERAL aclexplode(col.attacl) a "
        "WHERE col.attrelid=c.oid AND a.grantee<>c.relowner AND (a.is_grantable OR a.privilege_type<>'SELECT')) "
        "AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid) "
        "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) "
        "AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f') "
        "AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)) AS closed "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        f"WHERE c.oid=ANY(CAST({objects_parameter} AS oid[]))"
    )


def _require_closed_local_custody_proof(ownership, proof_by_field):
    """Every declared heap and sequence must still have its original closed native custody."""
    if (
        proof_by_field["object_count"] != len(ownership.relation_oids) + len(ownership.sequence_oids)
        or not proof_by_field["closed"]
    ):
        raise PTG2PhysicalBindingError("PTG local native custody is not closed")


async def require_closed_local_driver_custody(connection, ownership, owner_oid):
    """Recheck all non-owner write and sequence grants, including unrelated principals."""
    if not connection.is_in_transaction():
        raise PTG2PhysicalBindingError("PTG local custody verification requires a transaction")
    heaps = [oid for _name, oid in ownership.relation_oids]
    sequences = [oid for _name, oid, _table, _column in ownership.sequence_oids]
    proof_by_field = await connection.fetchrow(
        _closed_local_custody_query(
            owner_parameter="$1",
            schema_parameter="$2",
            heaps_parameter="$3",
            sequences_parameter="$4",
            objects_parameter="$5",
        ),
        owner_oid,
        ownership.schema_oid,
        heaps,
        sequences,
        heaps + sequences,
    )
    if proof_by_field is None:
        raise PTG2PhysicalBindingError("PTG local native custody is unavailable")
    _require_closed_local_custody_proof(ownership, proof_by_field)


def _native_metadata_digest(metadata):
    """Use the existing protected ledger's canonical metadata encoding, never row hashes."""
    import hashlib
    import json

    return hashlib.sha256(
        json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    ).hexdigest()


def _prepared_local_binding(evidence, snapshot_id, owner_oid):
    """Decode a protected local inventory; decoding itself grants no authority."""
    from process.reference_family_archive import ReferenceFamilyStageOwnership

    try:
        ownership_by_field = evidence["ownership"]
        if set(ownership_by_field) != {
            "importer_id",
            "dataset_id",
            "schema_name",
            "schema_oid",
            "relation_oids",
            "sequence_oids",
        }:
            raise ValueError
        ownership = ReferenceFamilyStageOwnership(
            ownership_by_field["importer_id"],
            UUID(ownership_by_field["dataset_id"]),
            ownership_by_field["schema_name"],
            ownership_by_field["schema_oid"],
            tuple(tuple(pair) for pair in ownership_by_field["relation_oids"]),
            tuple(tuple(entry) for entry in ownership_by_field["sequence_oids"]),
        )
        relation_by_name = dict(ownership.relation_oids)
        physical_binding = PTG2PhysicalBinding(
            PHYSICAL_BINDING_CONTRACT,
            snapshot_id,
            evidence["initialization"]["destination_layout_key"],
            evidence["data"]["payload_snapshot_id"],
            evidence["data"]["payload_snapshot_key"],
            ownership.dataset_id,
            ownership.schema_oid,
            owner_oid,
            tuple((name, relation_by_name[name]) for name in local_data_family_spec().table_names),
            ownership.sequence_oids,
        )
        if (
            ownership.importer_id != local_data_family_spec().importer_id
            or ownership.schema_name != physical_binding.schema_name
            or tuple(name for name, _oid in ownership.relation_oids)
            != tuple(sorted(local_data_family_spec().table_names))
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise PTG2PhysicalBindingError("PTG local prepared physical descriptor differs") from error
    return ownership, physical_binding


async def _prepared_local_header(session, snapshot_id, *, owner_oid=None):
    """Select real publisher-validated authority; portable or caller-supplied JSON is not proof."""
    from sqlalchemy import text

    if owner_oid is None:
        owner_oid = await _local_preparation_owner(session)
    preparations = (
        (
            await session.execute(
                text(
                    f"SELECT * FROM {_PREPARATION} WHERE importer_id='ptg' AND profile_contract='ptg_result.postgres.v2' "
                    "AND validation#>>'{evidence,initialization,destination_snapshot_id}'=:snapshot_id LIMIT 2 FOR SHARE NOWAIT"
                ),
                {"snapshot_id": snapshot_id},
            )
        )
        .mappings()
        .all()
    )
    if len(preparations) != 1:
        raise PTG2PhysicalBindingError("PTG local validated preparation is unavailable")
    preparation_by_field = dict(preparations[0])
    validation_by_field = preparation_by_field["validation"]
    if (
        preparation_by_field["state"] != "validated"
        or preparation_by_field["frozen_owner_oid"] != owner_oid
        or not isinstance(validation_by_field, dict)
        or set(validation_by_field) != {"inventory_sha256", "evidence"}
        or validation_by_field["inventory_sha256"] != preparation_by_field["inventory_sha256"]
        or _native_metadata_digest(validation_by_field) != preparation_by_field["validation_sha256"]
    ):
        raise PTG2PhysicalBindingError("PTG local preparation validation differs")
    evidence = validation_by_field["evidence"]
    model_sha256 = local_data_model_digest()
    if (
        not isinstance(evidence, dict)
        or evidence.get("contract") != "ptg_result.postgres.v2"
        or not isinstance(evidence.get("native_audit"), dict)
        or not isinstance(evidence.get("data"), dict)
        or evidence.get("native_audit", {}).get("contract") != "ptg-local-data.native-set-audit.v1"
        or evidence["native_audit"].get("catalog_sha256") != evidence.get("catalog_sha256")
        or evidence["native_audit"].get("model_sha256") != model_sha256
        or evidence["data"].get("model_sha256") != model_sha256
        or not isinstance(evidence.get("activation_evidence"), dict)
        or evidence["activation_evidence"].get("control_sha256") != evidence.get("control_sha256")
    ):
        raise PTG2PhysicalBindingError("PTG local native validation evidence differs")
    ownership, physical_binding = _prepared_local_binding(evidence, snapshot_id, owner_oid)
    if (
        preparation_by_field["stage_schema"] != ownership.schema_name
        or preparation_by_field["stage_schema_oid"] != ownership.schema_oid
    ):
        raise PTG2PhysicalBindingError("PTG local preparation namespace differs")
    await _pin_local_prepared_family(session, preparation_by_field, ownership, physical_binding, evidence, owner_oid)
    return preparation_by_field, evidence, physical_binding


async def _pin_local_prepared_family(session, preparation_by_field, ownership, physical_binding, evidence, owner_oid):
    """Fence every authenticated payload heap before repeating custody and indexed catalog proof."""
    from sqlalchemy import text

    await _require_local_preparation_inventory(session, preparation_by_field, ownership)
    # Names select locks only after protected inventory authentication. Recheck
    # names/OIDs/owners/catalog after every exact family heap is fenced.
    for name, _oid in physical_binding.relation_oids:
        await session.execute(text(f"LOCK TABLE ONLY {physical_binding.relation(name)} IN ACCESS SHARE MODE NOWAIT"))
    await verify_local_data_family(session, ownership)
    await _require_closed_local_custody(session, ownership, owner_oid)
    if await local_data_catalog_digest(session, ownership) != evidence["catalog_sha256"]:
        raise PTG2PhysicalBindingError("PTG local indexed catalog changed")


def _local_candidate_control_query(
    schema_name, physical_binding, *, snapshot_parameter, payload_parameter, lock_controls=True
):
    """Compile one internal control query for the existing native driver or Session."""
    schema = _quote_ident(schema_name)
    return (
        "SELECT snapshot.*,run.options,run.status AS run_status,run.report AS run_report,"
        "run.options->'invalid_price_exclusion_policy' AS invalid_price_exclusion_policy,"
        "binding.snapshot_key,scope.plan_id,scope.plan_market_type,scope.coverage_scope_id,"
        "layout.state AS layout_state,layout.generation AS layout_generation,layout.generation AS storage_generation,"
        "layout.mapping_digest AS layout_mapping_digest,layout.layout_manifest,"
        "root.state AS v4_root_state,root.map_digest AS v4_root_map_digest,"
        "current_pointer.snapshot_id AS current_snapshot_id,"
        "attestation.report_digest AS audit_report_digest,attestation.report AS audit_report,"
        "attestation.activation_intent AS audit_activation_intent,attestation.activated_at AS audit_activated_at,"
        "attestation.contract AS audit_contract,"
        "attestation.source_key AS attested_source_key,"
        "encode(attestation.coverage_scope_id,'hex') AS attested_coverage_scope_id,"
        "encode(attestation.source_set_digest,'hex') AS attested_source_set_digest,"
        "encode(attestation.audit_sample_digest,'hex') AS attested_audit_sample_digest,"
        "frozen.binding_payload AS frozen_binding_payload,timezone('UTC',statement_timestamp()) AS staged_at "
        f"FROM {schema}.ptg2_snapshot snapshot JOIN {schema}.ptg2_import_run run USING(import_run_id) "
        f"JOIN {schema}.ptg2_v3_snapshot_binding binding USING(snapshot_id) "
        f"JOIN {schema}.ptg2_v3_snapshot_scope scope USING(snapshot_id) "
        f"JOIN {schema}.ptg2_v3_snapshot_layout layout ON layout.snapshot_key=binding.snapshot_key "
        f"JOIN {physical_binding.relation('ptg2_v3_snapshot_layout')} payload_layout "
        f"ON payload_layout.snapshot_key={payload_parameter} AND payload_layout.state='sealed' "
        "AND layout.state=payload_layout.state AND layout.generation=payload_layout.generation "
        "AND layout.mapping_digest=payload_layout.mapping_digest AND layout.support_digest=payload_layout.support_digest "
        "AND layout.layout_manifest=payload_layout.layout_manifest "
        "AND layout.logical_byte_count=payload_layout.logical_byte_count "
        "AND layout.storage_shard_id IS NOT DISTINCT FROM payload_layout.storage_shard_id "
        f"JOIN {physical_binding.relation('ptg2_v4_snapshot_map_root')} root ON root.snapshot_key={payload_parameter} "
        f"LEFT JOIN {schema}.ptg2_current_source_snapshot current_pointer "
        "ON current_pointer.source_key=run.options->>'source_key' "
        f"LEFT JOIN {schema}.ptg2_v3_candidate_audit_attestation attestation ON attestation.snapshot_id=snapshot.snapshot_id "
        f"LEFT JOIN {schema}.ptg2_frozen_source_file_binding frozen ON frozen.internal_run_id=run.import_run_id "
        f"WHERE snapshot.snapshot_id={snapshot_parameter}"
        + (" FOR SHARE OF snapshot,run,binding,scope,layout NOWAIT" if lock_controls else "")
    )


async def _local_candidate_control(session, schema_name, physical_binding, *, lock_controls=True):
    """Read actual destination metadata and the exact isolated immutable map root."""
    from sqlalchemy import text

    return (
        (
            await session.execute(
                text(
                    _local_candidate_control_query(
                        schema_name,
                        physical_binding,
                        snapshot_parameter=":snapshot_id",
                        payload_parameter=":payload_key",
                        lock_controls=lock_controls,
                    )
                ),
                {"snapshot_id": physical_binding.snapshot_id, "payload_key": physical_binding.payload_snapshot_key},
            )
        )
        .mappings()
        .one_or_none()
    )


def _local_publication_binding(evidence, publication):
    """Decode exact inventory identity only; this does not authenticate publication."""
    expected_fields = {
        "contract",
        "destination_snapshot_id",
        "destination_layout_key",
        "payload_snapshot_id",
        "payload_snapshot_key",
        "dataset_id",
        "schema_oid",
        "owner_oid",
        "relation_oids",
        "sequence_oids",
        "destination_activation",
        "published_control_sha256",
    }
    if (
        not isinstance(publication, dict)
        or set(publication) != expected_fields
        or publication["contract"] != PHYSICAL_BINDING_CONTRACT
    ):
        raise PTG2PhysicalBindingError("PTG local publication receipt differs")
    snapshot_id = evidence["initialization"]["destination_snapshot_id"]
    ownership, physical_binding = _prepared_local_binding(evidence, snapshot_id, publication["owner_oid"])
    descriptor_by_field = {
        key: getattr(physical_binding, key)
        for key in (
            "destination_layout_key",
            "payload_snapshot_id",
            "payload_snapshot_key",
            "schema_oid",
            "owner_oid",
        )
    }
    descriptor_by_field.update(
        destination_snapshot_id=snapshot_id,
        dataset_id=str(ownership.dataset_id),
        relation_oids=[list(pair) for pair in physical_binding.relation_oids],
        sequence_oids=[list(entry) for entry in physical_binding.sequence_oids],
    )
    if any(publication[key] != expected for key, expected in descriptor_by_field.items()):
        raise PTG2PhysicalBindingError("PTG local published inventory differs")
    return ownership, physical_binding


async def require_local_published_control(connection, *, evidence, publication):
    """Rejoin controls after trusted installed custody checks in this same transaction.

    The caller authenticates the consumed header, installation, package and
    complete native catalog. Neither argument alone is authority. This binds
    the exact activation transition without repeating payload scans.
    """
    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    if not connection.is_in_transaction():
        raise PTG2PhysicalBindingError("PTG local publication verification requires a transaction")
    ownership, physical_binding = _local_publication_binding(evidence, publication)
    await require_closed_local_driver_custody(connection, ownership, physical_binding.owner_oid)
    if await local_data_driver_catalog_digest(connection, ownership) != evidence["catalog_sha256"]:
        raise PTG2PhysicalBindingError("PTG local published catalog changed")
    schema_name = resolve_ptg2_schema()
    candidate = await connection.fetchrow(
        _local_candidate_control_query(
            schema_name,
            physical_binding,
            snapshot_parameter="$1",
            payload_parameter="$2",
        ),
        physical_binding.snapshot_id,
        physical_binding.payload_snapshot_key,
    )
    if candidate is None:
        raise PTG2PhysicalBindingError("PTG local published control is unavailable")
    plans = await connection.fetch(
        f"SELECT plan_id,plan_market_type FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_plan_scope "
        "WHERE snapshot_id=$1 ORDER BY plan_id,plan_market_type FOR SHARE NOWAIT",
        physical_binding.snapshot_id,
    )
    _require_local_published_postimage(candidate, plans, evidence, publication, physical_binding)
    return physical_binding


async def publish_local_data_candidate_in_transaction(
    session, *, operation, expected_attestation_digest, rollback_owner_id
):
    """Use the existing candidate-validation publisher in the caller transaction."""
    from process.ptg_parts.result_archive_candidate_validation import (
        publish_local_data_candidate_in_transaction as publish,
    )

    return await publish(
        session,
        operation=operation,
        expected_attestation_digest=expected_attestation_digest,
        rollback_owner_id=rollback_owner_id,
    )


async def _local_publication_receipt(session, evidence, physical_binding):
    """Read back the actual candidate-validation publication postimage."""
    from process.ptg_parts.result_archive_candidate_validation import local_data_publication_receipt

    return await local_data_publication_receipt(session, evidence, physical_binding)


def _require_local_published_postimage(candidate, plans, evidence, publication, physical_binding):
    """Verify only the permitted staged-to-published control transition."""
    import json
    from copy import deepcopy

    from process.ptg_parts import result_archive_candidate_initialization as initialization
    from process.ptg_parts.ptg2_candidate_attestation import PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
    from process.ptg_parts.source_pointers import candidate_snapshot_attributes

    candidate_by_field = dict(candidate)
    for field_name in ("manifest", "options", "run_report"):
        if isinstance(candidate_by_field[field_name], str):
            candidate_by_field[field_name] = json.loads(candidate_by_field[field_name])
        if not isinstance(candidate_by_field[field_name], dict):
            raise PTG2PhysicalBindingError("PTG local published control encoding differs")
    manifest_by_field = deepcopy(candidate_by_field["manifest"])
    ready = evidence["activation_evidence"]
    activated = publication["destination_activation"]
    plan_scopes = tuple((plan["plan_id"], plan["plan_market_type"]) for plan in plans)
    activation = manifest_by_field.get("activation", {})
    # Snapshot timestamps are UTC-naive; consumed audit timestamps are timestamptz.
    published_at = candidate_by_field["published_at"]
    if published_at is not None and published_at.utcoffset() is None:
        published_at = published_at.replace(tzinfo=UTC)
    if (
        candidate_by_field["status"] != "published"
        or candidate_by_field["run_status"] != "validated"
        or candidate_by_field["import_run_id"] != evidence["initialization"]["destination_import_run_id"]
        or candidate_by_field["snapshot_key"] != physical_binding.destination_layout_key
        or candidate_by_field["previous_snapshot_id"] != ready["expected_current_snapshot_id"]
        or candidate_by_field["plan_id"] != evidence["native_audit"]["identity"]["plan_id"]
        or candidate_by_field["plan_market_type"] != evidence["native_audit"]["identity"]["plan_market_type"]
        or bytes(candidate_by_field["coverage_scope_id"]).hex()
        != evidence["native_audit"]["identity"]["coverage_scope_id"]
        or activation.get("state") != "activated"
        or activation.get("mode") != "reviewed_audit_only_control"
        or candidate_by_field["audit_activation_intent"] != "audit_only"
        or candidate_by_field["audit_contract"] not in PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
        or candidate_by_field["audit_activated_at"] is None
        or candidate_by_field["audit_activated_at"] != published_at
        or activated.get("snapshot_id") != physical_binding.snapshot_id
        or activated.get("source_key") != ready["source_key"]
        or activated.get("previous_snapshot_id") != ready["expected_current_snapshot_id"]
        or activated.get("activated_at") != candidate_by_field["published_at"].isoformat()
        or activated.get("audit_report_digest") != bytes(candidate_by_field["audit_report_digest"] or b"").hex()
        or initialization._local_control_sha256(manifest_by_field, candidate_by_field["options"], plan_scopes)
        != publication["published_control_sha256"]
    ):
        raise PTG2PhysicalBindingError("PTG local published activation differs")
    staged = candidate_snapshot_attributes(
        candidate_by_field, source_key=ready["source_key"], previous_snapshot_id=ready["expected_current_snapshot_id"]
    )
    if (
        initialization._local_control_sha256(staged["manifest"], candidate_by_field["options"], plan_scopes)
        != evidence["control_sha256"]
        or candidate_by_field["run_report"] != staged["manifest"]
    ):
        raise PTG2PhysicalBindingError("PTG local staged preimage changed during publication")


async def _require_local_control(session, schema_name, evidence, physical_binding):
    """Require the genuine staged control postimage recorded by the native preparation."""
    from process.ptg_parts import result_archive_candidate_initialization as initialization

    candidate = await _local_candidate_control(session, schema_name, physical_binding)
    if candidate is None:
        raise PTG2PhysicalBindingError("PTG local candidate control is unavailable")
    plans = await initialization._staged_plan_scopes(
        session, staging_schema=schema_name, source_snapshot_id=physical_binding.snapshot_id
    )
    return _require_local_control_postimage(candidate, plans, evidence, physical_binding)


def _require_local_control_postimage(candidate, plans, evidence, physical_binding):
    """Compare actual staged metadata without treating a decoded descriptor as authority."""
    from process.ptg_parts import result_archive_candidate_initialization as initialization

    candidate_by_field = dict(candidate)
    marker_by_field, _key = initialization._local_cleanup_marker(evidence, evidence["initialization"])
    activation_evidence = evidence["activation_evidence"]
    if (
        candidate_by_field["status"] != "validated"
        or candidate_by_field["run_status"] != "validated"
        or candidate_by_field["manifest"].get("physical_binding_contract") != PHYSICAL_BINDING_CONTRACT
        or candidate_by_field["manifest"].get("local_data_preparation") != marker_by_field
        or candidate_by_field["import_run_id"] != evidence["initialization"]["destination_import_run_id"]
        or candidate_by_field["snapshot_key"] != physical_binding.destination_layout_key
        or candidate_by_field["options"].get("source_key") != activation_evidence.get("source_key")
        or candidate_by_field["previous_snapshot_id"] != activation_evidence.get("expected_current_snapshot_id")
        or initialization._local_control_sha256(candidate_by_field["manifest"], candidate_by_field["options"], plans)
        != evidence["control_sha256"]
        or candidate_by_field["run_report"] != candidate_by_field["manifest"]
    ):
        raise PTG2PhysicalBindingError("PTG local candidate control changed")
    return candidate_by_field


async def local_candidate_audit_state(session, *, candidate_run_id=None, snapshot_id=None, schema_name=None):
    """Resolve a prepared candidate in this transaction, never from its metadata marker alone."""
    from sqlalchemy import text

    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
    from process.ptg_parts.result_archive_candidate_validation import local_data_physical_read_state
    from process.reference_family_archive import _require_transaction

    _require_transaction(session)
    configured_schema = resolve_ptg2_schema()
    if schema_name is not None and schema_name != configured_schema:
        raise PTG2PhysicalBindingError("PTG local control schema differs")
    if not snapshot_id and not candidate_run_id:
        raise PTG2PhysicalBindingError("PTG local candidate selector is missing")
    snapshot_rows = (
        (
            await session.execute(
                text(
                    f"SELECT snapshot_id,import_run_id,manifest FROM {_quote_ident(configured_schema)}.ptg2_snapshot "
                    "WHERE (CAST(:snapshot_id AS text) IS NULL OR snapshot_id=:snapshot_id) "
                    "AND (CAST(:run_id AS text) IS NULL OR import_run_id=:run_id) LIMIT 2"
                ),
                {"snapshot_id": snapshot_id, "run_id": candidate_run_id},
            )
        )
        .mappings()
        .all()
    )
    if len(snapshot_rows) != 1:
        if snapshot_rows or snapshot_id:
            raise PTG2PhysicalBindingError("PTG local candidate selector differs")
        return None
    snapshot_by_field = dict(snapshot_rows[0])
    manifest_by_field = snapshot_by_field["manifest"]
    declared = isinstance(manifest_by_field, dict) and (
        "physical_binding_contract" in manifest_by_field or "local_data_preparation" in manifest_by_field
    )
    if not declared:
        return None
    if manifest_by_field.get("physical_binding_contract") != PHYSICAL_BINDING_CONTRACT:
        raise PTG2PhysicalBindingError("PTG local candidate declaration differs")
    has_view = await session.scalar(
        text("SELECT to_regclass(:table) IS NOT NULL"), {"table": f"{configured_schema}.ptg2_prepared_physical_binding"}
    )
    if not has_view:
        raise PTG2PhysicalBindingError("PTG local preparation authority is unavailable")
    _authority, evidence, physical_binding, candidate = await local_data_physical_read_state(
        session, snapshot_by_field["snapshot_id"], is_prepared=True
    )
    return await _local_candidate_evidence_state(session, candidate, evidence, physical_binding)


async def _local_candidate_evidence_state(session, candidate, evidence, physical_binding):
    """Recompute the existing audit identity from actual isolated source rows under read locks."""
    from sqlalchemy import text

    from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL, _candidate_identity

    source_records = [
        dict(source_record)
        for source_record in (
            await session.execute(
                text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(physical_binding.schema_name))),
                {"snapshot_id": physical_binding.payload_snapshot_id},
            )
        )
        .mappings()
        .all()
    ]
    candidate["raw_container_sha256_values"] = [
        source_record["raw_container_sha256"] for source_record in source_records
    ]
    candidate["frozen_source_records"] = source_records
    identity = _candidate_identity(candidate, physical_binding=physical_binding)
    portable_identity_by_field = {
        key: identity_value.hex() if isinstance(identity_value, bytes) else identity_value
        for key, identity_value in identity.items()
    }
    if portable_identity_by_field != evidence["native_audit"]["identity"]:
        raise PTG2PhysicalBindingError("PTG local candidate audit identity changed")
    return {"candidate": candidate, "source_records": source_records, "physical_binding": physical_binding}


async def revalidate_local_preparation(session, *, operation, metadata_sha256):
    """Authenticate a prepared replay without rebuilding controls or repeating payload scans."""
    snapshot_id = "snapshot-archive-" + str(UUID(str(operation["operation_id"])))
    preparation, evidence, physical_binding = await _prepared_local_header(session, snapshot_id)
    if (
        str(preparation["operation_id"]) != str(operation["operation_id"])
        or preparation["package_id"] != operation["package_id"]
        or evidence["metadata_sha256"] != metadata_sha256
    ):
        raise PTG2PhysicalBindingError("PTG local prepared replay differs")
    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    candidate = await _require_local_control(session, resolve_ptg2_schema(), evidence, physical_binding)
    await _local_candidate_evidence_state(session, candidate, evidence, physical_binding)
    return evidence


async def resolve_local_physical_binding(session, snapshot_id):
    """Return installed authority only after a fresh same-transaction native read proof."""
    from process.ptg_parts.result_archive_candidate_validation import local_data_physical_read_state

    _authority, _evidence, physical_binding, _candidate = await local_data_physical_read_state(
        session, snapshot_id, is_prepared=False
    )
    return physical_binding


def physical_family_tables():
    """Reuse the reviewed archive closure, including its payload root and selected CAS."""
    return physical_family_spec().table_names


def physical_family_spec():
    """Declare the payload family through installed types, not portable relation paths."""
    from process.ptg_parts.result_archive_candidate_preparation import physical_family_spec as prepare_physical_family

    return prepare_physical_family()


def local_data_family_spec():
    """Close selected payload, provenance and coordinate data, never authority."""
    from process.ptg_parts.result_archive_candidate_preparation import local_data_family_spec as prepare_local_family

    return prepare_local_family()


def local_data_model_digest():
    """Bind the trusted heap, identity, constraint, index and relationship declarations."""
    from process.ptg_parts.result_archive_candidate_preparation import local_data_model_digest as prepared_model_digest

    return prepared_model_digest()


async def capture_local_data_family(session, dataset_id):
    """Reuse exact native heap and model-owned identity-sequence custody."""
    from process import reference_family_archive as native

    return await native.capture_model_family_stage_ownership(
        session, local_data_family_spec(), dataset_id, include_identity=True
    )


async def local_data_catalog_digest(session, ownership):
    """Bind actual native objects and columns; this digest never grants custody."""
    from sqlalchemy import text

    from process import reference_family_archive as native

    native._require_transaction(session)
    object_query, column_query = _local_catalog_queries(":schema_oid", ":schema_name", ":relations")
    objects = await session.execute(text(object_query), {"schema_oid": ownership.schema_oid})
    columns = await session.execute(
        text(column_query),
        {"schema_name": ownership.schema_name, "relations": [name for name, _oid in ownership.relation_oids]},
    )
    return _local_catalog_digest(ownership, objects.mappings().all(), columns.mappings().all())


def _local_catalog_queries(schema_oid_parameter, schema_name_parameter, relations_parameter):
    """Share fixed native catalog statements across the existing Session and driver callers."""
    object_query = (
        "SELECT 'relation' AS kind,c.oid::bigint AS oid,c.relname AS name,c.relowner::bigint AS owner,"
        "c.relkind::text||':'||c.relpersistence::text||':'||c.relfilenode::text||':'||c.reltoastrelid::text "
        f"AS definition FROM pg_class c WHERE c.relnamespace={schema_oid_parameter} "
        "UNION ALL SELECT 'index',i.indexrelid::bigint,c.relname,c.relowner::bigint,"
        "pg_get_indexdef(i.indexrelid)||':'||i.indisvalid::text||':'||i.indisready::text "
        f"FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relnamespace={schema_oid_parameter} "
        "UNION ALL SELECT 'constraint',con.oid::bigint,con.conname,c.relowner::bigint,"
        "pg_get_constraintdef(con.oid)||':'||con.convalidated::text FROM pg_constraint con "
        f"JOIN pg_class c ON c.oid=con.conrelid WHERE c.relnamespace={schema_oid_parameter} ORDER BY kind,oid"
    )
    column_query = (
        "SELECT c.relname AS table_name,a.attname AS column_name,"
        "pg_catalog.format_type(a.atttypid,a.atttypmod) AS data_type,a.attnotnull,a.attidentity,a.attgenerated "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid "
        f"WHERE n.nspname={schema_name_parameter} AND c.relname=ANY(CAST({relations_parameter} AS text[])) AND c.relkind IN ('r','p') "
        "AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum"
    )
    return object_query, column_query


def _local_catalog_digest(ownership, objects, columns):
    """Keep the existing protected catalog digest encoding unchanged."""
    column_records = [
        {
            "table": str(column["table_name"]),
            "column": str(column["column_name"]),
            "type": str(column["data_type"]),
            "not_null": bool(column["attnotnull"]),
            "identity": str(column["attidentity"]),
            "generated": str(column["attgenerated"]),
        }
        for column in columns
    ]
    if {column["table"] for column in column_records} != {name for name, _oid in ownership.relation_oids}:
        raise PTG2PhysicalBindingError("PTG local model columns differ")
    return _native_metadata_digest(
        {"objects": [dict(native_object) for native_object in objects], "columns": column_records}
    )


async def local_data_driver_catalog_digest(connection, ownership):
    """Use the same fixed native catalog proof without a Session adapter or payload scans."""
    if not connection.is_in_transaction():
        raise PTG2PhysicalBindingError("PTG local catalog verification requires a transaction")
    object_query, column_query = _local_catalog_queries("$1", "$1", "$2")
    objects = await connection.fetch(object_query, ownership.schema_oid)
    columns = await connection.fetch(
        column_query, ownership.schema_name, [name for name, _oid in ownership.relation_oids]
    )
    return _local_catalog_digest(ownership, objects, columns)


async def verify_local_data_family(session, ownership):
    """Check the complete local data inventory; this token alone grants no authority."""
    from process import reference_family_archive as native

    if not isinstance(ownership, native.ReferenceFamilyStageOwnership):
        raise PTG2PhysicalBindingError("PTG local data ownership is invalid")
    observed = await capture_local_data_family(session, ownership.dataset_id)
    if observed != ownership:
        raise PTG2PhysicalBindingError("PTG local data ownership changed")


async def precreate_local_data_family(session, dataset_id):
    """Create all closed-family native heaps before any source or archive COPY."""
    from process import reference_family_archive as native

    return await native.precreate_model_family_stage(
        session, local_data_family_spec(), dataset_id, include_identity=True
    )


async def complete_local_data_family(session, ownership):
    """Finish every index/native constraint before any model relationship check."""
    from process import reference_family_archive as native

    try:
        await native.complete_model_family_stage(session, local_data_family_spec(), ownership, include_identity=True)
    except native.ReferenceFamilyArchiveError as error:
        if str(error) == "reference family stage ownership is invalid":
            raise PTG2PhysicalBindingError("PTG local data ownership is invalid") from error
        if str(error) == "reference family stage ownership differs":
            raise PTG2PhysicalBindingError("PTG local data ownership changed") from error
        raise


async def cleanup_local_data_family(session, ownership):
    """Reuse restrictive native cleanup for the exact locally captured model family."""
    from process import reference_family_archive as native

    await native.cleanup_model_family_stage(session, local_data_family_spec(), ownership, include_identity=True)


async def validate_local_data_family(session, ownership, metadata):
    """Validate isolated data under caller-held custody, never copied root authority."""
    from process import reference_family_archive as native
    from process.ptg_parts import result_archive_closure as closure

    native._require_transaction(session)
    await verify_local_data_family(session, ownership)
    schema = _quote_ident(ownership.schema_name)
    snapshot_id, snapshot_key = metadata["source_snapshot_id"], metadata["source_snapshot_key"]
    layout = await closure._locked_layout(
        session, schema=schema, snapshot_id=snapshot_id, payload_snapshot_key=snapshot_key
    )
    _require_local_payload_layout(layout, metadata, snapshot_key)
    scope = _validate_local_root_coordinates(layout, metadata)
    if (
        await capture_local_serving_scope(
            session, schema_name=ownership.schema_name, snapshot_id=snapshot_id, source_key=scope["source_key"]
        )
        != scope
    ):
        raise PTG2PhysicalBindingError("PTG local payload captured scope differs")
    count_by_table = await _local_payload_counts(
        session, ownership, snapshot_id=snapshot_id, snapshot_key=snapshot_key, scope=scope
    )
    if count_by_table != metadata["row_counts"] or any(
        count_by_table[name] != 1 for name in ("ptg2_snapshot", "ptg2_v3_snapshot_layout")
    ):
        raise PTG2PhysicalBindingError("PTG local payload counts differ")
    await _validate_local_graph_reachability(session, ownership.schema_name, snapshot_id)
    await closure.validate_local_price_attribute_dictionary(
        session, schema_name=ownership.schema_name, snapshot_key=snapshot_key
    )
    block_hashes, map_packs, finalizer_packs = await closure._archive_block_selection(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        layout_by_field=layout,
        max_block_hashes=closure._DEFAULT_MAX_BLOCK_HASHES,
    )
    if (
        count_by_table["ptg2_v3_block"] != len(block_hashes)
        or map_packs != count_by_table["ptg2_v4_snapshot_map_pack"]
        or finalizer_packs != count_by_table["ptg2_v4_finalizer_map_pack"]
    ):
        raise PTG2PhysicalBindingError("PTG local payload reached closure differs")
    return {
        "model_sha256": local_data_model_digest(),
        "payload_snapshot_id": snapshot_id,
        "payload_snapshot_key": snapshot_key,
        "row_counts": count_by_table,
        "layout_mapping_sha256": metadata["layout_mapping_sha256"],
        "map_sha256": metadata["map_sha256"],
        "finalizer_map_sha256": metadata["finalizer_map_sha256"],
    }


def _require_local_payload_layout(layout, metadata, snapshot_key):
    """Preserve exact captured map and finalizer seals before any payload validation."""
    from process.ptg_parts import result_archive_closure as closure

    if closure._validate_layout(layout) != (
        snapshot_key,
        bytes.fromhex(metadata["layout_mapping_sha256"]),
        bytes.fromhex(metadata["map_sha256"]),
        bytes.fromhex(metadata["finalizer_map_sha256"]),
    ):
        raise PTG2PhysicalBindingError("PTG local payload layout differs")


async def _local_payload_counts(session, ownership, *, snapshot_id, snapshot_key, scope):
    """Check the complete isolated family with aggregate counts and set-based scope predicates."""
    from sqlalchemy import text

    schema = _quote_ident(ownership.schema_name)
    count_by_table = {}
    for model in local_data_family_spec().model_types:
        table = f"{schema}.{_quote_ident(model.__tablename__)}"
        count_by_table[model.__tablename__] = int(await session.scalar(text(f"SELECT count(*) FROM {table}")))
        for column_name, expected in (("snapshot_key", snapshot_key), ("snapshot_id", snapshot_id)):
            if column_name in model.__table__.columns and await session.scalar(
                text(
                    f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {_quote_ident(column_name)} IS DISTINCT FROM :expected)"
                ),
                {"expected": expected},
            ):
                raise PTG2PhysicalBindingError("PTG local payload scope differs")
        if "coverage_scope_id" in model.__table__.columns and await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE coverage_scope_id IS DISTINCT FROM :expected)"),
            {"expected": bytes.fromhex(scope["coverage_scope_id"])},
        ):
            raise PTG2PhysicalBindingError("PTG local payload coverage differs")
    return count_by_table


async def _validate_local_graph_reachability(session, schema_name, snapshot_id):
    """Reject graph rows outside the selected snapshot's indexed reachable closure."""
    from sqlalchemy import text

    from process.ptg_parts import result_archive_candidate_initialization as initialization
    from process.ptg_parts import result_archive_closure as closure

    selection_by_table = {relation.table_name: relation for relation in closure._relations(schema_name)}
    for table_name in initialization._SOURCE_GRAPH_COLUMNS_BY_TABLE:
        predicate = selection_by_table[table_name].predicate_sql
        if (
            await session.scalar(
                text(
                    f"SELECT EXISTS(SELECT 1 FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)} WHERE ({predicate}) IS NOT TRUE)"
                ),
                {"snapshot_id": snapshot_id},
            )
            is not False
        ):
            raise PTG2PhysicalBindingError("PTG local payload graph reachability differs")


def _validate_local_root_coordinates(layout_by_field, metadata):
    """Recheck copied semantic seals against captured data, not destination authority."""
    from api.ptg2_tables import (
        PTG2ManifestArtifactError,
        _strict_coverage_scope_id,
        _strict_v3_source_set,
        _validated_published_source_set,
    )
    from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256

    try:
        scope = validate_local_serving_scope(metadata["closure_metadata"]["serving_scope"])
        publication = metadata["source_publication"]
        identity = publication.get("identity", publication)
        manifest = layout_by_field["manifest"]
        if (
            scope["snapshot_id"] != metadata["source_snapshot_id"]
            or scope["source_key"] != publication["source_key"]
            or result_archive_manifest_sha256(manifest) != identity["snapshot_manifest_sha256"]
        ):
            raise ValueError
        source_count = len(scope["source_assignments"])
        observed_source_set = _validated_published_source_set(
            scope["source_assignments"], expected_source_count=source_count
        )
        for root in (manifest, layout_by_field["layout_manifest"]):
            serving = root["serving_index"]
            if (
                _strict_coverage_scope_id(serving) != scope["coverage_scope_id"]
                or type(serving["source_count"]) is not int
                or serving["source_count"] != source_count
            ):
                raise ValueError
            declared_source_set = _strict_v3_source_set(serving, source_count=source_count)
            if declared_source_set is not None and declared_source_set != observed_source_set:
                raise ValueError
        if _strict_v3_source_set(manifest["serving_index"], source_count=source_count) is None:
            raise ValueError
        if (
            "source_set_digest" in identity
            and identity["source_set_digest"] != observed_source_set["raw_container_sha256_digest"]
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, PTG2ManifestArtifactError) as error:
        raise PTG2PhysicalBindingError("PTG local payload root coordinates differ") from error
    return scope


def validate_local_serving_scope(scope_by_field):
    """Decode bounded coordinate evidence, never local admission or client authorization."""
    from process.ptg_parts.result_archive_candidate_validation import validate_local_serving_scope as validate_scope

    return validate_scope(scope_by_field)


async def capture_local_serving_scope(session, *, schema_name, snapshot_id, source_key):
    """Capture complete low-volume coordinates in the caller's pinned source transaction."""
    from sqlalchemy import text

    from process import reference_family_archive as native

    native._require_transaction(session)
    schema = _quote_ident(schema_name)
    parameters_by_name = {"snapshot_id": snapshot_id}
    scope = (
        (
            await session.execute(
                text(
                    f"SELECT plan_id,lower(plan_market_type) AS plan_market_type,encode(coverage_scope_id,'hex') AS coverage_scope_id "
                    f"FROM {schema}.ptg2_v3_snapshot_scope WHERE snapshot_id=:snapshot_id FOR KEY SHARE"
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .one()
    )
    plans = (
        (
            await session.execute(
                text(
                    f"SELECT plan_id,lower(plan_market_type) AS plan_market_type FROM {schema}.ptg2_v3_snapshot_plan_scope "
                    "WHERE snapshot_id=:snapshot_id ORDER BY plan_id,lower(plan_market_type) LIMIT 257 FOR KEY SHARE"
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )
    source_rows = (
        (
            await session.execute(
                text(
                    f"SELECT {','.join(_SOURCE_FIELDS)} FROM {schema}.ptg2_v3_snapshot_source "
                    "WHERE snapshot_id=:snapshot_id ORDER BY source_key LIMIT 257 FOR KEY SHARE"
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )
    return validate_local_serving_scope(
        {
            "contract": SERVING_SCOPE_CONTRACT,
            "snapshot_id": snapshot_id,
            "source_key": source_key,
            "coverage_scope_id": scope["coverage_scope_id"],
            "primary_plan": [scope["plan_id"], scope["plan_market_type"]],
            "plan_scopes": [[plan["plan_id"], plan["plan_market_type"]] for plan in plans],
            "source_assignments": [dict(source_row) for source_row in source_rows],
        }
    )


def _is_positive_oid(value):
    return type(value) is int and 0 < value <= 4_294_967_295


@dataclass(frozen=True)
class PTG2PhysicalBinding:
    """Local inventory carrier, not a portable receipt or an admission capability.

    Destination metadata keys must never replace the IDs encoded in retained
    payloads. A publisher must authenticate this inventory and keep its exact
    family pinned before any future serving resolution can be admitted.
    """

    contract: str
    snapshot_id: str
    destination_layout_key: int
    payload_snapshot_id: str
    payload_snapshot_key: int
    dataset_id: UUID
    schema_oid: int
    owner_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    sequence_oids: tuple[tuple[str, int, str, str], ...]

    def __post_init__(self):
        """Reject partial families and malformed local identities before resolution."""
        if self.contract != PHYSICAL_BINDING_CONTRACT or not isinstance(self.dataset_id, UUID):
            raise PTG2PhysicalBindingError("PTG physical binding contract or dataset differs")
        if any(
            type(snapshot_id) is not str
            or not snapshot_id
            or len(snapshot_id) > 96
            or snapshot_id != snapshot_id.strip()
            or any(ord(character) < 32 for character in snapshot_id)
            for snapshot_id in (self.snapshot_id, self.payload_snapshot_id)
        ):
            raise PTG2PhysicalBindingError("PTG physical binding snapshot identity is invalid")
        if any(
            type(layout_key) is not int or not 0 < layout_key < 2**63
            for layout_key in (self.destination_layout_key, self.payload_snapshot_key)
        ):
            raise PTG2PhysicalBindingError("PTG physical binding layout identity is invalid")
        if not all(_is_positive_oid(oid) for oid in (self.schema_oid, self.owner_oid)):
            raise PTG2PhysicalBindingError("PTG physical binding namespace custody is invalid")
        if (
            type(self.relation_oids) is not tuple
            or any(type(pair) is not tuple or len(pair) != 2 for pair in self.relation_oids)
            or tuple(name for name, _oid in self.relation_oids) != local_data_family_spec().table_names
            or not all(_is_positive_oid(oid) for _name, oid in self.relation_oids)
            or len({oid for _name, oid in self.relation_oids}) != len(self.relation_oids)
        ):
            raise PTG2PhysicalBindingError("PTG physical binding complete model inventory differs")
        expected_sequences = tuple(
            sorted(
                (f"{model.__tablename__}_{column.name}_seq", model.__tablename__, column.name)
                for model in local_data_family_spec().model_types
                for column in model.__table__.columns
                if column.identity is not None
            )
        )
        if (
            type(self.sequence_oids) is not tuple
            or any(type(entry) is not tuple or len(entry) != 4 for entry in self.sequence_oids)
            or tuple((name, table, column) for name, _oid, table, column in self.sequence_oids) != expected_sequences
            or not all(_is_positive_oid(oid) for _name, oid, _table, _column in self.sequence_oids)
            or len({oid for _name, oid, _table, _column in self.sequence_oids}) != len(self.sequence_oids)
            or {oid for _name, oid, _table, _column in self.sequence_oids} & {oid for _name, oid in self.relation_oids}
        ):
            raise PTG2PhysicalBindingError("PTG physical binding identity sequence inventory differs")

    @property
    def schema_name(self):
        """Use only the existing locally UUID-derived archive custody namespace."""
        from process.reference_family_archive import reference_family_stage_schema

        return reference_family_stage_schema(self.dataset_id)

    def relation(self, table):
        """Derive an internal exact model path without accepting peer relation names."""
        if table not in local_data_family_spec().table_names:
            raise PTG2PhysicalBindingError("PTG physical binding model is outside the complete family")
        return f"{_quote_ident(self.schema_name)}.{_quote_ident(table)}"


def require_legacy_physical_resolution(declaration):
    """Keep new or unknown physical bindings closed until publisher integration."""
    if declaration is not None:
        raise PTG2PhysicalBindingError("PTG snapshot-local physical binding is not available")
