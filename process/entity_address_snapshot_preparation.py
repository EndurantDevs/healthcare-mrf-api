# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate frozen address candidates separately from bounded native publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from contextlib import nullcontext

from sqlalchemy import text

from process import entity_address_alias_guard as alias_guard
from process import entity_address_snapshot_alias as alias
from process import entity_address_snapshot_destination as destination
from process import entity_address_snapshot_serving as serving

EntityAddressSnapshotDestinationError = destination.EntityAddressSnapshotDestinationError
CONTRACT = "entity_address_archive_preparation.postgres.v1"
SET_CONTRACT = "entity_address_archive_preparation.postgres.v2"
_EVIDENCE_SEQUENCE = "entity_address_evidence_evidence_id_seq"
_ORDINARY_PRINCIPAL_SQL = alias_guard.ORDINARY_PRINCIPAL_SQL


async def prepare_private_entity_address_archive_destination(
    session,
    *,
    owner,
    semantic_receipt,
    source_alias_receipt,
    destination,
):
    """Prepare provisional contents while preserving the original private inventory."""
    from process import entity_address_snapshot_destination as native_destination
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    _require(
        native_destination.validate_entity_address_archive_receipt(semantic_receipt).contract != archive_contract,
        "v2 requires authenticated publisher load",
    )

    _require(
        isinstance(destination, Mapping)
        and {"db_schema", "import_date"} <= set(destination)
        and set(destination) <= {"db_schema", "import_date", "source_serving_generation", "dependency_bindings"},
        "destination is invalid",
    )
    preparation_by_field = {
        **destination,
        "owner": owner,
        "semantic_receipt": semantic_receipt,
        "source_alias_receipt": source_alias_receipt,
    }
    if preparation_by_field.get("dependency_bindings") is not None:
        preparation_by_field["dependency_bindings"] = (
            native_destination.geo_projection.validate_projection_dependency_bindings(
                preparation_by_field["db_schema"], preparation_by_field["dependency_bindings"]
            )
        )
    native_destination._require_caller_transaction(session)
    async with native_destination.db.bind_existing_session(session):
        return await native_destination._prepare_bound_destination(
            session, preparation_by_field, preserve_private_stage=True
        )


def _require(condition, reason):
    if not condition:
        raise EntityAddressSnapshotDestinationError("entity-address protected preparation " + reason)


def _digest(value):
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as error:
        raise EntityAddressSnapshotDestinationError(
            "entity-address protected preparation payload is invalid"
        ) from error
    return hashlib.sha256(encoded).hexdigest()


async def _publisher_authority(session):
    """Root native authority in the existing protected namespace, not supplied role names."""
    destination._require_caller_transaction(session)
    catalog_row = (
        (
            await session.execute(
                text(
                    "SELECT owner.oid AS owner_oid, NOT owner.rolcanlogin AND NOT owner.rolsuper "
                    "AND NOT owner.rolcreaterole AND NOT owner.rolcreatedb AND NOT owner.rolreplication "
                    "AND NOT owner.rolbypassrls AS owner_safe, "
                    "session_user=current_user AND caller.rolcanlogin AND NOT caller.rolsuper "
                    "AND NOT caller.rolcreaterole AND NOT caller.rolcreatedb AND NOT caller.rolreplication "
                    "AND NOT caller.rolbypassrls AND pg_catalog.pg_has_role(caller.oid,owner.oid,'USAGE') "
                    "AND current_setting('session_replication_role')='origin' AS caller_safe "
                    "FROM pg_catalog.pg_namespace namespace JOIN pg_catalog.pg_roles owner ON owner.oid=namespace.nspowner "
                    "JOIN pg_catalog.pg_roles caller ON caller.rolname=session_user "
                    "WHERE namespace.nspname='hp_snapshot_retention'"
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        catalog_row is not None and catalog_row["owner_safe"] is True and catalog_row["caller_safe"] is True,
        "publisher is unavailable",
    )
    return int(catalog_row["owner_oid"])


async def _require_no_untrusted_mutation(session, relation_oids, owner_oid, *, sequence=False):
    privilege = (
        "pg_catalog.has_sequence_privilege(principal.oid,relation.oid,'USAGE,UPDATE')"
        if sequence
        else "(pg_catalog.has_table_privilege(principal.oid,relation.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES') "
        "OR pg_catalog.has_any_column_privilege(principal.oid,relation.oid,'INSERT,UPDATE,REFERENCES'))"
    )
    unsafe = await session.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles principal,pg_catalog.pg_class relation "
            "WHERE relation.oid=ANY(:relation_oids) AND " + _ORDINARY_PRINCIPAL_SQL + " AND " + privilege + ")"
        ),
        {"relation_oids": relation_oids, "owner_oid": owner_oid},
    )
    _require(unsafe is False, "ordinary mutation is available")


async def _seal_published_relation(session, relation_oid, owner_oid):
    """Preserve read grants while closing effective ordinary DML and ownership bypasses."""
    _require(await _publisher_authority(session) == owner_oid, "publisher owner differs")
    relation = (
        (
            await session.execute(
                text(
                    "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname) AS name,quote_ident(r.rolname) AS owner,c.relowner "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=:owner WHERE c.oid=:oid"
                ),
                {"owner": owner_oid, "oid": relation_oid},
            )
        )
        .mappings()
        .one()
    )
    if relation["relowner"] == owner_oid:
        await _require_no_untrusted_mutation(session, [relation_oid], owner_oid)
        await _seal_owned_sequences(session, relation_oid, owner_oid, relation["owner"])
        return
    await session.execute(text(f"ALTER TABLE {relation['name']} OWNER TO {relation['owner']}"))
    principals = (
        (
            await session.execute(
                text("SELECT quote_ident(principal.rolname) FROM pg_roles principal WHERE " + _ORDINARY_PRINCIPAL_SQL),
                {"owner_oid": owner_oid},
            )
        )
        .scalars()
        .all()
    )
    columns = (
        (
            await session.execute(
                text(
                    "SELECT quote_ident(attname) FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum"
                ),
                {"oid": relation_oid},
            )
        )
        .scalars()
        .all()
    )
    for principal in ("PUBLIC", *principals):
        await session.execute(
            text(f"REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON {relation['name']} FROM {principal}")
        )
        column_names = ",".join(columns)
        await session.execute(
            text(
                f"REVOKE INSERT ({column_names}),UPDATE ({column_names}),REFERENCES ({column_names}) "
                f"ON {relation['name']} FROM {principal}"
            )
        )
    await _require_no_untrusted_mutation(session, [relation_oid], owner_oid)
    await _seal_owned_sequences(session, relation_oid, owner_oid, relation["owner"])


async def _seal_owned_sequences(session, relation_oid, owner_oid, quoted_owner):
    """Close sequence mutation without changing the immutable table's read grants."""
    sequences = (
        (
            await session.execute(
                text(
                    "SELECT s.oid,quote_ident(n.nspname)||'.'||quote_ident(s.relname) AS name "
                    "FROM pg_depend d JOIN pg_class s ON s.oid=d.objid JOIN pg_namespace n ON n.oid=s.relnamespace "
                    "WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass "
                    "AND d.refobjid=:oid AND d.deptype IN ('a','i') AND s.relkind='S'"
                ),
                {"oid": relation_oid},
            )
        )
        .mappings()
        .all()
    )
    principals = (
        (
            await session.execute(
                text("SELECT quote_ident(principal.rolname) FROM pg_roles principal WHERE " + _ORDINARY_PRINCIPAL_SQL),
                {"owner_oid": owner_oid},
            )
        )
        .scalars()
        .all()
    )
    for sequence in sequences:
        await session.execute(text(f"ALTER SEQUENCE {sequence['name']} OWNER TO {quoted_owner}"))
        for principal in ("PUBLIC", *principals):
            await session.execute(text(f"REVOKE USAGE,UPDATE ON SEQUENCE {sequence['name']} FROM {principal}"))
    await _require_no_untrusted_mutation(session, [sequence["oid"] for sequence in sequences], owner_oid, sequence=True)


async def _require_alias_authority(session, db_schema, owner_oid):
    """Admit only explicitly provisioned, reviewed trigger-only generation authority."""
    try:
        return await alias_guard.require_entity_address_alias_guard(session, schema=db_schema, owner_oid=owner_oid)
    except alias.EntityAddressSnapshotAliasError as error:
        raise EntityAddressSnapshotDestinationError(str(error)) from error


async def require_entity_address_archive_publisher(session, *, db_schema):
    """Fail closed without provisioning or changing ordinary native publication."""
    schema = serving._schema_name(db_schema)
    owner_oid = await _publisher_authority(session)
    await _require_alias_authority(session, schema, owner_oid)
    return owner_oid


async def _authenticated_preparation(session, stored, preparation, validation, authenticate_preparation):
    owner_oid = await _publisher_authority(session)
    _require(callable(authenticate_preparation), "authentication is unavailable")
    proof = await authenticate_preparation(session, preparation, validation)
    state = "frozen" if validation is None else "validated"
    _require(isinstance(proof, Mapping) and proof.get("state") == state, "state differs")
    _require(proof.get("frozen_owner_oid") == owner_oid, "owner differs")
    _require(type(proof.get("builder_oid")) is int and proof["builder_oid"] != owner_oid, "builder differs")
    _require(_digest(proof.get("inventory")) == proof.get("inventory_sha256"), "inventory differs")
    if validation is not None:
        _require(
            isinstance(proof.get("validation"), Mapping) and proof["validation"].get("evidence") == validation,
            "seal differs",
        )
    destination._validated_destination_metadata(stored)
    owner = destination.validate_entity_address_archive_stage_ownership(stored["restored"]["ownership"])
    expected_relations = [{"table_name": name, "relation_oid": oid} for name, oid in owner.relation_oids]
    inventory = proof["inventory"]
    _require(
        inventory.get("schema_name") == owner.schema_name
        and inventory.get("schema_oid") == owner.schema_oid
        and inventory.get("relations") == expected_relations,
        "inventory differs",
    )
    return proof, owner


async def _protected_catalog(session, proof, owner, *, read_only=False):
    """Lock and fingerprint only the exact frozen heaps, indexes and owned sequence."""
    await destination.restore._lock_owned_restore_relations(session, owner, read_only=read_only)
    await destination.verify_entity_address_archive_stage_ownership(session, owner=owner)
    owner_oid = proof["frozen_owner_oid"]
    schema_safe = await session.scalar(
        text(
            "SELECT n.nspowner=:owner_oid AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles principal "
            "WHERE " + _ORDINARY_PRINCIPAL_SQL + " AND pg_catalog.has_schema_privilege(principal.oid,n.oid,'CREATE')) "
            "FROM pg_catalog.pg_namespace n WHERE n.oid=:schema_oid AND n.nspname=:schema"
        ),
        {"owner_oid": owner_oid, "schema_oid": owner.schema_oid, "schema": owner.schema_name},
    )
    _require(schema_safe is True, "private namespace differs")
    oids = [oid for _name, oid in owner.relation_oids]
    catalog_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.oid,c.relowner,c.relkind::text,c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity,"
                    "pg_catalog.pg_relation_filenode(c.oid) AS filenode,"
                    "c.relispartition,EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS unsafe "
                    "FROM pg_catalog.pg_class c WHERE c.oid=ANY(:oids) ORDER BY c.oid"
                ),
                {"oids": oids},
            )
        )
        .mappings()
        .all()
    )
    _require(
        len(catalog_rows) == len(owner.relation_oids)
        and all(
            catalog_row["relowner"] == owner_oid
            and catalog_row["relkind"] == "r"
            and catalog_row["relpersistence"] == "p"
            and not any(
                catalog_row[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "unsafe")
            )
            for catalog_row in catalog_rows
        ),
        "candidate catalog differs",
    )
    await _require_no_untrusted_mutation(session, oids, owner_oid)
    sequence = await _protected_sequence(session, proof, owner)
    return _digest(
        {
            "relations": [dict(catalog_row) for catalog_row in catalog_rows],
            "sequence": sequence,
            "schema": await _candidate_schema_identity(session, owner),
        }
    )


async def _candidate_schema_identity(session, owner):
    """Bind local index identities as well as the portable, normalized schema shapes."""
    receipt = destination.restore.importlib.import_module("process.entity_address_snapshot_receipt")
    is_set = alias.AUTHORITY_TABLE in dict(owner.relation_oids)
    index_state = ",i.indisvalid,i.indisready,i.indislive" if is_set else ""
    shapes = []
    async with destination.db.bind_existing_session(session), destination._preserve_receipt_settings():
        await receipt._normalize_receipt_session(session, owner.schema_name)
        for name, oid in owner.relation_oids:
            shapes.append([name, oid, await receipt._schema_identity(session, oid, owner.schema_name, name)])
    indexes = (
        (
            await session.execute(
                text(
                    "SELECT i.indexrelid AS oid,i.indrelid,c.relname,c.relnamespace,c.relowner,"
                    "pg_catalog.pg_relation_filenode(c.oid) AS filenode" + index_state + " FROM pg_catalog.pg_index i "
                    "JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=ANY(:oids) ORDER BY i.indexrelid"
                ),
                {"oids": [oid for _name, oid in owner.relation_oids]},
            )
        )
        .mappings()
        .all()
    )
    if is_set:
        _require(
            all(entry["indisvalid"] and entry["indisready"] and entry["indislive"] for entry in indexes),
            "candidate indexes are incomplete",
        )
    return {"shapes": shapes, "indexes": [dict(catalog_row) for catalog_row in indexes]}


async def _protected_sequence(session, proof, owner):
    catalog_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.oid,c.relname,c.relowner,c.relnamespace,d.refobjid,a.attname "
                    "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_depend d "
                    "ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.refclassid='pg_class'::regclass "
                    "AND d.deptype IN ('a','i') JOIN pg_catalog.pg_attribute a "
                    "ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid WHERE c.relkind='S' AND d.refobjid=ANY(:oids)"
                ),
                {"oids": [oid for _name, oid in owner.relation_oids]},
            )
        )
        .mappings()
        .all()
    )
    _require(len(catalog_rows) == 1, "sequence differs")
    sequence = catalog_rows[0]
    expected_sequence_by_field = {
        "sequence_name": _EVIDENCE_SEQUENCE,
        "sequence_oid": sequence["oid"],
        "owner_table": "entity_address_evidence",
        "owner_column": "evidence_id",
    }
    _require(
        sequence["relname"] == _EVIDENCE_SEQUENCE
        and sequence["relowner"] == proof["frozen_owner_oid"]
        and sequence["relnamespace"] == owner.schema_oid
        and sequence["attname"] == "evidence_id"
        and sequence["refobjid"] == dict(owner.relation_oids)["entity_address_evidence"]
        and proof["inventory"].get("sequences") == [expected_sequence_by_field],
        "sequence differs",
    )
    await _require_no_untrusted_mutation(session, [sequence["oid"]], proof["frozen_owner_oid"], sequence=True)
    return expected_sequence_by_field


async def _move_to_destination(session, stored, owner):
    schema, _date, names = destination.restore._stage_plan(
        db_schema=stored["restored"]["db_schema"], import_date=stored["restored"]["import_date"]
    )
    moved = await destination.restore._move_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    expected_oids = tuple(
        (catalog_row["table_name"], catalog_row["oid"]) for catalog_row in stored["restored"]["stage_relation_oids"]
    )
    _require(moved == expected_oids, "relocation differs")
    return schema, names


async def validate_entity_address_archive_preparation(
    session,
    *,
    stored,
    preparation,
    authenticate_preparation,
):
    """Fully scan a frozen private candidate; the caller durably authenticates the seal."""
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    _require(stored.get("contract") != archive_contract, "v2 requires authenticated publisher load")
    proof, owner = await _authenticated_preparation(session, stored, preparation, None, authenticate_preparation)
    schema = stored["restored"]["db_schema"]
    await alias._lock_alias_relations(session, schema)
    await _require_alias_authority(session, schema, proof["frozen_owner_oid"])
    await _protected_catalog(session, proof, owner)
    async with destination.db.bind_existing_session(session):
        schema, names = await _move_to_destination(session, stored, owner)
        await destination._validated_bound_destination(session, stored=stored)
        await destination.restore._return_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    return {
        "contract": CONTRACT,
        "stored_sha256": _digest(stored),
        "inventory_sha256": proof["inventory_sha256"],
        "protected_owner_oid": proof["frozen_owner_oid"],
        "builder_oid": proof["builder_oid"],
        "catalog_sha256": await _protected_catalog(session, proof, owner),
        "alias": dict(stored["destination_alias_receipt"]),
        "alias_catalog_sha256": await _require_alias_authority(session, schema, proof["frozen_owner_oid"]),
    }


def _require_seal(stored, proof, validation):
    fields = {
        "contract",
        "stored_sha256",
        "inventory_sha256",
        "protected_owner_oid",
        "builder_oid",
        "catalog_sha256",
        "alias",
        "alias_catalog_sha256",
    }
    _require(
        isinstance(validation, Mapping) and set(validation) == fields and validation["contract"] == CONTRACT,
        "seal is invalid",
    )
    _require(
        validation["stored_sha256"] == _digest(stored)
        and validation["inventory_sha256"] == proof["inventory_sha256"]
        and validation["protected_owner_oid"] == proof["frozen_owner_oid"]
        and validation["builder_oid"] == proof["builder_oid"]
        and validation["alias"] == stored["destination_alias_receipt"],
        "seal differs",
    )


async def _require_alias_fence(session, schema, proof, validation):
    await alias._lock_alias_relations(session, schema)
    _require(
        await _require_alias_authority(session, schema, proof["frozen_owner_oid"])
        == validation["alias_catalog_sha256"],
        "alias catalog changed",
    )
    expected = alias.validate_entity_address_alias_semantic_receipt(validation["alias"])
    current = await alias._alias_state(session, schema)
    _require(
        current == (expected.alias_schema_version, expected.active_ruleset_version, expected.local_generation),
        "alias generation changed",
    )


def _prepared_adoption(stored):
    schema, date, _names, _oids, _integrity, context, validation = destination.restore._validated_rehydration_state(
        stored["restored"]
    )
    stage, support, swaps, patches, relations, required = destination.adoption._prepared_full_result_stage(
        db_schema=schema, import_date=date
    )
    return destination.adoption.PreparedEntityAddressSnapshotAdoption(
        db_schema=schema,
        stage_cls=stage,
        support_stage_class_map=support,
        swaps=swaps,
        patch_statements=patches,
        relation_names=relations,
        required_names=required,
        context=context,
        publish_validation=validation,
    )


_CMS_RECEIPT_TABLE = "provider_directory_cms_serving_receipt"


def _cms_address_authority(alias):
    fields = ("local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation", "relation_oids")
    return (
        "jsonb_build_object("
        + ",".join(f"'{field}',{alias}.{field}" for field in fields)
        + ")"
        + f" || jsonb_build_object('published_at',to_char({alias}.published_at AT TIME ZONE 'UTC',"
        + '\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'))'
    )


def _cms_guard_bodies(schema):
    """Recognize the unchanged canonical migration SQL, including its receipt prerequisite."""
    old, new = _cms_address_authority("OLD"), _cms_address_authority("NEW")
    return {
        "cms_serving_address_transition": f"""BEGIN
            IF TG_OP='UPDATE' AND ({old}) IS NOT DISTINCT FROM ({new}) THEN RETURN NULL; END IF;
            IF (false) OR EXISTS (SELECT 1 FROM "{schema}".{_CMS_RECEIPT_TABLE}) THEN
                PERFORM "{schema}".cms_serving_require_fresh('address',CASE WHEN TG_OP='INSERT' THEN NULL ELSE {old} END);
            END IF;
            RETURN NULL; END""",
        "cms_serving_no_truncate": f"""BEGIN
            IF EXISTS (SELECT 1 FROM "{schema}".{_CMS_RECEIPT_TABLE}) THEN
                RAISE EXCEPTION 'cms_serving_native_truncate_forbidden'; END IF; RETURN NULL; END""",
    }


async def _lock_empty_cms_receipts(session, schema, receipt_oid):
    """Pin an inert heap and exclude inserts until deferred transition guards have fired."""
    await session.execute(text(f'LOCK TABLE ONLY "{schema}"."{_CMS_RECEIPT_TABLE}" IN SHARE MODE NOWAIT'))
    safe = await session.scalar(
        text(
            "SELECT c.oid=:oid AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition "
            "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid) "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type ty ON ty.oid=a.atttypid "
            "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
            "AND (ty.typnamespace<>'pg_catalog'::regnamespace OR a.attgenerated NOT IN ('','s'))) "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i "
            "JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=idx.relam, "
            "pg_catalog.unnest(i.indclass) cls(oid) JOIN pg_catalog.pg_opclass op ON op.oid=cls.oid "
            "WHERE i.indrelid=c.oid AND (am.amname<>'btree' OR op.opcnamespace<>'pg_catalog'::regnamespace "
            "OR (i.indexprs IS NOT NULL AND pg_catalog.pg_get_expr(i.indexprs,i.indrelid)<>'true') "
            "OR (i.indpred IS NOT NULL AND pg_catalog.pg_get_expr(i.indpred,i.indrelid)<>'(predecessor_receipt_id IS NULL)'))) "
            "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=:schema AND c.relname=:name"
        ),
        {"oid": receipt_oid, "schema": schema, "name": _CMS_RECEIPT_TABLE},
    )
    _require(safe is True, "CMS receipt catalog is unsupported")
    populated = await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM "{schema}"."{_CMS_RECEIPT_TABLE}")'))
    _require(populated is False, "populated CMS receipt history requires composite publication")


_CMS_GUARDS_SQL = """
SELECT t.oid AS trigger_oid,t.tgconstraint AS constraint_oid,t.tgname,p.prosrc,
 t.tgparentid=0 AND t.tgconstrrelid=0 AND t.tgconstrindid=0 AND t.tgenabled IN ('O','A')
 AND t.tgnargs=0 AND t.tgargs=''::bytea AND t.tgqual IS NULL AND t.tgattr=''::int2vector
 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
 AND p.pronamespace=c.relnamespace AND p.proname=t.tgname AND p.pronargs=0 AND p.pronargdefaults=0
 AND p.provariadic=0 AND p.proargtypes=''::oidvector AND p.proallargtypes IS NULL
 AND p.proargmodes IS NULL AND p.proargnames IS NULL AND NOT p.proretset
 AND p.prokind='f' AND p.prorettype='pg_catalog.trigger'::regtype AND NOT p.prosecdef
 AND NOT p.proisstrict AND NOT p.proleakproof AND p.prosupport=0
 AND p.provolatile='v' AND p.proparallel='u' AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
 AND language.lanname='plpgsql'
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles principal WHERE {ordinary}
     AND pg_catalog.pg_has_role(principal.oid,p.proowner,'MEMBER'))
 AND ((t.tgname='cms_serving_no_truncate' AND t.tgtype=34 AND t.tgconstraint=0
       AND NOT t.tgdeferrable AND NOT t.tginitdeferred)
  OR (t.tgname='cms_serving_address_transition' AND t.tgtype=29 AND t.tgdeferrable AND t.tginitdeferred
      AND k.contype='t' AND k.conname=t.tgname AND k.conrelid=c.oid AND k.contypid=0
      AND k.conindid=0 AND k.confrelid=0 AND k.conparentid=0 AND k.condeferrable AND k.condeferred
      AND k.convalidated AND k.conislocal AND k.coninhcount=0 AND k.conkey IS NULL
      AND k.confkey IS NULL AND k.conbin IS NULL)) AS safe
FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_language language ON language.oid=p.prolang
LEFT JOIN pg_catalog.pg_constraint k ON k.oid=t.tgconstraint
WHERE n.nspname=:schema AND c.relname='entity_address_result_generation' AND NOT t.tgisinternal
ORDER BY t.tgname
""".format(ordinary=_ORDINARY_PRINCIPAL_SQL)


async def _cms_receipt_guard_oids(session, schema, owner_oid):
    """Return only attested hook OIDs; legacy absence never exempts any trigger."""
    receipt_oid = await session.scalar(
        text("SELECT pg_catalog.to_regclass(:relation)::oid"),
        {"relation": f'"{schema}"."{_CMS_RECEIPT_TABLE}"'},
    )
    if receipt_oid is None:
        return [], []
    await _lock_empty_cms_receipts(session, schema, receipt_oid)
    catalog_rows = (
        (await session.execute(text(_CMS_GUARDS_SQL), {"schema": schema, "owner_oid": owner_oid})).mappings().all()
    )
    expected = _cms_guard_bodies(schema)
    _require(
        len(catalog_rows) == len(expected)
        and {catalog_row["tgname"] for catalog_row in catalog_rows} == set(expected)
        and all(
            catalog_row["safe"] is True
            and " ".join(catalog_row["prosrc"].split()) == " ".join(expected[catalog_row["tgname"]].split())
            for catalog_row in catalog_rows
        ),
        "CMS publication guards are unsupported",
    )
    return (
        [catalog_row["trigger_oid"] for catalog_row in catalog_rows],
        [catalog_row["constraint_oid"] for catalog_row in catalog_rows if catalog_row["constraint_oid"]],
    )


async def _lock_publication_state(session, schema, owner_oid):
    """Drain ordinary writers before rejecting executable hooks on native state updates."""
    names = (destination.geo_projection.GEO_ASSURANCE_STATE_TABLE, serving.result_generation.TABLE_NAME)
    await session.execute(
        text(
            "LOCK TABLE " + ", ".join(f'"{schema}"."{name}"' for name in names) + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
        )
    )
    guard_triggers, guard_constraints = await _cms_receipt_guard_oids(session, schema, owner_oid)
    catalog_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.oid,c.relkind::text,c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,"
                    "EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal "
                    "AND t.oid<>ALL(CAST(:guard_triggers AS oid[]))) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type ty ON ty.oid=a.atttypid "
                    "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND ty.typnamespace<>'pg_catalog'::regnamespace) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid AND k.contype NOT IN ('p','u','n','c') "
                    "AND k.oid<>ALL(CAST(:guard_constraints AS oid[]))) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE "
                    "((d.classid='pg_constraint'::regclass AND d.objid IN (SELECT oid FROM pg_catalog.pg_constraint WHERE conrelid=c.oid)) "
                    "OR (d.classid='pg_attrdef'::regclass AND d.objid IN (SELECT oid FROM pg_catalog.pg_attrdef WHERE adrelid=c.oid))) "
                    "AND (d.refclassid='pg_proc'::regclass OR d.refclassid='pg_operator'::regclass)) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_index i,pg_catalog.unnest(i.indclass) cls(oid) "
                    "JOIN pg_catalog.pg_opclass op ON op.oid=cls.oid WHERE i.indrelid=c.oid "
                    "AND (op.opcnamespace<>'pg_catalog'::regnamespace OR i.indexprs IS NOT NULL OR i.indpred IS NOT NULL)) AS unsafe "
                    "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relname=ANY(:names)"
                ),
                {
                    "schema": schema,
                    "names": list(names),
                    "guard_triggers": guard_triggers,
                    "guard_constraints": guard_constraints,
                },
            )
        )
        .mappings()
        .all()
    )
    _require(
        len(catalog_rows) == 2
        and all(
            catalog_row["relkind"] == "r"
            and catalog_row["relpersistence"] == "p"
            and not any(
                catalog_row[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "unsafe")
            )
            for catalog_row in catalog_rows
        ),
        "publication state catalog is unsupported",
    )


async def _restore_builder_owner(session, proof, owner, schema, names):
    builder = (
        (
            await session.execute(
                text(
                    "SELECT pg_catalog.quote_ident(rolname) AS name,rolcanlogin AND NOT rolsuper AND NOT rolcreaterole "
                    "AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls "
                    "AND NOT pg_catalog.pg_has_role(oid,CAST(:owner_oid AS oid),'MEMBER') AS safe FROM pg_catalog.pg_roles WHERE oid=:builder"
                ),
                {"owner_oid": proof["frozen_owner_oid"], "builder": proof["builder_oid"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(builder is not None and builder["safe"] is True, "builder authority changed")
    for name, _oid in owner.relation_oids:
        await session.execute(text(f'ALTER TABLE "{schema}"."{names[name]}" OWNER TO {builder["name"]}'))
    sequence_name = names["entity_address_evidence"] + "_evidence_id_seq"
    await session.execute(text(f'ALTER SEQUENCE "{schema}"."{sequence_name}" OWNER TO {builder["name"]}'))
    await destination.restore._drop_empty_owned_schema(session, owner)


async def activate_validated_entity_address_archive_destination(
    session,
    *,
    stored,
    validation,
    preparation,
    expected_incumbent,
    callbacks,
    authenticate_preparation,
):
    """Publish an authenticated frozen candidate without rescanning candidate or alias rows."""
    if isinstance(validation, Mapping) and validation.get("contract") == SET_CONTRACT:
        return await _activate_set_validated_destination(
            session,
            stored=stored,
            validation=validation,
            preparation=preparation,
            expected_incumbent=expected_incumbent,
            callbacks=callbacks,
            authenticate_preparation=authenticate_preparation,
        )
    proof, owner = await _authenticated_preparation(session, stored, preparation, validation, authenticate_preparation)
    _require_seal(stored, proof, validation)
    schema = stored["restored"]["db_schema"]
    await serving.require_entity_address_receive_incumbent(session, schema_name=schema, expected=expected_incumbent)
    await _require_alias_fence(session, schema, proof, validation)
    await _lock_publication_state(session, schema, proof["frozen_owner_oid"])
    _require(await _protected_catalog(session, proof, owner) == validation["catalog_sha256"], "catalog seal differs")
    async with destination.db.bind_existing_session(session):
        schema, names = await _move_to_destination(session, stored, owner)
        prepared = _prepared_adoption(stored)
        dependency_bindings = await _resolve_dependency_bindings(session, schema, prepared)
        expected_geo = destination._validated_geo_preparation(stored["geo_assurance"], db_schema=schema)
        actual_geo = await destination._capture_geo_preparation(
            session,
            db_schema=schema,
            stage_table_oid=expected_geo.stage_table_oid,
            projected_rows=expected_geo.projected_rows,
            dependency_bindings=dependency_bindings,
        )
        _require(actual_geo == expected_geo, "geo assurance changed")
        await _restore_builder_owner(session, proof, owner, schema, names)
        return await destination.adoption.adopt_prepared_entity_address_snapshot(prepared, callbacks=callbacks)


async def _resolve_dependency_bindings(session, schema, prepared):
    """Resolve grouped dependency renames and update the same candidate with compare-and-swap."""
    from process.entity_address_dependency_bindings import resolve_prepared_bindings

    expected_bindings = prepared.context.get("dependency_bindings")
    dependency_bindings = await resolve_prepared_bindings(session, schema, expected_bindings)
    if dependency_bindings != expected_bindings:
        changed = await session.scalar(
            text(
                f'UPDATE "{schema}".entity_address_geo_assurance_state '
                "SET candidate_dependency_bindings=CAST(:bindings AS jsonb) "
                "WHERE singleton IS TRUE AND candidate_dependency_bindings=CAST(:expected AS jsonb) RETURNING singleton"
            ),
            {"bindings": json.dumps(dependency_bindings), "expected": json.dumps(expected_bindings)},
        )
        _require(changed is True, "dependency bindings changed")
    return dependency_bindings


def _loaded_input(owner, semantic_receipt, source_alias_receipt, destination_payload):
    return {
        "ownership": owner.as_dict(),
        "semantic_receipt": semantic_receipt.as_dict(),
        "source_alias_receipt": source_alias_receipt.as_dict(),
        "destination": dict(destination_payload),
    }


async def prepare_loaded_entity_address_archive_destination(
    session,
    *,
    owner,
    semantic_receipt,
    source_alias_receipt,
    destination,
    authenticate_preparation,
    publisher_selected_inputs=None,
    authenticate_selected_mrf=None,
):
    """Trusted approved-byte heap load, transforms, indexes and set checks on one session."""
    from process import entity_address_snapshot_destination as native
    from process.entity_address_dependency_bindings import (
        lock_publisher_selected_inputs,
        validate_publisher_selected_inputs,
    )

    owner, semantic, source_alias = _validated_loaded_input(owner, semantic_receipt, source_alias_receipt, destination)
    selected = (
        None
        if publisher_selected_inputs is None
        else validate_publisher_selected_inputs(destination["db_schema"], publisher_selected_inputs)
    )
    _require(
        authenticate_selected_mrf is None or (selected is not None and callable(authenticate_selected_mrf)),
        "selected MRF authentication differs",
    )
    owner_oid = await _publisher_authority(session)
    proof = await authenticate_preparation(session)
    _require_loaded_inventory(proof, owner, owner_oid)
    schema, *_ = native.restore._stage_plan(db_schema=destination["db_schema"], import_date=destination["import_date"])
    _require(schema != owner.schema_name, "destination differs")
    await _require_alias_authority(session, schema, owner_oid)
    await _protected_catalog(session, proof, owner)
    if selected is not None:
        selected = await lock_publisher_selected_inputs(session, schema, selected)
    destination_alias = await alias.capture_entity_address_alias_authority_receipt(session, schema_name=schema)
    async with native.db.bind_existing_session(session):
        prepared = await _prepare_loaded_sets(
            session,
            owner,
            semantic,
            source_alias,
            destination_alias,
            destination,
            selected,
            authenticate_selected_mrf,
        )
    return await _loaded_validation(
        session,
        prepared,
        proof,
        owner,
        _loaded_input(owner, semantic, source_alias, destination),
        destination_alias,
        selected,
    )


async def _loaded_validation(session, prepared, proof, owner, original_input, destination_alias, selected):
    """Seal the actual indexed candidate while retaining its original dependency input."""
    schema = original_input["destination"]["db_schema"]
    stored = prepared.as_dict()
    validation_by_field = {
        "contract": SET_CONTRACT,
        "prepared": stored,
        "input_sha256": _digest(original_input),
        "stored_sha256": _digest(stored),
        "inventory_sha256": proof["inventory_sha256"],
        "protected_owner_oid": proof["frozen_owner_oid"],
        "builder_oid": proof["builder_oid"],
        "catalog_sha256": await _protected_catalog(session, proof, owner),
        "alias": destination_alias.as_dict(),
        "alias_catalog_sha256": await _require_alias_authority(session, schema, proof["frozen_owner_oid"]),
    }
    if selected is not None:
        validation_by_field["publisher_selected_inputs_sha256"] = _digest(selected)
    return validation_by_field


def _validated_loaded_input(owner, semantic_receipt, source_alias_receipt, destination_options):
    """Validate approved immutable input without replacing its dependency bounds."""
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    owner = destination.validate_entity_address_archive_stage_ownership(owner)
    semantic = destination.validate_entity_address_archive_receipt(semantic_receipt)
    source_alias = alias.validate_entity_address_alias_semantic_receipt(source_alias_receipt)
    _require(semantic.contract == archive_contract and source_alias.contract == alias.SET_CONTRACT, "contracts differ")
    alias_table = next(entry for entry in semantic.tables if entry.table_name == alias.AUTHORITY_TABLE)
    _require(alias_table.row_count == source_alias.active_alias_count, "source alias accounting differs")
    _require(
        isinstance(destination_options, Mapping)
        and {"db_schema", "import_date"} <= set(destination_options)
        and set(destination_options)
        <= {"db_schema", "import_date", "source_serving_generation", "dependency_bindings"},
        "destination differs",
    )
    return owner, semantic, source_alias


def _require_loaded_inventory(proof, owner, owner_oid):
    """Bind authenticated custody to the exact eight original input OIDs."""
    _require(
        isinstance(proof, Mapping)
        and proof.get("state") == "frozen"
        and proof.get("frozen_owner_oid") == owner_oid
        and type(proof.get("builder_oid")) is int
        and proof["builder_oid"] > 0
        and proof["builder_oid"] != owner_oid,
        "frozen authority differs",
    )
    inventory = proof.get("inventory")
    _require(
        isinstance(inventory, Mapping)
        and inventory.get("schema_name") == owner.schema_name
        and inventory.get("schema_oid") == owner.schema_oid
        and inventory.get("relations")
        == [{"table_name": name, "relation_oid": oid} for name, oid in owner.relation_oids]
        and _digest(inventory) == proof.get("inventory_sha256")
        and len(owner.relation_oids) == 8
        and alias.AUTHORITY_TABLE in dict(owner.relation_oids),
        "inventory differs",
    )


async def _prepare_loaded_sets(
    session,
    owner,
    semantic,
    source_alias,
    destination_alias,
    destination_options,
    selected=None,
    authenticate_selected_mrf=None,
):
    """Apply documented local transforms, complete indexes and validate the isolated candidate."""
    from process import entity_address_snapshot_destination as native
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract
    from process.entity_address_snapshot_receipt import STAGE_INTEGRITY_CONTRACT

    schema, date = destination_options["db_schema"], destination_options["import_date"]
    remap, geo, bindings, stage_oids, names = await _complete_loaded_heaps(
        session, owner, semantic, source_alias, destination_alias, destination_options, selected
    )
    prepared = await _audit_loaded_adoption(session, destination_options, selected, authenticate_selected_mrf)
    prepared.context.update(snapshot_contract=archive_contract)
    if bindings is not None:
        prepared.context["dependency_bindings"] = bindings
    if selected is not None:
        prepared.context["publisher_selected_inputs"] = selected
    integrity = await native.capture_entity_address_stage_integrity_receipt(
        session, schema_name=schema, stage_table_names=names, contract=STAGE_INTEGRITY_CONTRACT
    )
    restored = native._prepared_restore_receipt(
        validated_owner=owner,
        normalized_date=date,
        stage_oids=stage_oids,
        post_remap_receipt=semantic,
        stage_integrity=integrity,
        prepared=prepared,
    )
    await native.restore._return_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    return native.PreparedEntityAddressSnapshotDestination(
        restored, semantic, source_alias, destination_alias, remap, geo
    )


async def _complete_loaded_heaps(
    session, owner, semantic, source_alias, destination_alias, destination_options, selected
):
    """Derive model stage names, transform heaps, and complete all indexes before set audit."""
    native = destination
    schema, date, names = native.restore._stage_plan(
        db_schema=destination_options["db_schema"], import_date=destination_options["import_date"]
    )
    remap, _ = await native._remap_base_versions(
        session,
        schema_name=owner.schema_name,
        source_alias=source_alias,
        destination_alias=destination_alias,
        pre_remap_receipt=semantic,
    )
    await native.restore._reset_restored_evidence_sequence(session, owner)
    stage_oids = await native.restore._move_owned_relations(
        session, owner=owner, db_schema=schema, stage_names=names, heaps=True
    )
    projection_options_by_field = dict(destination_options)
    if selected is not None:
        projection_options_by_field["dependency_bindings"] = selected["dependency_bindings"]
    geo, bindings = await _project_loaded_geo(
        session, semantic, projection_options_by_field, names, stage_oids, selected
    )
    await native.restore._return_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    await native.restore.complete_entity_address_archive_restore(
        session, owner=owner, db_schema=schema, import_date=date
    )
    await native.restore._actual_receipt(session, schema_name=owner.schema_name, expected=semantic)
    await alias.require_matching_entity_address_alias_authority(
        session, authority_schema=owner.schema_name, alias_schema=schema
    )
    await native.restore._move_owned_relations(session, owner=owner, db_schema=schema, stage_names=names)
    return remap, geo, bindings, stage_oids, names


async def _audit_loaded_adoption(session, destination_options, selected, authenticate_selected_mrf):
    """Audit future archive coverage only after the isolated candidate's indexes are complete."""
    native = destination
    schema, date = destination_options["db_schema"], destination_options["import_date"]
    if authenticate_selected_mrf is None:
        archive_scope = nullcontext(None)
    else:
        archive_scope = native.restore.family_archive.selected_canonical_archive(
            session,
            schema_name=schema,
            owner_oid=await _publisher_authority(session),
            selected_relations={
                "npi": selected["dependency_bindings"][f"{schema}.npi_address"],
                "mrf": selected["dependency_bindings"][f"{schema}.mrf_address"],
            },
            authenticate_inventory=authenticate_selected_mrf,
        )
    async with archive_scope as archive_relation:
        return await native.adoption.prepare_completed_entity_address_snapshot_adoption(
            db_schema=schema,
            import_date=date,
            preserve_unversioned_base_rows=True,
            source_serving_generation=destination_options.get("source_serving_generation"),
            **({} if archive_relation is None else {"archive_relation": archive_relation}),
        )


async def _project_loaded_geo(session, semantic, destination_options, names, stage_oids, selected=None):
    """Derive geo fields once from the explicitly locked local dependency set."""
    native = destination
    schema = destination_options["db_schema"]
    main = names[native.entity_address_unified.EntityAddressUnified.__tablename__]
    main_rows = native._main_table_receipt(semantic).row_count
    bindings = destination_options.get("dependency_bindings")
    if bindings is not None:
        bindings = native.geo_projection.validate_projection_dependency_bindings(schema, bindings)
    options = {} if bindings is None else {"dependency_bindings": bindings}
    projected = await native.entity_address_unified._materialize_geo_assurance(
        schema, main, force=True, context={}, run_id="", stage_rows=main_rows, **options
    )
    _require(projected == main_rows, "geo projection accounting differs")
    geo = await native._capture_geo_preparation(
        session,
        db_schema=schema,
        stage_table_oid=dict(stage_oids)[main],
        projected_rows=projected,
        publisher_selected_inputs_sha256=None if selected is None else _digest(selected),
        **options,
    )
    return geo, bindings


async def _activate_set_validated_destination(
    session, *, stored, validation, preparation, expected_incumbent, callbacks, authenticate_preparation
):
    """Recheck the immutable seal and publish all seven serving OIDs in the caller transaction."""
    prepared_stored = _validated_set_prepared(validation)
    proof, owner = await _authenticated_preparation(
        session, prepared_stored, preparation, validation, authenticate_preparation
    )
    _require_set_input(stored, validation, prepared_stored, proof, owner)
    schema = prepared_stored["restored"]["db_schema"]
    await serving.require_entity_address_receive_incumbent(session, schema_name=schema, expected=expected_incumbent)
    await _require_alias_fence(session, schema, proof, validation)
    await _lock_publication_state(session, schema, proof["frozen_owner_oid"])
    _require(await _protected_catalog(session, proof, owner) == validation["catalog_sha256"], "catalog seal differs")
    async with destination.db.bind_existing_session(session):
        schema, _names = await _move_to_destination(session, prepared_stored, owner)
        prepared = _prepared_adoption(prepared_stored)
        prepared.context["protected_owner_oid"] = proof["frozen_owner_oid"]
        prepared.context["protected_stage_oids"] = {
            entry["table_name"]: entry["oid"] for entry in prepared_stored["restored"]["stage_relation_oids"]
        }
        dependency_bindings = await _resolve_dependency_bindings(session, schema, prepared)
        expected_geo = destination._validated_geo_preparation(prepared_stored["geo_assurance"], db_schema=schema)
        actual_geo = await destination._capture_geo_preparation(
            session,
            db_schema=schema,
            stage_table_oid=expected_geo.stage_table_oid,
            projected_rows=expected_geo.projected_rows,
            dependency_bindings=dependency_bindings,
            publisher_selected_inputs_sha256=expected_geo.publisher_selected_inputs_sha256,
        )
        _require(actual_geo == expected_geo, "geo assurance changed")
        published = await destination.adoption.adopt_prepared_entity_address_snapshot(prepared, callbacks=callbacks)
        return await _publication_receipt(session, schema, owner, prepared, published)


def _validated_set_prepared(validation):
    """Reject mixed or open-ended validation envelopes before any database work."""
    fields = {
        "contract",
        "prepared",
        "input_sha256",
        "stored_sha256",
        "inventory_sha256",
        "protected_owner_oid",
        "builder_oid",
        "catalog_sha256",
        "alias",
        "alias_catalog_sha256",
    }
    _require(
        isinstance(validation, Mapping)
        and set(validation) in (fields, fields | {"publisher_selected_inputs_sha256"})
        and validation["contract"] == SET_CONTRACT,
        "v2 seal differs",
    )
    prepared_stored = validation["prepared"]
    _require(
        isinstance(prepared_stored, Mapping) and prepared_stored.get("contract") == destination.SET_CONTRACT,
        "prepared contract differs",
    )
    _require_selected_inputs_seal(prepared_stored, validation)
    return prepared_stored


def _require_selected_inputs_seal(prepared, validation):
    """Bind the separate immutable selection to both native context and geo evidence."""
    from process.entity_address_dependency_bindings import validate_publisher_selected_inputs

    context = prepared.get("restored", {}).get("context", {})
    geo = prepared.get("geo_assurance", {})
    selected = context.get("publisher_selected_inputs")
    if not any(
        (
            selected is not None,
            "publisher_selected_inputs_sha256" in validation,
            "publisher_selected_inputs_sha256" in geo,
        )
    ):
        return
    try:
        selected = validate_publisher_selected_inputs(prepared["restored"]["db_schema"], selected)
    except (ValueError, KeyError, TypeError) as error:
        raise EntityAddressSnapshotDestinationError("entity-address selected input seal differs") from error
    _require(
        context.get("dependency_bindings") == selected["dependency_bindings"]
        and validation.get("publisher_selected_inputs_sha256") == _digest(selected)
        and geo.get("publisher_selected_inputs_sha256") == _digest(selected),
        "selected input seal differs",
    )


def _require_set_input(stored, validation, prepared_stored, proof, owner):
    """Bind completed authority to the unchanged approved input and local frozen inventory."""
    from process.entity_address_snapshot_receipt import CONTRACT as archive_contract

    _require_selected_inputs_seal(prepared_stored, validation)
    _require(
        _digest(prepared_stored) == validation["stored_sha256"]
        and validation["inventory_sha256"] == proof["inventory_sha256"]
        and validation["protected_owner_oid"] == proof["frozen_owner_oid"]
        and validation["builder_oid"] == proof["builder_oid"]
        and validation["alias"] == prepared_stored["destination_alias_receipt"],
        "v2 seal differs",
    )
    _require(
        isinstance(stored, Mapping)
        and set(stored) == {"contract", "ownership", "semantic_receipt", "source_alias_receipt", "destination"}
        and stored.get("contract") == archive_contract,
        "input contract differs",
    )
    semantic = destination.validate_entity_address_archive_receipt(stored["semantic_receipt"])
    source_alias = alias.validate_entity_address_alias_semantic_receipt(stored["source_alias_receipt"])
    input_owner = destination.validate_entity_address_archive_stage_ownership(stored["ownership"])
    _require(
        input_owner == owner
        and semantic.as_dict() == prepared_stored["source_semantic_receipt"]
        and source_alias.as_dict() == prepared_stored["source_alias_receipt"]
        and validation["input_sha256"] == _digest(_loaded_input(owner, semantic, source_alias, stored["destination"])),
        "input seal differs",
    )
    context = prepared_stored.get("restored", {}).get("context", {})
    if "publisher_selected_inputs" not in context:
        _require(
            context.get("dependency_bindings") == stored["destination"].get("dependency_bindings"),
            "ordinary dependency bounds differ",
        )


async def _publication_receipt(session, schema, owner, prepared, result):
    relations = []
    for model in destination.restore._models():
        oid = await session.scalar(
            text("SELECT to_regclass(:relation)::oid"), {"relation": f'"{schema}"."{model.__tablename__}"'}
        )
        _require(oid == dict(owner.relation_oids)[model.__tablename__], "published OID differs")
        relations.append({"table_name": model.__tablename__, "relation_oid": oid})
    return {
        **result,
        "publication": {
            "contract": "entity-address-table-publication.v2",
            "schema_name": schema,
            "relations": relations,
            "retained_relations": prepared.context.get("retained_relations", []),
            "alias_authority": {
                "schema_name": owner.schema_name,
                "schema_oid": owner.schema_oid,
                "relation_oid": dict(owner.relation_oids)[alias.AUTHORITY_TABLE],
            },
        },
    }


__all__ = [
    "prepare_loaded_entity_address_archive_destination",
    "EntityAddressSnapshotDestinationError",
    "require_entity_address_archive_publisher",
    "prepare_private_entity_address_archive_destination",
    "validate_entity_address_archive_preparation",
    "activate_validated_entity_address_archive_destination",
]
