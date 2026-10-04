# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate frozen address candidates separately from bounded native publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from sqlalchemy import text

from process import entity_address_alias_guard as alias_guard
from process import entity_address_snapshot_alias as alias
from process import entity_address_snapshot_destination as destination
from process import entity_address_snapshot_serving as serving

EntityAddressSnapshotDestinationError = destination.EntityAddressSnapshotDestinationError
CONTRACT = "entity_address_archive_preparation.postgres.v1"
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


async def _protected_catalog(session, proof, owner):
    """Lock and fingerprint only the exact frozen heaps, indexes and owned sequence."""
    await destination.restore._lock_owned_restore_relations(session, owner)
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
        len(catalog_rows) == 7
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
                    "pg_catalog.pg_relation_filenode(c.oid) AS filenode FROM pg_catalog.pg_index i "
                    "JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=ANY(:oids) ORDER BY i.indexrelid"
                ),
                {"oids": [oid for _name, oid in owner.relation_oids]},
            )
        )
        .mappings()
        .all()
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
        from process.entity_address_dependency_bindings import resolve_prepared_bindings

        expected_bindings = prepared.context.get("dependency_bindings")
        dependency_bindings = await resolve_prepared_bindings(session, schema, expected_bindings)
        if dependency_bindings != expected_bindings:
            changed = await session.scalar(text(
                f'UPDATE "{schema}".entity_address_geo_assurance_state '
                "SET candidate_dependency_bindings=CAST(:bindings AS jsonb) "
                "WHERE singleton IS TRUE AND candidate_dependency_bindings=CAST(:expected AS jsonb) "
                "RETURNING singleton"
            ), {"bindings": json.dumps(dependency_bindings), "expected": json.dumps(expected_bindings)})
            _require(changed is True, "dependency bindings changed")
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


__all__ = [
    "EntityAddressSnapshotDestinationError",
    "require_entity_address_archive_publisher",
    "prepare_private_entity_address_archive_destination",
    "validate_entity_address_archive_preparation",
    "activate_validated_entity_address_archive_destination",
]
