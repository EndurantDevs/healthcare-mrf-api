# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable named snapshots and content validation for serving recovery."""

import copy
import re
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process import entity_address_result_generation as generations
from process.provider_directory_output_snapshot_catalog import decoded, encoded
from process.provider_directory_projection_stage import _copy_driver

ROOTS = ("provider_directory_address_overlay", "entity_address_unified")
JOURNAL = "provider_directory_serving_recovery"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
PREPARATION_SECONDS = 2 * 60 * 60
_VALIDATED = ContextVar("serving_recovery_validated_relations", default=None)

_CATALOG_SQL = """
SELECT n.nspname AS schema,c.relname,
 jsonb_build_object('kind',c.relkind,'persistence',c.relpersistence,'partition',c.relispartition,
 'rls',c.relrowsecurity,'force_rls',c.relforcerowsecurity,'options',c.reloptions,
 'parents',EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid),
 'children',EXISTS(SELECT FROM pg_inherits WHERE inhparent=c.oid),
 'hooks',(SELECT count(*) FROM pg_trigger WHERE tgrelid=c.oid),
 'rules',(SELECT count(*) FROM pg_rewrite WHERE ev_class=c.oid)) AS topology,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('number',a.attnum,'name',a.attname,
 'type',format_type(a.atttypid,a.atttypmod),'typmod',a.atttypmod,
 'native_collation',a.attcollation=t.typcollation,
 'required',a.attnotnull,'default',pg_get_expr(d.adbin,d.adrelid),
 'identity',a.attidentity,'generated',a.attgenerated) ORDER BY a.attnum),'[]'::jsonb)
 FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
 LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('name',k.conname,
 'type',k.contype,'keys',k.conkey,'definition',pg_get_constraintdef(k.oid),
 'validated',k.convalidated,'deferrable',k.condeferrable,'deferred',k.condeferred)
 ORDER BY k.conname),'[]'::jsonb) FROM pg_constraint k WHERE k.conrelid=c.oid) AS constraints,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('name',idx.relname,
 'method',am.amname,'primary',i.indisprimary,'unique',i.indisunique,
 'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'immediate',i.indimmediate,
 'exclusion',i.indisexclusion,'keys_count',i.indnkeyatts,'total_count',i.indnatts,
 'predicate',pg_get_expr(i.indpred,i.indrelid),'expression',pg_get_expr(i.indexprs,i.indrelid),
 'keys',ARRAY(SELECT a.attname::text FROM unnest(i.indkey::smallint[]) WITH ORDINALITY x(num,pos)
 LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=x.num ORDER BY x.pos),
 'options',i.indoption::smallint[],
 'native',NOT EXISTS(SELECT FROM unnest(i.indclass::oid[],i.indcollation::oid[],
 i.indkey::smallint[],i.indoption::smallint[]) x(opclass_oid,collation_oid,num,flags)
 LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=x.num
 LEFT JOIN pg_opclass op ON op.oid=x.opclass_oid
 JOIN pg_namespace op_schema ON op_schema.oid=op.opcnamespace
 WHERE (x.num<>0 AND a.attnum IS NULL) OR x.flags<>0
 OR x.collation_oid<>COALESCE(a.attcollation,0)
 OR NOT op.opcdefault OR op_schema.nspname<>'pg_catalog'))
 ORDER BY idx.relname),'[]'::jsonb)
 FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam
 WHERE i.indrelid=c.oid) AS indexes
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname
"""

_LAYOUT_FIELDS = {
    "topology": {
        "kind",
        "persistence",
        "partition",
        "rls",
        "force_rls",
        "options",
        "parents",
        "children",
        "hooks",
        "rules",
    },
    "columns": {
        "number",
        "name",
        "type",
        "typmod",
        "native_collation",
        "required",
        "default",
        "identity",
        "generated",
    },
    "constraints": {"name", "type", "keys", "definition", "validated", "deferrable", "deferred"},
    "indexes": {
        "name",
        "method",
        "primary",
        "unique",
        "valid",
        "ready",
        "live",
        "immediate",
        "exclusion",
        "keys_count",
        "total_count",
        "predicate",
        "expression",
        "keys",
        "options",
        "native",
    },
}


@dataclass(frozen=True)
class PreparedRelation:
    """Content measured under named locks within the active validation scope."""

    schema: str
    receipt_json: str
    elapsed_seconds: float
    heap_bytes: int
    total_bytes: int


def require(condition, code):
    """Refuse an incomplete ordinary serving proof before publication."""
    if not condition:
        raise ValueError("provider_directory_serving_recovery_" + code)


def qualified(schema, name):
    """Quote only validated native identifiers."""
    return storage_relation(schema, name)


async def ordinary(database):
    """Use the caller's SQL transaction; PostgreSQL enforces its access rights."""
    binding = database._transaction_binding()
    require(binding is not None and binding.session.in_transaction(), "ordinary_transaction_required")
    driver = await _copy_driver(binding.session)
    if not driver.is_in_transaction():
        await binding.session.execute(text("SELECT 1"))
    require(driver.is_in_transaction(), "ordinary_transaction_required")
    return binding.session, driver, {}


@asynccontextmanager
async def validation_scope(database):
    """Keep validated content only while this transaction holds its named locks."""
    if _VALIDATED.get() is not None:
        yield
        return
    async with database.transaction():
        validated_by_name = {}
        token = _VALIDATED.set(validated_by_name)
        try:
            yield
        finally:
            validated_by_name.clear()
            _VALIDATED.reset(token)


def _require_heap(native):
    topology = native["topology"]
    require(
        topology["kind"] == "r"
        and topology["persistence"] == "p"
        and not any(topology[key] for key in ("partition", "rls", "force_rls", "parents", "children", "hooks", "rules"))
        and bool(native["columns"])
        and all(index["valid"] and index["ready"] and index["live"] for index in native["indexes"])
        and all(constraint["validated"] for constraint in native["constraints"]),
        "native_heap_changed",
    )


async def catalog(database, schema, names):
    """Capture logical layout by schema-qualified name without access attestations."""
    from process.entity_address_snapshot_receipt import _normalize_receipt_session

    session, driver, _actor = await ordinary(database)
    await _normalize_receipt_session(session, schema)
    rows = await driver.fetch(_CATALOG_SQL, schema, list(names))
    require(len(rows) == len(names), "relation_missing")
    catalog_by_name = {}
    for row in rows:
        native_by_field = {
            key: decoded(row[key]) if key in {"topology", "columns", "constraints", "indexes"} else row[key]
            for key in row.keys()
        }
        _require_heap(native_by_field)
        catalog_by_name[native_by_field["relname"]] = native_by_field
    return catalog_by_name


async def relation(database, schema, name, *, content=True):
    """Measure the complete canonical row multiset of a named snapshot."""
    session, _driver, _actor = await ordinary(database)
    from process.entity_address_snapshot_receipt import _normalize_receipt_session, _row_identity

    await _normalize_receipt_session(session, schema)
    native = (await catalog(database, schema, (name,)))[name]
    receipt_by_field = {"native": native}
    if content:
        from process.entity_address_cutover_contract import preserve_transaction_sql_settings
        from process.entity_address_unified import _sql_literal

        async with preserve_transaction_sql_settings(database, ["statement_timeout"], _sql_literal):
            await database.status(f"SET LOCAL statement_timeout='{PREPARATION_SECONDS * 1000}ms'")
            count, row_digest = await _row_identity(session, schema, name)
        receipt_by_field.update(rows=count, data_sha256=row_digest)
    return receipt_by_field


async def prepare_relation(database, schema, name):
    """Hash a frozen heap before any live AccessExclusive cutover starts."""
    started = time.monotonic()
    validated_by_name = _VALIDATED.get()
    require(validated_by_name is not None, "preparation_scope_required")
    _session, driver, _actor = await ordinary(database)
    await lock(database, schema, (name,), mode="SHARE")
    captured = await relation(database, schema, name)
    sizes = await driver.fetchrow(
        "SELECT pg_catalog.pg_relation_size($1::regclass)::bigint AS heap_bytes,"
        "pg_catalog.pg_total_relation_size($1::regclass)::bigint AS total_bytes",
        qualified(schema, name),
    )
    validated_by_name[(schema, name)] = encoded(captured)
    return PreparedRelation(
        schema,
        encoded(captured),
        time.monotonic() - started,
        sizes["heap_bytes"],
        sizes["total_bytes"],
    )


def preparation_metrics(proof):
    """Report actual digest phase cost and capacity without treating it as authority."""
    return {
        "rows": decoded(proof.receipt_json)["rows"],
        "elapsed_seconds": proof.elapsed_seconds,
        "heap_bytes": proof.heap_bytes,
        "total_bytes": proof.total_bytes,
    }


async def recheck_prepared(database, proof, name, *, prepare_missing=False):
    """Reuse content only for its current locked name in this validation scope."""
    require(type(proof) is PreparedRelation, "prepared_relation_invalid")
    validated_by_name = _VALIDATED.get()
    require(validated_by_name is not None, "preparation_scope_required")
    cached_json = validated_by_name.get((proof.schema, name))
    if cached_json is None and prepare_missing:
        await prepare_relation(database, proof.schema, name)
        cached_json = validated_by_name[(proof.schema, name)]
    require(cached_json is not None, "prepared_name_changed")
    expected = decoded(proof.receipt_json)
    cached = decoded(cached_json)
    require_relation(expected, cached, renamed=True)
    await lock(database, proof.schema, (name,), mode="SHARE")
    actual = await relation(database, proof.schema, name, content=False)
    require_relation(cached, actual, content=False)
    actual.update(rows=expected["rows"], data_sha256=expected["data_sha256"])
    return actual


async def prepared_state(database, schema, proofs_by_root):
    """Capture fresh installed metadata without rescanning any frozen bulk relation."""
    roots_by_name = {}
    for root in ROOTS:
        roots_by_name[root] = await recheck_prepared(database, proofs_by_root[root], root)
    return {"metadata": await metadata(database, schema), "roots": roots_by_name}


def _logical_native(native, *, renamed=False):
    """Read logical layout from current or legacy catalog descriptions."""
    require(type(native) is dict, "receipt_invalid")
    layout_by_field = {key: native[key] for key in ("schema", "relname") if key in native}
    for group, fields in _LAYOUT_FIELDS.items():
        if group not in native:
            continue
        if group == "topology":
            layout_by_field[group] = {
                key: copy.deepcopy(value) for key, value in native[group].items() if key in fields
            }
            for key in ("parents", "children"):
                if key in layout_by_field[group]:
                    layout_by_field[group][key] = bool(layout_by_field[group][key])
            continue
        layout_by_field[group] = [
            {key: copy.deepcopy(value) for key, value in item.items() if key in fields} for item in native[group]
        ]
        if renamed and group in {"constraints", "indexes"}:
            for item in layout_by_field[group]:
                item.pop("name", None)
        if group != "columns":
            layout_by_field[group].sort(key=encoded)
    if renamed:
        layout_by_field.pop("relname", None)
    return layout_by_field


def normalized_native(native):
    """Compare logical layout across table and index renames."""
    return _logical_native(native, renamed=True)


def normalize_relation(receipt, schema):
    """Discard legacy physical metadata while retaining existing backup names."""
    require(type(receipt) is dict and type(receipt.get("native")) is dict, "receipt_invalid")
    original = receipt["native"]
    native = _logical_native(original)
    require(native.get("schema", schema) == schema, "relation_changed")
    native["schema"] = schema
    if "retained_name" in original or "oid" in original:
        native["retained_name"] = retained_name(original)
    originals_by_name = {item["name"]: item for item in original.get("indexes", ())}
    for index in native.get("indexes", ()):
        original_index = originals_by_name[index["name"]]
        if "retained_name" in original_index or "oid" in original_index:
            index["retained_name"] = retained_name(original_index, index=True)
    return {"native": native, **{key: receipt[key] for key in ("rows", "data_sha256") if key in receipt}}


def require_relation(expected, actual, *, renamed=False, content=True):
    """Compare names, logical layout and, when requested, complete row contents."""
    require(type(expected) is dict and type(actual) is dict, "receipt_invalid")
    expected_native = _logical_native(expected["native"], renamed=renamed)
    actual_native = _logical_native(actual["native"], renamed=renamed)
    require(expected_native == actual_native, "relation_changed")
    if content:
        require(
            type(expected.get("rows")) is int
            and expected["rows"] >= 0
            and type(expected.get("data_sha256")) is str
            and _HASH.fullmatch(expected["data_sha256"])
            and (expected["rows"], expected["data_sha256"]) == (actual.get("rows"), actual.get("data_sha256")),
            "content_changed",
        )


def validate_state(state_by_field):
    """Decode only the fixed complete serving receipt, never arbitrary object names."""
    require(type(state_by_field) is dict and set(state_by_field) == {"metadata", "roots"}, "state_invalid")
    metadata = state_by_field["metadata"]
    require(
        type(metadata) is dict
        and set(metadata)
        == {"address", "alias", "overlay_alias", "profile", "doctors", "geo", "relations", "common_receipt"},
        "state_invalid",
    )
    require(type(state_by_field["roots"]) is dict and set(state_by_field["roots"]) == set(ROOTS), "state_invalid")
    for root, receipt in state_by_field["roots"].items():
        require(
            type(receipt) is dict
            and set(receipt) == {"native", "rows", "data_sha256"}
            and receipt["native"]["relname"] == root
            and type(receipt["native"].get("schema")) is str,
            "state_invalid",
        )
        qualified(receipt["native"]["schema"], root)
        require_relation(receipt, receipt)
    relations = metadata["relations"]
    schema = state_by_field["roots"][ROOTS[1]]["native"]["schema"]
    require(
        type(relations) is list
        and relations == [f"{schema}.{name}" for name in generations.RELATION_NAMES]
        and all(receipt["native"]["schema"] == schema for receipt in state_by_field["roots"].values()),
        "state_invalid",
    )
    address = metadata["address"]
    require(
        type(address) is dict
        and address.get("singleton") is True
        and type(address.get("local_generation")) is int
        and address["local_generation"] >= 0,
        "state_invalid",
    )
    require(type(metadata["alias"]) is dict and type(metadata["alias"].get("generation")) is int, "state_invalid")
    return state_by_field


def _logical_metadata(metadata, schema):
    result = copy.deepcopy(metadata)
    for family in ("address", "doctors"):
        if type(result.get(family)) is dict:
            result[family].pop("relation_oids", None)
    if type(result.get("profile")) is dict:
        for field in ("profile_target_oid", "evidence_target_oid"):
            result["profile"].pop(field, None)
    relations = result.get("relations")
    if type(relations) is list and all(type(value) is int for value in relations):
        result["relations"] = [f"{schema}.{name}" for name in generations.RELATION_NAMES]
    if result.get("geo") is not None:
        version = result["geo"].get("version")
        relation_name = result["geo"].get("relation", f"{schema}.{ROOTS[1]}" if version is not None else None)
        require(relation_name == (f"{schema}.{ROOTS[1]}" if version is not None else None), "state_invalid")
        result["geo"] = {"version": version, "relation": relation_name}
    if result.get("common_receipt") is not None:
        common = result["common_receipt"]
        result["common_receipt"] = {
            "receipt_id": common["receipt_id"],
            "predecessor_receipt_id": common.get(
                "predecessor_receipt_id", common.get("payload", {}).get("predecessor_receipt_id")
            ),
        }
    return result


def normalize_state(state_by_field, schema):
    """Read old journal snapshots without rewriting their stored metadata."""
    require(type(state_by_field) is dict and set(state_by_field) == {"metadata", "roots"}, "state_invalid")
    state_by_part = {
        "metadata": _logical_metadata(state_by_field["metadata"], schema),
        "roots": {root: normalize_relation(receipt, schema) for root, receipt in state_by_field["roots"].items()},
    }
    return validate_state(state_by_part)


def normalized_state(state_by_field):
    """Compare serving data without a future retention-name plan."""
    validate_state(state_by_field)
    schema = state_by_field["roots"][ROOTS[1]]["native"]["schema"]
    return {
        "metadata": _logical_metadata(state_by_field["metadata"], schema),
        "roots": {
            root: {**receipt, "native": _logical_native(receipt["native"])}
            for root, receipt in state_by_field["roots"].items()
        },
    }


def require_state(expected, actual):
    """Require the same logical serving generation and validated contents."""
    require(normalized_state(expected) == normalized_state(actual), "state_changed")


async def _optional_json(driver, schema, table, selection, predicate):
    name = qualified(schema, table)
    if await driver.fetchval("SELECT to_regclass($1)::oid", name) is None:
        return None
    rows = await driver.fetch(f"SELECT {selection} AS value FROM {name} AS state WHERE {predicate}")
    require(len(rows) <= 1, "authority_ambiguous")
    return decoded(rows[0]["value"]) if rows else None


async def metadata(database, schema):
    """Capture logical serving generations and their schema-qualified names."""
    _session, driver, _actor = await ordinary(database)
    selections = (
        ("address", generations.TABLE_NAME, "to_jsonb(state)", "singleton"),
        ("alias", "address_alias_state_v1", "to_jsonb(state)", "singleton"),
        (
            "overlay_alias",
            "address_alias_artifact_state_v1",
            "to_jsonb(state)",
            "artifact_name='provider_directory_address_overlay'",
        ),
        ("profile", "provider_directory_profile_serving_generation", "to_jsonb(state)", "singleton_key='global'"),
        ("doctors", "reference_family_result_generation", "to_jsonb(state)", "importer_id='cms-doctors'"),
        (
            "geo",
            "entity_address_geo_assurance_state",
            "jsonb_build_object('version',active_geo_assurance_version)",
            "singleton",
        ),
    )
    metadata_by_field = {}
    for key, table, selection, predicate in selections:
        metadata_by_field[key] = await _optional_json(driver, schema, table, selection, predicate)
    metadata_by_field["relations"] = list(await generations._current_relation_names(database, schema))
    table = qualified(schema, "provider_directory_cms_serving_receipt")
    if await driver.fetchval("SELECT to_regclass($1)::oid", table) is None:
        metadata_by_field["common_receipt"] = None
    else:
        receipt_rows = await driver.fetch(
            f"SELECT receipt_id,predecessor_receipt_id FROM {table} receipt WHERE NOT EXISTS "
            f"(SELECT FROM {table} successor WHERE successor.predecessor_receipt_id=receipt.receipt_id)"
        )
        require(len(receipt_rows) <= 1, "common_history_changed")
        metadata_by_field["common_receipt"] = dict(receipt_rows[0]) if receipt_rows else None
    require(metadata_by_field["address"] is not None and metadata_by_field["alias"] is not None, "authority_missing")
    return _logical_metadata(metadata_by_field, schema)


async def state(database, schema):
    """Capture complete contents and logical serving metadata."""
    session, _driver, _actor = await ordinary(database)
    from process.entity_address_snapshot_receipt import _normalize_receipt_session

    await _normalize_receipt_session(session, schema)
    roots_by_name = {}
    for root in ROOTS:
        roots_by_name[root] = await relation(database, schema, root)
    return {"metadata": await metadata(database, schema), "roots": roots_by_name}


async def lock_metadata(database, schema):
    """Pin the fixed serving metadata through ordinary SQL locks."""
    _session, driver, _actor = await ordinary(database)
    await driver.execute(
        "LOCK TABLE "
        + ",".join(qualified(schema, name) for name in generations.RELATION_NAMES[1:])
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    for table, predicate in (
        ("address_alias_state_v1", "singleton"),
        ("reference_family_result_generation", "importer_id='cms-doctors'"),
        ("provider_directory_profile_serving_generation", "singleton_key='global'"),
    ):
        name = qualified(schema, table)
        if await driver.fetchval("SELECT to_regclass($1)::oid", name) is not None:
            await driver.execute(f"LOCK TABLE {name} IN SHARE MODE NOWAIT")
            await driver.fetch(f"SELECT 1 FROM {name} WHERE {predicate} FOR SHARE NOWAIT")
    await generations._locked_authority(database, schema)
    for table, predicate in (
        ("address_alias_artifact_state_v1", "artifact_name='provider_directory_address_overlay'"),
        ("entity_address_geo_assurance_state", "singleton"),
    ):
        rows = await driver.fetch(f"SELECT 1 FROM {qualified(schema, table)} WHERE {predicate} FOR UPDATE NOWAIT")
        require(len(rows) == 1, "metadata_authority_missing")
    name = qualified(schema, "provider_directory_cms_serving_receipt")
    if await driver.fetchval("SELECT to_regclass($1)::oid", name) is not None:
        await driver.execute(f"LOCK TABLE {name} IN SHARE MODE NOWAIT")


async def lock(database, schema, names, *, mode="ACCESS EXCLUSIVE"):
    """Lock a named snapshot set in deterministic order."""
    require(mode in {"SHARE", "ACCESS EXCLUSIVE"}, "lock_mode_invalid")
    _session, driver, _actor = await ordinary(database)
    await driver.execute(
        "LOCK TABLE "
        + ",".join(qualified(schema, name) for name in sorted(set(names)))
        + " IN "
        + mode
        + " MODE NOWAIT"
    )


def retained_name(receipt, *, index=False):
    """Read an explicit backup name or decode a legacy name without verifying IDs."""
    native = receipt if type(receipt) is dict else {"oid": receipt}
    if type(native.get("native")) is dict:
        native = native["native"]
    name = native.get("retained_name")
    if name is None:
        legacy_number = native.get("oid")
        require(type(legacy_number) is int and 0 < legacy_number < 2**32, "retained_name_missing")
        name = ("pdsrv_i_" if index else "pdsrv_r_") + format(legacy_number, "x")
    storage_identifier(name)
    require(name.startswith("pdsrv_i_" if index else "pdsrv_r_"), "retained_name_invalid")
    return name


def plan_retention(receipt, generation, phase):
    """Allocate portable backup names once for this logical publication phase."""
    result = copy.deepcopy(receipt)
    native = result["native"]
    require(type(generation) is str and bool(generation) and type(phase) is str and bool(phase), "retention_invalid")
    scope = encoded([generation, phase, native["schema"], native["relname"]])
    native["retained_name"] = "pdsrv_r_" + uuid5(NAMESPACE_URL, scope).hex
    for index in native["indexes"]:
        index["retained_name"] = "pdsrv_i_" + uuid5(NAMESPACE_URL, scope + ":" + index["name"]).hex
    return result


def _renamed_index(receipt, original, replacement):
    if any(item["name"] == original for item in receipt["native"].get("indexes", ())):
        for group in ("indexes", "constraints"):
            for item in receipt["native"].get(group, ()):
                if item["name"] == original:
                    item["name"] = replacement
    return encoded(receipt)


def record_rename(schema, original, replacement, *, index=False):
    """Move validated content only after its successful SQL rename."""
    validated_by_name = _VALIDATED.get()
    if validated_by_name is None:
        return
    if index:
        for key, receipt_json in tuple(validated_by_name.items()):
            if key[0] != schema:
                continue
            validated_by_name[key] = _renamed_index(decoded(receipt_json), original, replacement)
        return
    receipt_json = validated_by_name.pop((schema, original), None)
    validated_by_name.pop((schema, replacement), None)
    if receipt_json is not None:
        receipt = decoded(receipt_json)
        receipt["native"]["relname"] = replacement
        validated_by_name[(schema, replacement)] = encoded(receipt)


async def rename(database, schema, original, replacement, *, index=False):
    """Never overwrite an occupied relation name during a metadata-only rotation."""
    _session, driver, _actor = await ordinary(database)
    require(
        await driver.fetchval("SELECT to_regclass($1)::oid", qualified(schema, replacement)) is None,
        "retained_name_occupied",
    )
    await driver.execute(
        "ALTER "
        + ("INDEX " if index else "TABLE ")
        + qualified(schema, original)
        + " RENAME TO "
        + storage_identifier(replacement)
    )
    record_rename(schema, original, replacement, index=index)


async def retain(database, schema, expected):
    """Keep the original named snapshot and indexes without copying rows."""
    native = expected["native"]
    await lock(database, schema, (native["relname"],))
    actual = await relation(database, schema, native["relname"], content=False)
    require_relation(expected, actual, content=False)
    for index in native["indexes"]:
        await rename(database, schema, index["name"], retained_name(index, index=True), index=True)
    await rename(database, schema, native["relname"], retained_name(native))


async def restore(database, schema, expected, *, prepared):
    """Restore original names after retaining the replaced successor."""
    native = expected["native"]
    retained = retained_name(native)
    actual = await recheck_prepared(database, prepared, retained)
    require_relation(expected, actual, renamed=True)
    await rename(database, schema, retained, native["relname"])
    for index in native["indexes"]:
        await rename(database, schema, retained_name(index, index=True), index["name"], index=True)
