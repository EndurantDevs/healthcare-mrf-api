# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native receipts for ordinary provider serving recovery."""

import copy
import hashlib
import re
import time
from dataclasses import dataclass

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process import entity_address_result_generation as generations
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    decoded,
    encoded,
    portable_catalog_payload,
)
from process.provider_directory_projection_stage import _copy_driver

ROOTS = ("provider_directory_address_overlay", "entity_address_unified")
JOURNAL = "provider_directory_serving_recovery"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
PREPARATION_SECONDS = 2 * 60 * 60


@dataclass(frozen=True)
class PreparedRelation:
    """Validated logical content for a server-selected immutable named snapshot."""

    session: object
    task: object
    actor_json: str
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
    """Use the actual caller transaction; PostgreSQL enforces all access."""
    from process import provider_directory_generic_source_policy as source

    binding = database._transaction_binding()
    require(binding is not None and binding.session.in_transaction(), "ordinary_transaction_required")
    require(source._PUBLISHER.get() is None and source._OWNER.get() is None, "ordinary_context_required")
    driver = await _copy_driver(binding.session)
    if not driver.is_in_transaction():
        await binding.session.execute(text("SELECT 1"))
    require(driver.is_in_transaction(), "transaction_required")
    return binding.session, driver, None


def _require_heap(native, owner=None):
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
    """Read portable logical layouts at the caller-selected finite qualified names."""
    from process.entity_address_snapshot_receipt import _normalize_receipt_session

    session, driver, _actor = await ordinary(database)
    await _normalize_receipt_session(session, schema)
    rows = await driver.fetch(OUTPUT_CATALOG_SQL, schema, list(names))
    require(len(rows) == len(names) and {row["relname"] for row in rows} == set(names), "relation_missing")
    catalog_by_name = {}
    for row in rows:
        catalog_by_field = {"schema": schema, **portable_catalog_payload(row)}
        _require_heap(catalog_by_field)
        await driver.execute(f"SELECT 1 FROM {qualified(schema, catalog_by_field['relname'])} LIMIT 0")
        catalog_by_name[catalog_by_field["relname"]] = catalog_by_field
    return catalog_by_name


async def relation(database, schema, name, *, content=True):
    """Bind a complete canonical row multiset to one real native heap and indexes."""
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


async def _require_freeze(driver, schema, name):
    """Pin a named immutable snapshot using actual native read access."""
    await driver.execute(f"LOCK TABLE {qualified(schema, name)} IN ACCESS SHARE MODE NOWAIT")


async def prepare_relation(database, schema, name):
    """Close new candidate writes before its one complete content/layout validation."""
    from process.entity_address_publication_permissions import make_relation_read_only

    started = time.monotonic()
    session, driver, _actor = await ordinary(database)
    if name not in ROOTS and not name.startswith("pdsrv_r_"):
        await make_relation_read_only(session, schema, name)
    await _require_freeze(driver, schema, name)
    captured = await relation(database, schema, name)
    sizes = await driver.fetchrow(
        "SELECT pg_catalog.pg_relation_size(to_regclass($1))::bigint AS heap_bytes,"
        "pg_catalog.pg_total_relation_size(to_regclass($1))::bigint AS total_bytes",
        qualified(schema, name),
    )
    return PreparedRelation(
        None,
        None,
        None,
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


async def recheck_prepared(database, proof, name):
    """Reuse validated content only for the server's frozen named snapshot frame."""
    require(type(proof) is PreparedRelation, "prepared_record_invalid")
    _session, driver, _actor = await ordinary(database)
    expected = decoded(proof.receipt_json)
    require(
        type(expected.get("rows")) is int
        and expected["rows"] >= 0
        and isinstance(expected.get("data_sha256"), str)
        and _HASH.fullmatch(expected["data_sha256"]),
        "receipt_invalid",
    )
    await _require_freeze(driver, proof.schema, name)
    actual = await relation(database, proof.schema, name, content=False)
    require_relation(expected, actual, renamed=True, content=False)
    actual.update(rows=expected["rows"], data_sha256=expected["data_sha256"])
    return actual


async def prepared_state(database, schema, proofs_by_root):
    """Capture fresh installed metadata without rescanning any frozen bulk relation."""
    roots_by_name = {}
    for root in ROOTS:
        roots_by_name[root] = await recheck_prepared(database, proofs_by_root[root], root)
    return {"protocol": 2, "metadata": await metadata(database, schema), "roots": roots_by_name}


def normalized_native(native, *, schema=None, renamed=False):
    """Compare portable schema structure; old physical fields remain opaque."""
    result = portable_catalog_payload(native)
    namespace = native.get("schema", schema)
    require(schema is None or namespace == schema, "namespace_changed")
    if namespace is not None:
        storage_identifier(namespace)
        result["schema"] = namespace
    if renamed:
        result["relname"] = "retained_snapshot"
    return result


def require_relation(expected, actual, *, renamed=False, content=True):
    """Require complete portable layout and optionally the validated row multiset."""
    require(type(expected) is dict and type(actual) is dict, "receipt_invalid")
    schema = actual["native"].get("schema", expected["native"].get("schema"))
    require(
        normalized_native(expected["native"], schema=schema, renamed=renamed)
        == normalized_native(actual["native"], schema=schema, renamed=renamed),
        "relation_changed",
    )
    if content:
        require(
            type(expected.get("rows")) is int
            and expected["rows"] >= 0
            and isinstance(expected.get("data_sha256"), str)
            and _HASH.fullmatch(expected["data_sha256"])
            and (expected["rows"], expected["data_sha256"]) == (actual.get("rows"), actual.get("data_sha256")),
            "content_changed",
        )


def validate_state(state_by_field, schema=None):
    """Validate data/generation fields and return portable state without rewriting old input."""
    require(
        type(state_by_field) is dict
        and set(state_by_field) in ({"metadata", "roots"}, {"protocol", "metadata", "roots"}),
        "state_invalid",
    )
    require(
        "protocol" not in state_by_field or type(state_by_field["protocol"]) is int and state_by_field["protocol"] == 2,
        "state_invalid",
    )
    require(type(state_by_field["roots"]) is dict and set(state_by_field["roots"]) == set(ROOTS), "state_invalid")
    if state_by_field.get("protocol") == 2:
        require(type(state_by_field["metadata"]) is dict, "state_invalid")
        declared = state_by_field["metadata"].get("relations")
        require(isinstance(declared, list) and len(declared) == len(generations.RELATION_NAMES), "state_invalid")
        require(all(isinstance(pair, list) and len(pair) == 2 for pair in declared), "state_invalid")
        schema = schema or declared[0][0]
        require(declared == [[schema, name] for name in generations.RELATION_NAMES], "state_invalid")
        require(
            all(
                type(receipt) is dict
                and type(receipt.get("native")) is dict
                and receipt["native"].get("schema") == schema
                for receipt in state_by_field["roots"].values()
            ),
            "state_invalid",
        )
    receipts_by_root = {}
    for root, receipt in state_by_field["roots"].items():
        require(
            type(receipt) is dict
            and set(receipt) == {"native", "rows", "data_sha256"}
            and receipt["native"]["relname"] == root,
            "state_invalid",
        )
        require_relation(receipt, receipt)
        receipts_by_root[root] = {**receipt, "native": normalized_native(receipt["native"], schema=schema)}
    metadata_by_field = logical_metadata(state_by_field["metadata"], schema=schema)
    address = metadata_by_field["address"]
    require(
        type(address) is dict
        and address.get("singleton") is True
        and type(address.get("local_generation")) is int
        and address["local_generation"] >= 0,
        "state_invalid",
    )
    require(
        type(metadata_by_field["alias"]) is dict and type(metadata_by_field["alias"].get("generation")) is int,
        "state_invalid",
    )
    return {"protocol": 2, "metadata": metadata_by_field, "roots": receipts_by_root}


def logical_metadata(saved_metadata, *, schema=None):
    """Project complete logical generations and declared names without altering saved input."""
    required_fields = {"address", "alias", "overlay_alias", "profile", "doctors", "geo", "relations", "common_receipt"}
    require(type(saved_metadata) is dict and set(saved_metadata) == required_fields, "state_invalid")
    metadata_by_field = copy.deepcopy(saved_metadata)
    if schema is None:
        entries = metadata_by_field["relations"]
        if entries and isinstance(entries[0], (list, tuple)):
            schema = entries[0][0]
    if schema is not None:
        expected_relations = [[schema, name] for name in generations.RELATION_NAMES]
        if metadata_by_field["relations"] and isinstance(metadata_by_field["relations"][0], (list, tuple)):
            require(metadata_by_field["relations"] == expected_relations, "state_invalid")
        metadata_by_field["relations"] = expected_relations
    else:
        metadata_by_field["relations"] = list(generations.RELATION_NAMES)
    for field in ("address", "doctors"):
        if isinstance(metadata_by_field[field], dict):
            metadata_by_field[field].pop("relation_oids", None)
    if isinstance(metadata_by_field["profile"], dict):
        for field in ("profile_target_oid", "evidence_target_oid"):
            metadata_by_field["profile"].pop(field, None)
    if isinstance(metadata_by_field["geo"], dict):
        metadata_by_field["geo"].pop("table_oid", None)
        if schema is not None and metadata_by_field["geo"].get("signature") is not None:
            from api import ptg2_geo_projection as projection

            bindings = metadata_by_field["geo"].get("bindings")
            signature = projection.projection_named_relation_signature(
                schema, metadata_by_field["geo"]["signature"], bindings
            )
            metadata_by_field["geo"]["signature"] = {name: [namespace, table] for name, namespace, table in signature}
            if bindings is not None:
                metadata_by_field["geo"]["bindings"] = {
                    name: {field: binding[field] for field in ("schema_name", "table_name")}
                    for name, binding in bindings.items()
                }
                projection.validate_projection_dependency_qnames(schema, metadata_by_field["geo"]["bindings"])
    return metadata_by_field


async def _optional_json(driver, schema, table, selection, predicate):
    name = qualified(schema, table)
    if await driver.fetchval("SELECT to_regclass($1)::oid", name) is None:
        return None
    rows = await driver.fetch(f"SELECT {selection} AS value FROM {name} AS state WHERE {predicate}")
    require(len(rows) <= 1, "authority_ambiguous")
    return decoded(rows[0]["value"]) if rows else None


async def metadata(database, schema):
    """Capture serving authorities without treating mutable build candidates as active state."""
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
            "jsonb_build_object('version',active_geo_assurance_version,"
            "'signature',active_relation_signature,'bindings',active_dependency_bindings)",
            "singleton",
        ),
    )
    metadata_by_field = {}
    for key, table, selection, predicate in selections:
        metadata_by_field[key] = await _optional_json(driver, schema, table, selection, predicate)
    metadata_by_field["relations"] = [[schema, name] for name in generations.RELATION_NAMES]
    table = qualified(schema, "provider_directory_cms_serving_receipt")
    if await driver.fetchval("SELECT to_regclass($1)::oid", table) is None:
        metadata_by_field["common_receipt"] = None
    else:
        receipt_rows = await driver.fetch(
            f"SELECT receipt_id,payload FROM {table} receipt WHERE NOT EXISTS "
            f"(SELECT FROM {table} successor WHERE successor.predecessor_receipt_id=receipt.receipt_id)"
        )
        require(len(receipt_rows) <= 1, "common_history_changed")
        metadata_by_field["common_receipt"] = (
            {"receipt_id": receipt_rows[0]["receipt_id"], "payload": decoded(receipt_rows[0]["payload"])}
            if receipt_rows
            else None
        )
    require(metadata_by_field["address"] is not None and metadata_by_field["alias"] is not None, "authority_missing")
    return logical_metadata(metadata_by_field, schema=schema)


async def state(database, schema):
    """Capture complete content and native authority only while readers remain admitted."""
    session, _driver, _actor = await ordinary(database)
    from process.entity_address_snapshot_receipt import _normalize_receipt_session

    await _normalize_receipt_session(session, schema)
    roots_by_name = {}
    for root in ROOTS:
        roots_by_name[root] = await relation(database, schema, root)
    return {"protocol": 2, "metadata": await metadata(database, schema), "roots": roots_by_name}


async def lock_metadata(database, schema):
    """Pin fixed serving authorities using their existing narrow native writer rights."""
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
    """Lock a fixed or previously authenticated native set in deterministic order."""
    require(mode in {"ACCESS SHARE", "SHARE", "ACCESS EXCLUSIVE"}, "lock_mode_invalid")
    _session, driver, _actor = await ordinary(database)
    await driver.execute(
        "LOCK TABLE "
        + ",".join(qualified(schema, name) for name in sorted(set(names)))
        + " IN "
        + mode
        + " MODE NOWAIT"
    )


def retained_name(generation, root=None, *, phase="forward", index=False):
    """New retained names bind only logical generation and the complete qualified target."""
    require(isinstance(generation, str) and isinstance(root, str), "generation_required")
    storage_identifier(root)
    token = hashlib.sha256(encoded({"generation": generation, "root": root, "phase": phase}).encode()).hexdigest()[:32]
    return ("pdsrv_i_" if index else "pdsrv_r_") + token


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


async def retain(database, schema, expected, *, generation, phase="forward"):
    """Retain the validated named snapshot without replacing an earlier generation."""
    native = expected["native"]
    actual = await relation(database, schema, native["relname"], content=False)
    require_relation(expected, actual, content=False)
    _session, driver, _actor = await ordinary(database)
    root = native["relname"]
    indexes = await driver.fetch(
        "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=to_regclass($1) ORDER BY c.relname",
        qualified(schema, root),
    )
    for item in indexes:
        await rename(
            database,
            schema,
            item["relname"],
            retained_name(generation, item["relname"], phase=phase, index=True),
            index=True,
        )
    name = retained_name(generation, root, phase=phase)
    await rename(database, schema, root, name)
    return {"schema": schema, "relation": name}


async def restore(database, schema, expected, *, prepared):
    """Restore validated old data by the prepared retained qualified name."""
    retained = decoded(prepared.receipt_json)["native"]["relname"]
    actual = await recheck_prepared(database, prepared, retained)
    require_relation(expected, actual, renamed=True)
    await rename(database, schema, retained, expected["native"]["relname"])


def has_same_relation(expected, actual):
    """Compare semantic content and portable layout without a catalog token."""
    try:
        require_relation(expected, actual)
    except ValueError:
        return False
    return True


async def resolve_legacy_retained(database, schema, expected):
    """Resolve one old unnamed retained snapshot with a bounded semantic search."""
    _session, driver, _actor = await ordinary(database)
    names = await driver.fetch(
        "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind='r' AND c.relname ~ '^pdsrv_r_[0-9a-f]{1,8}$' "
        "ORDER BY c.relname LIMIT 17",
        schema,
    )
    require(len(names) <= 16, "legacy_retained_search_limit")
    matches = []
    for item in names:
        name = item["relname"]
        await _require_freeze(driver, schema, name)
        layout = await relation(database, schema, name, content=False)
        try:
            require_relation(expected, layout, renamed=True, content=False)
        except ValueError:
            continue
        candidate = await relation(database, schema, name)
        try:
            require_relation(expected, candidate, renamed=True)
        except ValueError:
            continue
        matches.append({"schema": schema, "relation": name})
    require(len(matches) == 1, "legacy_retained_missing_or_ambiguous")
    return matches[0]
