# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed inverse plans for the existing common-serving journal owner."""

import importlib
from dataclasses import dataclass

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_common_serving_recovery_document as documents
from process import provider_directory_serving_recovery_receipts as receipts


@dataclass(frozen=True)
class RowRestore:
    """Exact affected row replacement; the publication owner controls transaction and order."""

    family: str
    delete_sql: str
    insert_sql: str
    parameters: tuple


@dataclass(frozen=True)
class InitialRestore:
    """Names derived only from original native heap/index OIDs and authentic baseline."""

    family: str
    root: str
    original_oid: int
    retained_relation: str
    retained_indexes: tuple[tuple[str, str], ...]


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_common_serving_inverse_invalid")


def restore_rows_sql(schema, retention_payload, qualified_source_ids):
    """Remove new-only keys and restore every old scoped row, including review history."""
    retained_by_field = documents.validate_retention(retention_payload)
    scopes_by_kind = {entry["family"]: entry for entry in retained_by_field["scopes"]}
    _require(type(qualified_source_ids) in {tuple, list} and bool(qualified_source_ids))
    _require(
        all(
            type(source_id) is str and source_id and source_id == source_id.strip()
            for source_id in qualified_source_ids
        )
    )
    restores = []
    for entry in retained_by_field["families"]:
        if entry["reused_initial"]:
            continue
        _require(entry["schema"] == schema)
        family = entry["family"]
        predicate, parameters = postimages._scope_predicate(schema, scopes_by_kind, family, qualified_source_ids)
        target_ref = storage_relation(schema, documents.FAMILIES[family])
        columns = ",".join(storage_identifier(column) for column in entry["columns"])
        retained = storage_relation(schema, entry["name"])
        restores.append(
            RowRestore(
                family,
                f"DELETE FROM {target_ref} AS live WHERE {predicate}",
                f"INSERT INTO {target_ref} ({columns}) SELECT {columns} FROM {retained}",
                parameters,
            )
        )
    return tuple(restores)


def initial_restore_plan(before_state, retention_payload):
    """Reinstall only the two actual original heaps; never fabricate legacy row digests."""
    baseline = documents.validate_state(before_state)
    retained_by_field = documents.validate_retention(retention_payload)
    initial_entries = [entry for entry in retained_by_field["families"] if entry["reused_initial"]]
    if baseline["initial_targets"] is None:
        _require(not initial_entries)
        return ()
    _require({entry["family"] for entry in initial_entries} == {"profile", "evidence"})
    restores = []
    for entry in initial_entries:
        root = documents.FAMILIES[entry["family"]]
        native = baseline["roots"][root]
        retained = receipts.retained_name(native["oid"])
        _require(entry["source_oid"] == entry["oid"] == native["oid"] and entry["name"] == retained)
        indexes = tuple(
            (receipts.retained_name(index["oid"], index=True), index["name"]) for index in native["indexes"]
        )
        restores.append(InitialRestore(entry["family"], root, native["oid"], retained, indexes))
    return tuple(restores)


async def assert_retained_rows(session, schema, retention_payload):
    """Recheck native copies on the existing owner; return initial refs requiring its witness."""
    from process import provider_directory_ordinary_retention_capacity as capacity
    from process import provider_directory_ordinary_row_retention as retention
    from process.entity_address_snapshot_receipt import _row_identity
    from process.provider_directory_cms_preparation import OwnedRelation

    fhir = importlib.import_module("process.provider_directory_fhir")
    retained_by_field = documents.validate_retention(retention_payload)
    actual_session, driver, actor = await receipts.ordinary(fhir.db)
    _require(
        actual_session is session
        and actor["owner"] == retained_by_field["creator"]["owner"]
        and actor["database_oid"] == retained_by_field["creator"]["database_oid"]
    )
    initial_refs = []
    for entry in (*retained_by_field["scopes"], *retained_by_field["families"]):
        _require(entry["schema"] == schema)
        relation = OwnedRelation(schema, entry["name"], entry["oid"], 0, "p")
        await receipts._require_freeze(driver, relation.oid)
        custody = await capacity._custody(fhir, relation)
        _require(custody["persistence"] == "p")
        if entry["reused_initial"]:
            catalog, _toast = await capacity._catalog(fhir, relation, initial_family=entry["family"])
            _require(capacity._layout_digest(catalog) == entry["layout_sha256"])
            initial_refs.append(entry)
            continue
        _require(custody["owner_oid"] == actor["owner"])
        _require(await retention._columns(driver, relation.oid) == tuple(entry["columns"]))
        if entry["kind"] == "scope":
            layout = await capacity.retained_scope_layout_sha256(fhir, entry["family"].removesuffix("_scope"), relation)
        else:
            layout = await capacity.retention_layout_sha256(fhir, relation)
        _require(layout == entry["layout_sha256"])
        async with receipts.normalized_receipt_scope(fhir.db, session, schema):
            _require(await _row_identity(session, schema, entry["name"]) == (entry["rows"], entry["data_sha256"]))
    final_session, final_driver, final_actor = await receipts.ordinary(fhir.db)
    _require(final_session is session and final_driver is driver and final_actor == actor)
    return tuple(initial_refs)
