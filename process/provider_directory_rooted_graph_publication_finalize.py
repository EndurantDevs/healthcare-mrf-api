# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded canonical cutover of a fully validated protected rooted candidate."""

import json
from time import monotonic

from db.migration_rooted_publication_freeze import SELECTOR
from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_dataset_scoped_publication import (
    EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    supersede_exact_current_dataset,
)
from process.provider_directory_generic_source_storage import _require
from process.provider_directory_rooted_graph_current_preflight import (
    capture_root_input_fence,
    current_root_proof,
    require_current_root_fence,
)
from process.provider_directory_rooted_graph_publication_candidate import HEADER, PARENT
from process.provider_directory_rooted_graph_source_runtime import current_rooted_graph_runtime


async def _insert_frozen_header(connection, runtime, document, root):
    """Copy one immutable header while preserving the existing canonical insert transition."""
    target = storage_relation(runtime.catalog_schema, root)
    source = storage_relation(document["tables"][root]["schema"], document["tables"][root]["name"])
    columns = await connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 "
        "AND NOT attisdropped AND attgenerated='' ORDER BY attnum",
        target,
    )
    names = [row["attname"] for row in columns]
    selected_columns = [
        "transaction_timestamp()" if name == "created_at" else "s." + storage_identifier(name) for name in names
    ]
    result = await connection.execute(
        f"INSERT INTO {target} ({','.join(map(storage_identifier, names))}) "
        f"SELECT {','.join(selected_columns)} FROM {source} s WHERE dataset_id=$1",
        document["identity"]["dataset_id"],
    )
    _require(result == "INSERT 0 1", "rooted_header_changed")


async def _switch_frozen_headers(database, document, previous):
    """Preserve the existing validated/published transitions with the already verified digest."""
    runtime = current_rooted_graph_runtime(database)
    header_values_by_field = dict(dataset_id=document["identity"]["dataset_id"], dataset_hash=document["dataset_hash"])
    for root in (PARENT, HEADER):
        changed = await database.status(
            f"UPDATE {storage_relation(runtime.catalog_schema, root)} SET status='validated', "
            "dataset_hash=:dataset_hash,validated_at=transaction_timestamp() "
            "WHERE dataset_id=:dataset_id AND status='building' AND NOT is_current AND dataset_hash IS NULL",
            **header_values_by_field,
        )
        _require(changed == 1, "rooted_header_changed")
    await supersede_exact_current_dataset(database, previous)
    for root in (PARENT, HEADER):
        changed = await database.status(
            f"UPDATE {storage_relation(runtime.catalog_schema, root)} SET status='published',is_current=true, "
            "published_at=transaction_timestamp() WHERE dataset_id=:dataset_id AND status='validated' AND NOT is_current",
            dataset_id=header_values_by_field["dataset_id"],
        )
        _require(changed == 1, "rooted_header_changed")


async def publish_prepared_rooted_graph_dataset(preparations, claim, before, *, database):
    """Cut over on the caller's original authenticated publisher transaction after full validation."""
    runtime = current_rooted_graph_runtime(database)
    _require(runtime is not None, "rooted_runtime_required")
    transaction = runtime.current_transaction(database)
    connection = transaction.connection
    _require(connection.is_in_transaction() and before["state"] == "validated", "rooted_transaction_changed")
    document = preparations.fhir_source[1].document
    proof = current_root_proof(database)
    _require(proof.current.dataset_id == document["identity"]["root_dataset_id"], "rooted_predecessor_changed")
    await connection.execute("SET LOCAL lock_timeout='250ms'")
    await connection.execute("SET LOCAL statement_timeout='3s'")
    started = monotonic()
    _require(
        await connection.fetchval(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", EXACT_DATASET_PUBLICATION_LOCK_IDENTITY
        ),
        "rooted_publication_busy",
    )
    previous = await require_current_root_fence(database, proof)
    await connection.execute("SELECT set_config($1,$2,true)", SELECTOR, document["identity"]["dataset_id"])
    for root in (PARENT, HEADER):
        await _insert_frozen_header(connection, runtime, document, root)
    await _switch_frozen_headers(database, document, previous)
    post_fence = json.loads(await capture_root_input_fence(database, previous))
    completed = await connection.fetchval("SELECT clock_timestamp()")
    return {
        "contract": "healthporta.provider-directory.rooted-publication.v1",
        "dataset_id": document["identity"]["dataset_id"],
        "endpoint_id": document["identity"]["endpoint_id"],
        "publication_acquisition_id": document["operation"]["publication_acquisition_id"],
        "source_handoff_sha256": preparations.fhir_source[1].document_sha256,
        "dataset_hash": document["dataset_hash"],
        "resource_count": sum(document["counts"].values()),
        "preparation_id": document["preparation_id"],
        "completed_at": completed.isoformat(),
        "rooted_input_fence": post_fence,
        "cutover_elapsed_seconds": monotonic() - started,
    }
