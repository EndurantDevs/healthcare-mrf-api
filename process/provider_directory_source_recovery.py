# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Carry complete REST resource families into a separate named acquisition."""

from collections import Counter
from importlib import import_module
from types import SimpleNamespace

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_proof_store as proof
from process.provider_directory_generic_candidate import (
    COPY_FRAME_BYTES,
    COPY_MAX_BYTES,
    COPY_MAX_ROWS,
    copy_record_bytes,
)
from process.provider_directory_partition_successor import _verify_partition

_CHECKPOINT = "provider_directory_pagination_checkpoint"
_PROOF = "provider_directory_dataset_proof_shard"
_PARTITION = ":LastUpdatedPartition"
_INPUT_COLUMNS = (*storage.RESOURCE_COLUMNS, "batch_ordinal", "row_ordinal")
_INPUT_SELECTION = ", ".join(
    "payload_json::text AS payload_json" if column == "payload_json" else column for column in _INPUT_COLUMNS[1:]
)
_INPUT_PARITY = ", ".join(
    "sha256(convert_to(payload_json::text, 'UTF8'))" if column == "payload_json" else column
    for column in _INPUT_COLUMNS[1:]
)
_SHARD_COLUMNS = (
    "dataset_id",
    "shard_id",
    "endpoint_id",
    "acquisition_root_run_id",
    "source_ids_json",
    "resource_count",
    "resource_counts_json",
    "first_identity_json",
    "last_identity_json",
    "input_sha256",
    "artifact_sha256",
    "artifact_byte_count",
    "payload_bytes",
)


async def _loading_candidate(connection, catalog, schema, run_id, *, predecessor):
    """Bind an exact named candidate to its locked catalog attempt."""
    run_relation = storage_relation(catalog, "import_run")
    relation = storage_relation(schema, storage.HEADER)
    run = await connection.fetchrow(
        f"SELECT status,started_at,finished_at,params FROM {run_relation} WHERE run_id=$1 "
        "AND importer='provider-directory-fhir' AND engine='healthcare-mrf-api' FOR SHARE NOWAIT",
        run_id,
    )
    storage._require(
        run is not None
        and (
            run["status"] == "failed" and run["finished_at"] is not None
            if predecessor
            else run["status"] == "queued" and run["started_at"] is None and run["finished_at"] is None
        ),
        "recovery_run_changed",
    )
    headers = await connection.fetch(
        f"SELECT dataset_id,endpoint_id,acquisition_root_run_id,publication_metadata_json,candidate_storage_json "
        f"FROM {relation} WHERE import_run_id=$1 AND status=$2 AND NOT is_current AND published_at IS NULL "
        "LIMIT 2 FOR NO KEY UPDATE NOWAIT",
        run_id,
        "failed" if predecessor else "acquiring",
    )
    storage._require(len(headers) == 1, "recovery_candidate_missing")
    (header,) = headers
    binding = storage._json(header["candidate_storage_json"])
    metadata = storage._json(header["publication_metadata_json"])
    storage._require(type(binding) is dict and type(metadata) is dict, "recovery_candidate_changed")
    identity = binding["identity"]
    storage_by_field = {"catalog_schema": catalog, "storage_schema": schema}
    storage._validate_binding_shape(binding, identity, phases=("loading",))
    storage._validate_isolated_binding_storage(binding, storage_by_field)
    storage._validate_binding_tables(binding, identity)
    storage._require(
        binding["contract"] == storage.NAMED_ISOLATED_CONTRACT
        and all(header[field] == identity[field] for field in ("dataset_id", "endpoint_id", "acquisition_root_run_id"))
        and all(metadata.get(key) == field_value for key, field_value in identity["metadata"].items())
        and (predecessor or identity["acquisition_root_run_id"] == run_id and binding["ordinal"] == 0),
        "recovery_candidate_changed",
    )
    return binding, storage._json(run["params"])


async def _checkpoints(connection, schema, binding, run_id, base, sources, *, predecessor):
    relation = storage_relation(schema, _CHECKPOINT)
    identity = binding["identity"]
    rows = await connection.fetch(
        f"SELECT * FROM {relation} WHERE dataset_id=$1 FOR UPDATE NOWAIT", identity["dataset_id"]
    )
    storage._require(
        bool(rows)
        and len({row["resource_type"] for row in rows}) == len(rows)
        and len({row["source_scope_hash"] for row in rows}) == 1
        and all(
            row["canonical_api_base"] == base
            and row["owner_run_id"] == run_id
            and row["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
            and storage._json(row["source_ids"]) == sources
            and row["resource_type"].removesuffix(_PARTITION) in identity["metadata"]["selected_resources"]
            and (
                (row["state"] == "complete") == (row["completed_at"] is not None)
                and row["state"] in {"active", "complete"}
                if predecessor
                else row["state"] == "active"
                and row["rows_processed"] == row["pages_processed"] == 0
                and row["completed_at"] is None
            )
            for row in rows
        ),
        "recovery_checkpoints_changed",
    )
    return {row["resource_type"]: row for row in rows}


def _input_selection(binding, schema, kinds):
    name = binding["tables"]["resource_input"]["name"]
    relation = storage_relation(schema, name)
    query = (
        f"SELECT {_INPUT_SELECTION} FROM {relation} "
        "WHERE dataset_id=$1 AND resource_type=ANY($2::text[]) AND batch_ordinal <= $3 "
        "AND batch_ordinal > greatest(coalesce(($4::jsonb->>'*')::bigint,0),"
        "coalesce(($4::jsonb->>resource_type)::bigint,0))"
    )
    return query, (
        binding["identity"]["dataset_id"],
        sorted(kinds),
        binding["ordinal"],
        storage._encoded(binding["resets"]),
    )


async def _copy_records(connection, schema, table, columns, cursor, transform):
    """Keep both the driver prefetch and native COPY frames bounded."""

    async def flush(records):
        """Write one bounded native batch to the prepared target."""
        status = await connection.copy_records_to_table(table, schema_name=schema, columns=columns, records=records)
        storage._require(status == f"COPY {len(records)}", "recovery_copy_incomplete")

    batch, charged = [], COPY_FRAME_BYTES
    async for row in cursor:
        record = transform(row)
        size = copy_record_bytes(record)
        if batch and (len(batch) == COPY_MAX_ROWS or charged + size > COPY_MAX_BYTES):
            await flush(batch)
            batch, charged = [], COPY_FRAME_BYTES
        batch.append(record)
        charged += size
    if batch:
        await flush(batch)


def _rebound_shard(row, predecessor, successor, source_ids, families, totals):
    old, new = predecessor["identity"], successor["identity"]
    payload = bytes(row["payload_bytes"])
    lines, decoded = proof._validated_shard_lines(row, payload)
    storage._require(
        row["endpoint_id"] == old["endpoint_id"]
        and row["acquisition_root_run_id"] == old["acquisition_root_run_id"]
        and decoded["source_ids_json"] == source_ids
        and decoded["resource_counts_json"].keys() <= families
        and row["shard_id"]
        == proof._json_hash(
            [old["dataset_id"], old["endpoint_id"], old["acquisition_root_run_id"], source_ids, row["input_sha256"]]
        ),
        "recovery_proof_changed",
    )
    descriptor = proof._proof_shard_descriptor(
        lines,
        cleaned_dataset_id=new["dataset_id"],
        cleaned_endpoint_id=new["endpoint_id"],
        cleaned_root_run_id=new["acquisition_root_run_id"],
        cleaned_source_ids=source_ids,
        input_sha256=row["input_sha256"],
        artifact_sha256=row["artifact_sha256"],
        artifact_byte_count=len(payload),
    )
    totals.update(descriptor["resource_counts"])
    values = proof._proof_shard_insert_params(descriptor, payload)
    return tuple(values[column] for column in _SHARD_COLUMNS)


async def _copy_checkpoints(connection, schema, successor, new_scope, complete):
    relation = storage_relation(schema, _CHECKPOINT)
    new = successor["identity"]
    for checkpoint in complete.values():
        status = await connection.execute(
            f"""INSERT INTO {relation} AS dst (
                canonical_api_base,resource_type,source_scope_hash,dataset_id,source_ids,acquisition_root_run_id,
                owner_run_id,start_url_hash,next_url,state,pages_processed,rows_processed,recent_cursor_hashes,
                completeness_json,created_at,updated_at,completed_at
            ) VALUES ($1,$2,$3,$4,$5::json,$6,$6,$7,$8,'complete',$9,$10,$11::json,$12::json,$13,now(),$14)
            ON CONFLICT (canonical_api_base,resource_type,source_scope_hash,acquisition_root_run_id) DO UPDATE SET
                next_url=excluded.next_url,state=excluded.state,pages_processed=excluded.pages_processed,
                rows_processed=excluded.rows_processed,recent_cursor_hashes=excluded.recent_cursor_hashes,
                completeness_json=excluded.completeness_json,updated_at=excluded.updated_at,completed_at=excluded.completed_at
            WHERE dst.dataset_id=excluded.dataset_id AND dst.owner_run_id=excluded.owner_run_id
              AND dst.start_url_hash=excluded.start_url_hash AND dst.state='active'
              AND dst.pages_processed=0 AND dst.rows_processed=0 AND dst.completed_at IS NULL""",
            checkpoint["canonical_api_base"],
            checkpoint["resource_type"],
            new_scope,
            new["dataset_id"],
            storage._json_text(checkpoint["source_ids"]),
            new["acquisition_root_run_id"],
            checkpoint["start_url_hash"],
            checkpoint["next_url"],
            checkpoint["pages_processed"],
            checkpoint["rows_processed"],
            storage._json_text(checkpoint["recent_cursor_hashes"]),
            storage._json_text(checkpoint["completeness_json"]),
            checkpoint["created_at"],
            checkpoint["completed_at"],
        )
        storage._require(status == "INSERT 0 1", "recovery_checkpoint_changed")


async def carry_forward_completed_fhir_inputs(
    connection,
    *,
    catalog_schema,
    predecessor_schema,
    successor_schema,
    failed_run_id,
    successor_run_id,
    require_stopped,
):
    """Transfer complete evidence under the stopped-worker and queued-worker fences.

    The Source operator verifies actual worker absence under the worker locks.
    """
    storage._require(
        connection.is_in_transaction()
        and await connection.fetchval("SHOW transaction_isolation") == "serializable"
        and len({catalog_schema, predecessor_schema, successor_schema}) == 3
        and failed_run_id != successor_run_id,
        "recovery_transaction_required",
    )
    async with connection.transaction():
        return await _carry_completed(
            connection,
            catalog_schema,
            predecessor_schema,
            successor_schema,
            failed_run_id,
            successor_run_id,
            require_stopped,
        )


async def _locked_recovery_pair(
    connection,
    catalog_schema,
    predecessor_schema,
    successor_schema,
    failed_run_id,
    successor_run_id,
    require_stopped,
):
    for run_id in sorted((failed_run_id, successor_run_id)):
        storage._require(
            await connection.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", "control-run-worker-action:v1:" + run_id
            ),
            "recovery_worker_action_busy",
        )
        await require_stopped(run_id)
    predecessor, old_params = await _loading_candidate(
        connection, catalog_schema, predecessor_schema, failed_run_id, predecessor=True
    )
    successor, new_params = await _loading_candidate(
        connection, catalog_schema, successor_schema, successor_run_id, predecessor=False
    )
    storage._require(
        await connection.fetchval(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", predecessor["identity"]["endpoint_id"]
        ),
        "recovery_endpoint_busy",
    )
    return predecessor, successor, old_params, new_params


def _require_recovery_scope(directory, predecessor, successor, old_params, new_params):
    old, new = predecessor["identity"], successor["identity"]
    source_ids = old["metadata"]["source_ids"]
    base = directory.bcbs_minnesota.API_BASE
    fields = (
        "source_ids",
        "selected_resources",
        "expected_resources",
        "resource_hash_contract",
    )
    storage._require(
        old["endpoint_id"] == new["endpoint_id"]
        and old["dataset_id"] != new["dataset_id"]
        and old["acquisition_root_run_id"] != new["acquisition_root_run_id"]
        and len(source_ids) == 1
        and all(old["metadata"].get(field) == new["metadata"].get(field) for field in fields)
        and set(new["metadata"]["selected_resources"]) == set(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)
        and set(new["metadata"]["expected_resources"]) == set(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)
        and all(
            params.get("provider_directory_endpoint_scope") == base
            and params.get("source_ids") == source_ids
            and params.get("allow_retained_pagination_reset") is False
            and params.get("full_refresh") is True
            and params.get("import_resources") is True
            and not any(params.get(key) for key in ("page_limit", "resource_limit", "bulk_export"))
            for params in (old_params, new_params)
        ),
        "recovery_scope_changed",
    )


async def _select_recovery_rows(connection, directory, predecessor, successor, prior, pending_by_kind):
    complete_by_kind = {
        kind: checkpoint
        for kind, checkpoint in prior.items()
        if checkpoint["state"] == "complete" and checkpoint["completed_at"]
    }
    families = {kind.removesuffix(_PARTITION) for kind in complete_by_kind}
    partitions = {kind.removesuffix(_PARTITION) for kind in complete_by_kind if kind.endswith(_PARTITION)}
    storage._require(
        0 < len(families) == len(complete_by_kind) < len(predecessor["identity"]["metadata"]["selected_resources"]),
        "recovery_scope_changed",
    )
    # The projection date affects the normalized fields of Practitioner only.
    projection_date = directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY
    storage._require(
        "Practitioner" not in families
        or predecessor["identity"]["metadata"].get(projection_date)
        == successor["identity"]["metadata"].get(projection_date),
        "recovery_practitioner_projection_changed",
    )
    kinds = families | {
        directory._last_updated_partition_fingerprint_resource_type(family, number)
        for family in partitions
        for number in (1, 2)
    }
    origin_query, arguments = _input_selection(predecessor, predecessor["storage"]["storage_schema"], kinds)
    counts = dict.fromkeys(kinds, 0)
    counts.update(
        (checkpoint["resource_type"], checkpoint["count"])
        for checkpoint in await connection.fetch(
            f"SELECT resource_type,count(*) AS count FROM ({origin_query}) AS selected GROUP BY resource_type",
            *arguments,
        )
    )
    for kind, checkpoint in complete_by_kind.items():
        family = kind.removesuffix(_PARTITION)
        if kind.endswith(_PARTITION):
            storage._require(
                kind in pending_by_kind and pending_by_kind[kind]["start_url_hash"] == checkpoint["start_url_hash"],
                "recovery_partition_changed",
            )
            _verify_partition(directory, checkpoint, family, counts)
        else:
            storage._require(
                checkpoint["next_url"] is None and checkpoint["rows_processed"] == counts[family],
                "recovery_count_changed",
            )
        storage._require(counts[family] > 0 and checkpoint["pages_processed"] > 0, "recovery_count_changed")
    return complete_by_kind, {family: counts[family] for family in sorted(families)}, origin_query, arguments


async def _copy_recovery_rows(connection, successor, origin_query, arguments):
    successor_schema = successor["storage"]["storage_schema"]
    dataset_id = successor["identity"]["dataset_id"]
    name = successor["tables"]["resource_input"]["name"]
    relation = storage_relation(successor_schema, name)
    shards = storage_relation(successor_schema, _PROOF)
    storage._require(
        not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {relation})")
        and not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {shards} WHERE dataset_id=$1)", dataset_id),
        "recovery_target_not_empty",
    )
    await _copy_records(
        connection,
        successor_schema,
        name,
        _INPUT_COLUMNS,
        connection.cursor(origin_query, *arguments, prefetch=32),
        lambda row: (dataset_id, *row),
    )
    # Compare exact content commitments without sorting large JSON or compressed payloads.
    before = f"SELECT {_INPUT_PARITY} FROM ({origin_query}) AS prior"
    after = f"SELECT {_INPUT_PARITY} FROM {relation} WHERE dataset_id=$5"
    storage._require(
        not await connection.fetchval(
            f"SELECT EXISTS(({before} EXCEPT ALL {after}) UNION ALL ({after} EXCEPT ALL {before}))",
            *arguments,
            dataset_id,
        ),
        "recovery_payload_parity_failed",
    )


async def _copy_recovery_proof(connection, predecessor, successor, counts):
    old, new = predecessor["identity"], successor["identity"]
    source_ids = old["metadata"]["source_ids"]
    families = counts.keys()
    predecessor_schema = predecessor["storage"]["storage_schema"]
    successor_schema = successor["storage"]["storage_schema"]
    prior_shards = storage_relation(predecessor_schema, _PROOF)
    shards = storage_relation(successor_schema, _PROOF)
    totals = Counter()
    await _copy_records(
        connection,
        successor_schema,
        _PROOF,
        _SHARD_COLUMNS,
        connection.cursor(
            f"SELECT * FROM {prior_shards} WHERE dataset_id=$1 AND resource_counts_json ?| $2::text[]",
            old["dataset_id"],
            sorted(families),
            prefetch=16,
        ),
        lambda row: _rebound_shard(row, predecessor, successor, source_ids, families, totals),
    )
    storage._require(totals == counts, "recovery_proof_count_changed")
    shard_fields = ", ".join(
        "sha256(payload_bytes)" if column == "payload_bytes" else column
        for column in _SHARD_COLUMNS
        if column not in {"dataset_id", "acquisition_root_run_id", "shard_id"}
    )
    old_projection = (
        f"SELECT {shard_fields} FROM {prior_shards} WHERE dataset_id=$1 AND resource_counts_json ?| $2::text[]"
    )
    new_projection = f"SELECT {shard_fields} FROM {shards} WHERE dataset_id=$3"
    storage._require(
        not await connection.fetchval(
            f"SELECT EXISTS(({old_projection} EXCEPT ALL {new_projection}) UNION ALL "
            f"({new_projection} EXCEPT ALL {old_projection}))",
            old["dataset_id"],
            sorted(families),
            new["dataset_id"],
        ),
        "recovery_proof_parity_failed",
    )


async def _record_recovery_allocation(connection, predecessor, successor):
    successor_schema = successor["storage"]["storage_schema"]
    relation = storage_relation(successor_schema, successor["tables"]["resource_input"]["name"])
    ordinal = await connection.fetchval(f"SELECT max(batch_ordinal) FROM {relation}")
    sequence = await connection.fetchval(
        "SELECT relname FROM pg_class WHERE oid=pg_get_serial_sequence($1,'batch_ordinal')::regclass", relation
    )
    storage._require(type(ordinal) is int and 0 < ordinal <= predecessor["ordinal"], "recovery_ordinal_changed")
    await connection.execute(
        f"ALTER SEQUENCE {storage_relation(successor_schema, sequence)} RESTART WITH {ordinal + 1}"
    )
    await storage._store(
        connection,
        SimpleNamespace(schema=successor_schema),
        successor["identity"],
        successor,
        {**successor, "ordinal": ordinal},
    )


async def _carry_completed(
    connection,
    catalog_schema,
    predecessor_schema,
    successor_schema,
    failed_run_id,
    successor_run_id,
    require_stopped,
):
    directory = import_module("process.provider_directory_fhir")
    predecessor, successor, old_params, new_params = await _locked_recovery_pair(
        connection,
        catalog_schema,
        predecessor_schema,
        successor_schema,
        failed_run_id,
        successor_run_id,
        require_stopped,
    )
    _require_recovery_scope(directory, predecessor, successor, old_params, new_params)
    base = directory.bcbs_minnesota.API_BASE
    source_ids = predecessor["identity"]["metadata"]["source_ids"]
    prior = await _checkpoints(
        connection, predecessor_schema, predecessor, failed_run_id, base, source_ids, predecessor=True
    )
    pending_by_kind = await _checkpoints(
        connection, successor_schema, successor, successor_run_id, base, source_ids, predecessor=False
    )
    complete, counts, origin_query, arguments = await _select_recovery_rows(
        connection, directory, predecessor, successor, prior, pending_by_kind
    )
    await _copy_recovery_rows(connection, successor, origin_query, arguments)
    await _copy_recovery_proof(connection, predecessor, successor, counts)
    await _record_recovery_allocation(connection, predecessor, successor)
    await _copy_checkpoints(
        connection, successor_schema, successor, next(iter(pending_by_kind.values()))["source_scope_hash"], complete
    )
    return counts
