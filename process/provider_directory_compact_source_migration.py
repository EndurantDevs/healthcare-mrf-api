# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resume a bounded conversion of an existing named candidate's observations.

The legacy heaps remain authoritative until the header switches atomically.
Progress, compact COPY and its data validation share each existing source
transaction; upstream checkpoints are never changed by this module.
"""

import asyncio
import hashlib
import logging
import re
import time

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_storage as layout
from process import provider_directory_generic_candidate as candidate_rows
from process import provider_directory_generic_source_policy as policy_scope
from process import provider_directory_generic_source_storage as storage
from process.native_copy_cleanup import cleanup_interrupted_copy
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

LOGGER = logging.getLogger(__name__)
KEY = "payload_conversion"
_SHARED_CHUNK_MAX_ROWS = 2048
_LEGACY_VERIFY_SAMPLE_ROWS = 256
_FIELDS = {
    "format",
    "state",
    "legacy_tables",
    "compact_tables",
    "ordinal",
    "resets",
    "after",
    "rows",
    "total_rows",
    "sequence",
}


async def initialize_source_candidate(directory, candidate, partition_initializations):
    """Preserve existing initialization overloads before independent conversion transactions."""
    if partition_initializations:
        initialized = await directory._ensure_endpoint_dataset_candidate(candidate, partition_initializations)
    else:
        initialized = await directory._ensure_endpoint_dataset_candidate(candidate)
    return initialized if isinstance(initialized, directory.EndpointDatasetCandidate) else candidate


async def prepare_retained_prefix(directory, candidate, cancellation):
    """Convert only the selected intake before its first REST acquisition."""
    return await migrate_source_candidate(
        directory.db,
        directory,
        candidate,
        check_cancelled=cancellation.check,
        payload_format=policy_scope.source_payload_format(),
    )


def validate_conversion(binding):
    """Validate only logical names, original ordering and durable conversion progress."""
    progress_by_field = binding[KEY]
    storage._require(
        binding["contract"] in storage.NAMED_CONTRACTS
        and type(progress_by_field) is dict
        and set(progress_by_field) == _FIELDS,
        "payload_conversion_changed",
    )
    layout.require_source_identity(binding["identity"], payload_format=progress_by_field["format"])
    _validate_progress(binding, progress_by_field)
    _validate_conversion_tables(binding, progress_by_field)
    if progress_by_field["state"] == "copying":
        storage._require(
            binding["contract"] == storage.NAMED_ISOLATED_CONTRACT
            and binding["phase"] == "loading"
            and binding["tables"] == progress_by_field["legacy_tables"]
            and binding["ordinal"] == progress_by_field["ordinal"]
            and binding["resets"] == progress_by_field["resets"],
            "payload_conversion_changed",
        )
    else:
        storage._require(
            binding["contract"] == storage.NAMED_COMPACT_CONTRACT
            and binding["payload_format"] == progress_by_field["format"]
            and progress_by_field["rows"] == progress_by_field["total_rows"]
            and all(
                binding["tables"].get(root) == table for root, table in progress_by_field["compact_tables"].items()
            ),
            "payload_conversion_changed",
        )


def _validate_progress(binding, progress_by_field):
    """Require bounded committed coordinates and the original allocation high-water."""
    storage._require(
        progress_by_field["format"] in (compact.FORMAT, compact.SHARED_BODY_FORMAT)
        and type(progress_by_field["state"]) is str
        and progress_by_field["state"] in {"copying", "retained"}
        and type(progress_by_field["ordinal"]) is int
        and 0 <= progress_by_field["ordinal"] <= binding["ordinal"]
        and type(progress_by_field["resets"]) is dict
        and all(
            type(number) is int and 0 <= number <= progress_by_field["ordinal"]
            for number in progress_by_field["resets"].values()
        )
        and type(progress_by_field["rows"]) is int
        and type(progress_by_field["total_rows"]) is int
        and 0 <= progress_by_field["rows"] <= progress_by_field["total_rows"]
        and type(progress_by_field["after"]) is list
        and len(progress_by_field["after"]) == 2
        and all(type(number) is int for number in progress_by_field["after"])
        and (
            (progress_by_field["rows"] == 0 and progress_by_field["after"] == [0, -1])
            or (
                progress_by_field["rows"] > 0
                and 0 < progress_by_field["after"][0] <= progress_by_field["ordinal"]
                and 0 <= progress_by_field["after"][1] < candidate_rows.COPY_MAX_ROWS
            )
        )
        and type(progress_by_field["sequence"]) is list
        and len(progress_by_field["sequence"]) == 2
        and type(progress_by_field["sequence"][0]) is int
        and 0 < progress_by_field["sequence"][0] < 2**63
        and type(progress_by_field["sequence"][1]) is bool
        and (
            progress_by_field["sequence"][0] >= progress_by_field["ordinal"]
            if progress_by_field["sequence"][1]
            else progress_by_field["ordinal"] == 0
        ),
        "payload_conversion_changed",
    )


def _validate_conversion_tables(binding, progress_by_field):
    """Keep alternate inventories closed, disjoint and within the same logical candidate."""
    legacy = progress_by_field["legacy_tables"]
    storage._require(
        type(legacy) is dict and set(legacy) == {"resource_input", "witness_input"}, "payload_conversion_tables"
    )
    for root, suffix in (("resource_input", "ri"), ("witness_input", "wi")):
        table = legacy[root]
        storage._require(
            type(table) is dict
            and set(table) in ({"name"}, {"schema", "name"})
            and ("schema" not in table or table["schema"] == binding.get("storage", {}).get("storage_schema"))
            and type(table["name"]) is str
            and re.fullmatch(r"pdgs_[0-9a-f]{32}_" + suffix, table["name"]),
            "payload_conversion_tables",
        )
    storage._require(
        legacy["resource_input"]["name"][:-3] == legacy["witness_input"]["name"][:-3], "payload_conversion_tables"
    )
    layout.validate_tables(
        {
            **binding,
            "phase": "loading",
            "payload_format": progress_by_field["format"],
            "tables": progress_by_field["compact_tables"],
        }
    )
    storage._require(
        not (
            {table["name"] for table in legacy.values()}
            & {table["name"] for table in progress_by_field["compact_tables"].values()}
        ),
        "payload_conversion_tables",
    )


def _legacy_relation(policy, binding):
    return storage_relation(policy.schema, binding[KEY]["legacy_tables"]["resource_input"]["name"])


async def _sequence(connection, schema, relation):
    """Read the ordinary allocation high-water, including rolled-back sequence gaps."""
    parts = await connection.fetchval("SELECT parse_ident(pg_get_serial_sequence($1,'batch_ordinal'))", relation)
    storage._require(
        type(parts) is list and len(parts) == 2 and parts[0] == schema,
        "payload_conversion_sequence",
    )
    name = storage_relation(schema, parts[1])
    values = await connection.fetchrow(f"SELECT last_value,is_called FROM {name}")
    return name, [values["last_value"], values["is_called"]]


async def _begin(connection, policy, previous, directory, payload_format):
    """Persist target names before the first separately committed payload segment."""
    relation = storage_relation(policy.schema, previous["tables"]["resource_input"]["name"])
    witness = storage_relation(policy.schema, previous["tables"]["witness_input"]["name"])
    storage._require(
        not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {witness})"), "compact_witness_unsupported"
    )
    selected = previous["identity"]["metadata"]["selected_resources"]
    fingerprint_types = [
        directory._last_updated_partition_fingerprint_resource_type(resource_type, number)
        for resource_type in selected
        for number in (1, 2)
    ]
    summary = await connection.fetchrow(
        f"SELECT count(*) AS rows,coalesce(max(batch_ordinal),0) AS ordinal,"
        "bool_or(dataset_id IS DISTINCT FROM $1 OR resource_type<>ALL($2::text[]) "
        "OR batch_ordinal<=0 OR row_ordinal<0 OR row_ordinal>=4096) AS invalid "
        f"FROM {relation}",
        previous["identity"]["dataset_id"],
        [*selected, *fingerprint_types],
    )
    storage._require(
        not summary["invalid"] and summary["ordinal"] == previous["ordinal"], "payload_conversion_input_changed"
    )
    name = previous["tables"]["resource_input"]["name"] + "_conversion_order"
    await connection.execute(f"CREATE UNIQUE INDEX {storage_identifier(name)} ON {relation}(batch_ordinal,row_ordinal)")
    _, sequence = await _sequence(connection, policy.schema, relation)
    is_deferred_indexes = payload_format == compact.SHARED_BODY_FORMAT
    progress_by_field = {
        "format": payload_format,
        "state": "copying",
        "legacy_tables": previous["tables"],
        "compact_tables": await layout.create_inputs(
            connection, policy, payload_format=payload_format, defer_secondary_indexes=is_deferred_indexes
        ),
        "ordinal": previous["ordinal"],
        "resets": previous["resets"],
        "after": [0, -1],
        "rows": 0,
        "total_rows": summary["rows"],
        "sequence": sequence,
    }
    current_binding_by_field = {**previous, KEY: progress_by_field}
    validate_conversion(current_binding_by_field)
    await storage._store(connection, policy, previous["identity"], previous, current_binding_by_field)
    return current_binding_by_field


async def _keys(connection, policy, binding):
    """Choose scalar keys/decoded byte charges before transferring any JSON bodies."""
    progress_by_field = binding[KEY]
    relation = _legacy_relation(policy, binding)
    keys = await connection.fetch(
        "SELECT batch_ordinal,row_ordinal,greatest(octet_length(payload_json::text),"
        "octet_length(payload_json::jsonb::text))+octet_length(dataset_id)+octet_length(resource_type)"
        "+octet_length(resource_id)+octet_length(payload_hash)+coalesce(octet_length(acquired_resource_sha256),0)"
        f"+512 AS bytes FROM {relation} WHERE batch_ordinal=(SELECT min(batch_ordinal) FROM {relation} "
        "WHERE (batch_ordinal,row_ordinal)>($1,$2)) AND (batch_ordinal,row_ordinal)>($1,$2) "
        "ORDER BY batch_ordinal,row_ordinal LIMIT $3",
        *progress_by_field["after"],
        candidate_rows.COPY_MAX_ROWS,
    )
    selected, charged = [], candidate_rows.COPY_FRAME_BYTES
    for key in keys:
        storage._require(type(key["bytes"]) is int and key["bytes"] > 0, "payload_conversion_row_invalid")
        if selected and (
            key["batch_ordinal"] != selected[0]["batch_ordinal"]
            or charged + key["bytes"] > candidate_rows.COPY_MAX_BYTES
        ):
            break
        storage._require(charged + key["bytes"] <= candidate_rows.COPY_MAX_BYTES, "payload_conversion_row_too_large")
        selected.append(key)
        charged += key["bytes"]
    return selected


async def _read_segment(connection, policy, binding, keys):
    """Fetch only a precharged contiguous portion of one original batch."""
    observations = await connection.fetch(
        f"SELECT {','.join(candidate_rows.RESOURCE_COLUMNS)},batch_ordinal,row_ordinal "
        f"FROM {_legacy_relation(policy, binding)} WHERE batch_ordinal=$1 AND row_ordinal=ANY($2::integer[]) "
        "ORDER BY row_ordinal",
        keys[0]["batch_ordinal"],
        [key["row_ordinal"] for key in keys],
    )
    storage._require(
        len(observations) == len(keys)
        and [observation["row_ordinal"] for observation in observations]
        == list(range(keys[0]["row_ordinal"], keys[0]["row_ordinal"] + len(observations))),
        "payload_conversion_input_changed",
    )
    return observations


async def _verify_segment(connection, tables, binding, keys, observations):
    """Compare all original columns with independently reconstructed bounded output."""
    from process.provider_directory_compact_source_completion import _bounded_resources

    expected_by_ordinal = {
        (observation["batch_ordinal"], observation["row_ordinal"]): dict(observation) for observation in observations
    }
    expected_keys = {(key["batch_ordinal"], key["row_ordinal"]) for key in keys}
    storage._require(
        len(expected_keys) == len(keys) == len(observations) == len(expected_by_ordinal)
        and set(expected_by_ordinal) == expected_keys,
        "payload_conversion_data_changed",
    )
    query = (
        f"SELECT batch_ordinal,row_ordinal,read_bytes FROM {tables.relation('aliases')} "
        "WHERE dataset_id=$1 AND batch_ordinal=$2 AND row_ordinal=ANY($3::integer[]) "
        "ORDER BY batch_ordinal,row_ordinal"
    )
    seen_keys = set()
    async for observation in _bounded_resources(
        connection,
        tables,
        binding["identity"]["dataset_id"],
        query,
        binding["identity"]["dataset_id"],
        keys[0]["batch_ordinal"],
        [key["row_ordinal"] for key in keys],
    ):
        key = (observation["batch_ordinal"], observation["row_ordinal"])
        storage._require(
            key in expected_by_ordinal
            and key not in seen_keys
            and all(
                storage._encoded(storage._json(observation[name]))
                == storage._encoded(storage._json(expected_by_ordinal[key][name]))
                if name == "payload_json"
                else observation[name] == expected_by_ordinal[key][name]
                for name in candidate_rows.RESOURCE_COLUMNS
            ),
            "payload_conversion_data_changed",
        )
        seen_keys.add(key)
    storage._require(seen_keys == expected_keys, "payload_conversion_data_changed")


async def _copy_segment(connection, policy, binding, keys):
    """Verify the bounded reconstructed destination before committing progress."""
    progress_by_field = binding[KEY]
    observations = await _read_segment(connection, policy, binding, keys)
    resources = [{name: observation[name] for name in candidate_rows.RESOURCE_COLUMNS} for observation in observations]
    tables = layout.payload_tables(policy, progress_by_field["compact_tables"])
    try:
        await compact.append_compact_payload_segment(
            connection,
            tables,
            resources,
            dataset_id=binding["identity"]["dataset_id"],
            batch_ordinal=keys[0]["batch_ordinal"],
            row_start=keys[0]["row_ordinal"],
        )
    except (asyncio.CancelledError, TimeoutError) as interruption:
        await cleanup_interrupted_copy(policy_scope.source_copy_session(), connection, interruption)
        raise
    await _verify_segment(connection, tables, binding, keys, observations)
    current_binding_by_field = {
        **binding,
        KEY: {
            **progress_by_field,
            "after": [keys[-1]["batch_ordinal"], keys[-1]["row_ordinal"]],
            "rows": progress_by_field["rows"] + len(keys),
        },
    }
    validate_conversion(current_binding_by_field)
    await storage._store(connection, policy, binding["identity"], binding, current_binding_by_field)
    return current_binding_by_field


async def _read_shared_chunk(connection, policy, binding):
    """Fetch one byte-bounded multi-batch chunk in a single TOAST read."""
    progress_by_field = binding[KEY]
    relation = _legacy_relation(policy, binding)
    byte_budget = candidate_rows.COPY_MAX_BYTES - candidate_rows.COPY_FRAME_BYTES
    row_limit = min(candidate_rows.COPY_MAX_ROWS, _SHARED_CHUNK_MAX_ROWS)
    observations = await connection.fetch(
        f"WITH candidates AS (SELECT {','.join(candidate_rows.RESOURCE_COLUMNS)},batch_ordinal,row_ordinal,"
        "octet_length(payload_json::text)+octet_length(dataset_id)+octet_length(resource_type)"
        "+octet_length(resource_id)+octet_length(payload_hash)+coalesce(octet_length(acquired_resource_sha256),0)"
        f"+512 AS row_bytes FROM {relation} WHERE (batch_ordinal,row_ordinal)>($1,$2) "
        "ORDER BY batch_ordinal,row_ordinal LIMIT $3),"
        "bounded AS (SELECT *,sum(row_bytes) OVER (ORDER BY batch_ordinal,row_ordinal) AS running_bytes FROM candidates) "
        f"SELECT {','.join(candidate_rows.RESOURCE_COLUMNS)},batch_ordinal,row_ordinal,row_bytes FROM bounded "
        "WHERE running_bytes<=$4 OR (batch_ordinal,row_ordinal)=(SELECT batch_ordinal,row_ordinal FROM candidates LIMIT 1) "
        "ORDER BY batch_ordinal,row_ordinal",
        *progress_by_field["after"],
        row_limit,
        byte_budget,
    )
    if not observations:
        return []
    first_bytes = observations[0]["row_bytes"]
    storage._require(type(first_bytes) is int and first_bytes > 0, "payload_conversion_row_invalid")
    storage._require(first_bytes <= byte_budget, "payload_conversion_row_too_large")
    return observations


def _group_chunk_segments(observations):
    """Partition an ordered chunk into contiguous original batch segments."""
    segments = []
    current_batch = None
    current_start = None
    expected_ordinal = None
    batch_resources = []
    for observation in observations:
        batch_ordinal = observation["batch_ordinal"]
        row_ordinal = observation["row_ordinal"]
        resource_by_field = {name: observation[name] for name in candidate_rows.RESOURCE_COLUMNS}
        if batch_ordinal == current_batch:
            storage._require(row_ordinal == expected_ordinal, "payload_conversion_input_changed")
            batch_resources.append(resource_by_field)
            expected_ordinal += 1
            continue
        if current_batch is not None:
            storage._require(batch_ordinal > current_batch, "payload_conversion_input_changed")
            segments.append((current_batch, current_start, batch_resources))
        current_batch = batch_ordinal
        current_start = row_ordinal
        expected_ordinal = row_ordinal + 1
        batch_resources = [resource_by_field]
    if current_batch is not None:
        segments.append((current_batch, current_start, batch_resources))
    return segments


async def _verify_reconstructed_sample(connection, tables, dataset_id, sampled_observations):
    """Compare reconstructed JSON byte-for-byte against original observations."""
    sample_by_key = {(obs["batch_ordinal"], obs["row_ordinal"]): dict(obs) for obs in sampled_observations}
    alias_keys = await connection.fetch(
        f"SELECT batch_ordinal,row_ordinal,read_bytes FROM {tables.relation('aliases')} "
        "WHERE dataset_id=$1 AND (batch_ordinal,row_ordinal) IN(SELECT * FROM unnest($2::bigint[],$3::integer[])) "
        "ORDER BY batch_ordinal,row_ordinal",
        dataset_id,
        [obs["batch_ordinal"] for obs in sampled_observations],
        [obs["row_ordinal"] for obs in sampled_observations],
    )
    storage._require(len(alias_keys) == len(sample_by_key), "payload_conversion_data_changed")
    seen_keys = set()
    selected, charged = [], candidate_rows.COPY_FRAME_BYTES
    for alias_key in alias_keys:
        if selected and charged + alias_key["read_bytes"] > candidate_rows.COPY_MAX_BYTES:
            await _check_reconstructed_page(connection, tables, dataset_id, selected, sample_by_key, seen_keys)
            selected, charged = [], candidate_rows.COPY_FRAME_BYTES
        selected.append(alias_key)
        charged += alias_key["read_bytes"]
    if selected:
        await _check_reconstructed_page(connection, tables, dataset_id, selected, sample_by_key, seen_keys)
    storage._require(seen_keys == set(sample_by_key), "payload_conversion_data_changed")


async def _check_reconstructed_page(connection, tables, dataset_id, selected, sample_by_key, seen_keys):
    reconstructed = await compact.read_compact_payload_keys(connection, tables, dataset_id, selected)
    storage._require(len(reconstructed) == len(selected), "payload_conversion_data_changed")
    for actual in reconstructed:
        key = (actual["batch_ordinal"], actual["row_ordinal"])
        expected = sample_by_key.get(key)
        storage._require(
            expected is not None
            and key not in seen_keys
            and all(
                compact._json(compact._decode_payload(actual[name]))
                == compact._json(compact._decode_payload(expected[name]))
                if name == "payload_json"
                else actual[name] == expected[name]
                for name in candidate_rows.RESOURCE_COLUMNS
            ),
            "payload_conversion_data_changed",
        )
        seen_keys.add(key)


async def _verify_shared_chunk(connection, tables, binding, observations):
    """Check every alias/join row in SQL and full JSON reconstruction on boundary rows."""
    dataset_id = binding["identity"]["dataset_id"]
    mismatched = await connection.fetchval(
        "WITH expected AS MATERIALIZED ("
        "SELECT * FROM unnest($2::bigint[],$3::integer[],$4::text[],$5::text[],$6::text[],$7::text[]) "
        "e(batch_ordinal,row_ordinal,resource_type,resource_id,payload_hash,acquired_resource_sha256)) "
        "SELECT EXISTS(SELECT FROM expected e "
        f"LEFT JOIN {tables.relation('aliases')} a ON (a.dataset_id,a.batch_ordinal,a.row_ordinal)="
        "($1,e.batch_ordinal,e.row_ordinal) "
        f"LEFT JOIN {tables.relation('body_blocks')} b "
        "ON (b.dataset_id,b.batch_ordinal,b.block_ordinal)=($1,a.batch_ordinal,a.block_ordinal) "
        "WHERE a.batch_ordinal IS NULL OR a.resource_type<>e.resource_type OR a.resource_id<>e.resource_id "
        "OR a.payload_hash<>e.payload_hash OR a.acquired_resource_sha256 IS DISTINCT FROM e.acquired_resource_sha256 "
        "OR b.batch_ordinal IS NULL "
        f"OR NOT EXISTS(SELECT 1 FROM {tables.relation('shared_bodies')} d "
        "WHERE (d.dataset_id,d.body_hash)=($1,decode((b.bodies->a.body_ordinal)->>'body_hash','hex'))) "
        f"OR (a.service_field_state=2 AND NOT EXISTS(SELECT 1 FROM {tables.relation('reference_sets')} s "
        "WHERE (s.dataset_id,s.set_hash)=($1,a.service_set_hash))))",
        dataset_id,
        [obs["batch_ordinal"] for obs in observations],
        [obs["row_ordinal"] for obs in observations],
        [obs["resource_type"] for obs in observations],
        [obs["resource_id"] for obs in observations],
        [obs["payload_hash"] for obs in observations],
        [obs["acquired_resource_sha256"] for obs in observations],
    )
    storage._require(not mismatched, "payload_conversion_data_changed")
    total = len(observations)
    sample_indices = sorted({0, total // 3, (2 * total) // 3, total - 1})
    await _verify_reconstructed_sample(
        connection, tables, dataset_id, [observations[index] for index in sample_indices]
    )


async def _copy_shared_chunk(connection, policy, binding, observations, *, cache):
    """Copy and verify one multi-batch chunk under the current owner transaction."""
    progress_by_field = binding[KEY]
    segments = _group_chunk_segments(observations)
    tables = layout.payload_tables(policy, progress_by_field["compact_tables"])
    try:
        pending_cache = await compact.append_compact_payload_segment(
            connection,
            tables,
            segments[0][2],
            dataset_id=binding["identity"]["dataset_id"],
            batch_ordinal=segments[0][0],
            row_start=segments[0][1],
            segments=segments,
            cache=cache,
        )
    except (asyncio.CancelledError, TimeoutError) as interruption:
        await cleanup_interrupted_copy(policy_scope.source_copy_session(), connection, interruption)
        raise
    await _verify_shared_chunk(connection, tables, binding, observations)
    current_binding_by_field = {
        **binding,
        KEY: {
            **progress_by_field,
            "after": [observations[-1]["batch_ordinal"], observations[-1]["row_ordinal"]],
            "rows": progress_by_field["rows"] + len(observations),
        },
    }
    validate_conversion(current_binding_by_field)
    await storage._store(connection, policy, binding["identity"], binding, current_binding_by_field)
    return current_binding_by_field, pending_cache


async def _verify_legacy_sample(connection, policy, binding, tables):
    """Compare a repeatable sample of legacy rows against reconstructed compact rows."""
    dataset_id = binding["identity"]["dataset_id"]
    seed = hashlib.sha256(storage._encoded(binding["identity"]).encode()).hexdigest()
    legacy_relation = _legacy_relation(policy, binding)
    sampled_observations = await connection.fetch(
        f"SELECT {','.join(candidate_rows.RESOURCE_COLUMNS)},batch_ordinal,row_ordinal "
        f"FROM {legacy_relation} "
        "ORDER BY md5(batch_ordinal::text||':'||row_ordinal::text||$1),batch_ordinal,row_ordinal LIMIT $2",
        seed,
        _LEGACY_VERIFY_SAMPLE_ROWS,
    )
    if sampled_observations:
        await _verify_reconstructed_sample(connection, tables, dataset_id, sampled_observations)


async def _finish_shared_body_indexes(connection, policy, binding, tables):
    """Build deferred secondary indexes and refresh planner statistics before validation."""
    for statement in compact.compact_payload_secondary_index_ddl(tables, if_not_exists=True):
        await connection.execute(statement)
    await storage.refresh_statistics(
        connection,
        policy.schema,
        [
            table["name"]
            for root, table in binding["tables"].items()
            if root
            in {
                "resource_aliases",
                "resource_reference_sets",
                "resource_body_blocks",
                layout.SHARED_BODY_ROOT,
                layout.REFERENCE_TOKENS_ROOT,
            }
        ],
    )


async def _finish(connection, policy, binding):
    """Switch the verified candidate pointer and atomically reclaim legacy shared-body heaps."""
    from process.provider_directory_compact_source_completion import validate_storage

    progress_by_field = binding[KEY]
    storage._require(progress_by_field["rows"] == progress_by_field["total_rows"], "payload_conversion_incomplete")
    _, sequence = await _sequence(connection, policy.schema, _legacy_relation(policy, binding))
    storage._require(sequence == progress_by_field["sequence"], "payload_conversion_sequence_changed")
    is_shared_format = progress_by_field["format"] == compact.SHARED_BODY_FORMAT
    candidate_binding_by_field = {
        **binding,
        "contract": storage.NAMED_COMPACT_CONTRACT,
        "payload_format": progress_by_field["format"],
        "tables": progress_by_field["compact_tables"],
        KEY: {**progress_by_field, "state": "retained"},
    }
    validate_conversion(candidate_binding_by_field)
    tables = layout.payload_tables(policy, candidate_binding_by_field["tables"])
    if is_shared_format:
        await _finish_shared_body_indexes(connection, policy, candidate_binding_by_field, tables)
    await validate_storage(connection, policy, candidate_binding_by_field)
    aliases = tables.relation("aliases")
    count = await connection.fetchval(f"SELECT count(*) FROM {aliases}")
    storage._require(count == progress_by_field["total_rows"], "payload_conversion_data_changed")
    if is_shared_format:
        await _verify_legacy_sample(connection, policy, binding, tables)
    await connection.fetchval("SELECT setval(pg_get_serial_sequence($1,'batch_ordinal'),$2,$3)", aliases, *sequence)
    if is_shared_format:
        for legacy_table in progress_by_field["legacy_tables"].values():
            await connection.execute(f"DROP TABLE {storage_relation(policy.schema, legacy_table['name'])}")
        current_binding_by_field = {
            name: contents for name, contents in candidate_binding_by_field.items() if name != KEY
        }
        storage._validate_binding(current_binding_by_field, binding["identity"], phases=("loading",))
    else:
        current_binding_by_field = candidate_binding_by_field
    await storage._store(connection, policy, binding["identity"], binding, current_binding_by_field)
    return current_binding_by_field


async def _advance_conversion_step(connection, policy, directory, binding, payload_format, cache):
    """Execute one bounded initialization, copy or finish transaction step."""
    if KEY not in binding:
        return await _begin(connection, policy, binding, directory, payload_format), None
    storage._require(binding[KEY]["format"] == payload_format, "payload_format_changed")
    if payload_format == compact.SHARED_BODY_FORMAT:
        observations = await _read_shared_chunk(connection, policy, binding)
        if observations:
            return await _copy_shared_chunk(connection, policy, binding, observations, cache=cache)
        return await _finish(connection, policy, binding), None
    keys = await _keys(connection, policy, binding)
    updated = (
        await _copy_segment(connection, policy, binding, keys) if keys else await _finish(connection, policy, binding)
    )
    return updated, None


async def migrate_source_candidate(database, directory, candidate, *, check_cancelled=None, payload_format=None):
    """Convert an explicitly selected layout in independently committed segments."""
    if candidate is None:
        return None
    identity = storage.candidate_identity(directory, candidate)
    if payload_format is None:
        if identity["metadata"].get("source_ids") != [directory._stable_source_id(directory.anthem.seed_rows()[0])]:
            return None
        payload_format = compact.FORMAT
    storage._require(policy_scope._PUBLISHER.get() is None, "payload_conversion_outer_transaction")
    cache = compact.MigrationCache() if payload_format == compact.SHARED_BODY_FORMAT else None
    started_at = time.monotonic()
    while True:
        if check_cancelled is not None:
            await check_cancelled()
        async with policy_scope.source_transaction(database):
            publisher = policy_scope.bound_publisher()
            if not isinstance(publisher.storage_binding, NamedFHIRSourceStorage):
                return None
            storage._require_shared_payload_format(publisher, payload_format)
            connection, policy = publisher.connection, publisher.policy
            storage._require_candidate_storage(publisher, directory, identity)
            binding = await storage._locked(connection, policy, identity, allow_conversion=True)
            storage._require(binding is not None, "preparation_required")
            layout.require_source_identity(identity, payload_format=payload_format)
            if binding["contract"] == storage.NAMED_COMPACT_CONTRACT:
                storage._require(binding["payload_format"] == payload_format, "payload_format_changed")
                return binding
            binding, pending_cache = await _advance_conversion_step(
                connection, policy, directory, binding, payload_format, cache
            )
        if cache is not None and pending_cache:
            cache.apply(pending_cache)
        progress_by_field = binding.get(KEY)
        if progress_by_field is None:
            LOGGER.info(
                "compact_prefix_conversion state=released format=%s elapsed_s=%.1f",
                binding.get("payload_format"),
                time.monotonic() - started_at,
            )
            return binding
        LOGGER.info(
            "compact_prefix_conversion rows=%d total=%d state=%s",
            progress_by_field["rows"],
            progress_by_field["total_rows"],
            progress_by_field["state"],
        )
        if binding["contract"] == storage.NAMED_COMPACT_CONTRACT:
            return binding


async def discard_conversion_tables(connection, policy, binding):
    """Include only this conversion's alternate inventory in existing exact disposal."""
    progress_by_field = binding[KEY]
    tables_by_root = (
        progress_by_field["compact_tables"]
        if progress_by_field["state"] == "copying"
        else progress_by_field["legacy_tables"]
    )
    if progress_by_field["state"] == "copying":
        view = tables_by_root["resource_input"]["name"]
        await connection.execute(f"DROP VIEW {storage_relation(policy.schema, view)}")
        tables_by_root = {root: table for root, table in tables_by_root.items() if root != "resource_input"}
    for table in tables_by_root.values():
        await connection.execute(f"DROP TABLE {storage_relation(policy.schema, table['name'])}")


async def release_conversion_inputs(connection, policy, binding):
    """Retire verified legacy inputs in the caller's existing handoff transaction.

    Later handoff failure rolls back both DROP and the header CAS. All compact
    observations remain retained; portable publication receives no legacy names.
    """
    if KEY not in binding:
        return binding
    storage._require(connection.is_in_transaction(), "transaction_required")
    storage._validate_binding(binding, binding["identity"], phases=("prepared",))
    storage._require(
        binding["contract"] == storage.NAMED_COMPACT_CONTRACT and binding[KEY]["state"] == "retained",
        "payload_conversion_incomplete",
    )
    for table in binding[KEY]["legacy_tables"].values():
        await connection.execute(f"DROP TABLE {storage_relation(policy.schema, table['name'])}")
    current_binding_by_field = {name: contents for name, contents in binding.items() if name != KEY}
    await storage._store(connection, policy, binding["identity"], binding, current_binding_by_field)
    return current_binding_by_field
