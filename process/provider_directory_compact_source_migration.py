# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resume a bounded conversion of an existing named candidate's observations.

The legacy heaps remain authoritative until the header switches atomically.
Progress, compact COPY and its data validation share each existing source
transaction; upstream checkpoints are never changed by this module.
"""

import asyncio
import logging
import re

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
    return await migrate_source_candidate(directory.db, directory, candidate, check_cancelled=cancellation.check)


def validate_conversion(binding):
    """Validate only logical names, original ordering and durable conversion progress."""
    progress_by_field = binding[KEY]
    storage._require(
        binding["contract"] in storage.NAMED_CONTRACTS
        and type(progress_by_field) is dict
        and set(progress_by_field) == _FIELDS,
        "payload_conversion_changed",
    )
    layout.require_source_identity(binding["identity"])
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
            and progress_by_field["rows"] == progress_by_field["total_rows"]
            and all(
                binding["tables"].get(root) == table for root, table in progress_by_field["compact_tables"].items()
            ),
            "payload_conversion_changed",
        )


def _validate_progress(binding, progress_by_field):
    """Require bounded committed coordinates and the original allocation high-water."""
    storage._require(
        progress_by_field["format"] == compact.FORMAT
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
    layout.validate_tables({**binding, "phase": "loading", "tables": progress_by_field["compact_tables"]})
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


async def _begin(connection, policy, previous, directory):
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
    progress_by_field = {
        "format": compact.FORMAT,
        "state": "copying",
        "legacy_tables": previous["tables"],
        "compact_tables": await layout.create_inputs(connection, policy),
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


async def _finish(connection, policy, binding):
    """Switch only the verified candidate pointer; old heaps remain private and retained."""
    from process.provider_directory_compact_source_completion import validate_storage

    progress_by_field = binding[KEY]
    storage._require(progress_by_field["rows"] == progress_by_field["total_rows"], "payload_conversion_incomplete")
    _, sequence = await _sequence(connection, policy.schema, _legacy_relation(policy, binding))
    storage._require(sequence == progress_by_field["sequence"], "payload_conversion_sequence_changed")
    current_binding_by_field = {
        **binding,
        "contract": storage.NAMED_COMPACT_CONTRACT,
        "payload_format": compact.FORMAT,
        "tables": progress_by_field["compact_tables"],
        KEY: {**progress_by_field, "state": "retained"},
    }
    validate_conversion(current_binding_by_field)
    await validate_storage(connection, policy, current_binding_by_field)
    aliases = layout.payload_tables(policy, current_binding_by_field["tables"]).relation("aliases")
    count = await connection.fetchval(f"SELECT count(*) FROM {aliases}")
    storage._require(count == progress_by_field["total_rows"], "payload_conversion_data_changed")
    await connection.fetchval("SELECT setval(pg_get_serial_sequence($1,'batch_ordinal'),$2,$3)", aliases, *sequence)
    await storage._store(connection, policy, binding["identity"], binding, current_binding_by_field)
    return current_binding_by_field


async def migrate_source_candidate(database, directory, candidate, *, check_cancelled=None):
    """Convert only the registered named source through independently committed segments."""
    if candidate is None:
        return None
    identity = storage.candidate_identity(directory, candidate)
    if identity["metadata"].get("source_ids") != [directory._stable_source_id(directory.anthem.seed_rows()[0])]:
        return None
    storage._require(policy_scope._PUBLISHER.get() is None, "payload_conversion_outer_transaction")
    while True:
        if check_cancelled is not None:
            await check_cancelled()
        async with policy_scope.source_transaction(database):
            publisher = policy_scope.bound_publisher()
            if not isinstance(publisher.storage_binding, NamedFHIRSourceStorage):
                return None
            connection, policy = publisher.connection, publisher.policy
            storage._require_candidate_storage(publisher, directory, identity)
            binding = await storage._locked(connection, policy, identity, allow_conversion=True)
            storage._require(binding is not None, "preparation_required")
            layout.require_source_identity(identity)
            if binding["contract"] == storage.NAMED_COMPACT_CONTRACT:
                return binding
            if KEY not in binding:
                binding = await _begin(connection, policy, binding, directory)
            else:
                keys = await _keys(connection, policy, binding)
                binding = (
                    await _copy_segment(connection, policy, binding, keys)
                    if keys
                    else await _finish(connection, policy, binding)
                )
        progress_by_field = binding[KEY]
        LOGGER.info(
            "compact_prefix_conversion rows=%d total=%d state=%s",
            progress_by_field["rows"],
            progress_by_field["total_rows"],
            progress_by_field["state"],
        )


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
