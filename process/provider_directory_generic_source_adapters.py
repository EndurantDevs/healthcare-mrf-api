# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing FHIR/CMS call adapters for the explicit publisher snapshot contract."""

import json
from contextlib import aclosing, asynccontextmanager

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS, GenericCandidateError
from process.provider_directory_generic_source_completion import (
    _reduced_rows,
    _resource_callbacks,
    complete_generic_source_candidate,
)
from process.provider_directory_generic_source_policy import (
    is_source_storage_required,
    source_owner_transaction,
    source_transaction,
    source_transaction_available,
)
from process.provider_directory_generic_source_storage import (
    HEADER,
    RELATIONS,
    _json,
    _lock_resource_family,
    _locked,
    _require,
    _resource_family,
    _verify_tables,
    append_generic_source_batch,
    generic_source_relation_scope,
    resolve_existing_source_candidate,
    resolve_generic_source_candidate,
)


def require_source_transaction(*, repeatable_read=False):
    """Refuse before target IO; isolation is established by the trusted broker."""
    publisher = source_transaction_available()
    _require(
        not repeatable_read
        or publisher is None
        or publisher.transaction_isolation in {"repeatable read", "serializable"},
        "repeatable_read_required",
    )
    return publisher


async def persist_source_rows(directory, rows, *, resource_hash_contract, semantic_projection_as_of):
    """One bounded native COPY per dataset batch, never per-resource SQL."""
    require_source_transaction()
    rows_by_dataset = {}
    for row in rows:
        rows_by_dataset.setdefault(row["dataset_id"], []).append(row)
    async with source_transaction(directory.db):
        await _persist_dataset_batches(directory, rows_by_dataset, resource_hash_contract, semantic_projection_as_of)


async def _persist_dataset_batches(directory, rows_by_dataset, resource_hash_contract, semantic_projection_as_of):
    for dataset_id, batch in sorted(rows_by_dataset.items()):
        if resource_hash_contract is None:
            _require(all(row["resource_type"].startswith("LU:") for row in batch), "hash_contract_required")
            candidate = await resolve_existing_source_candidate(directory.db, directory, dataset_id)
        else:
            candidate = await resolve_generic_source_candidate(
                directory.db,
                directory,
                dataset_id,
                resource_hash_contract=resource_hash_contract,
                semantic_projection_as_of=semantic_projection_as_of,
            )
        for offset in range(0, len(batch), 4096):
            await append_generic_source_batch(directory.db, directory, candidate, batch[offset : offset + 4096])


async def _legacy_candidate_count(directory, context, resource_type):
    count_row = await directory.db.first(
        f"""
        SELECT COUNT(*) AS unique_candidate_rows
          FROM {directory._qt(directory._schema(), "provider_directory_dataset_resource")}
         WHERE dataset_id = :dataset_id
           AND resource_type = :resource_type;
        """,
        dataset_id=context.dataset_id,
        resource_type=resource_type,
    )
    count_map = directory._pagination_checkpoint_row_mapping(count_row)
    return int(count_map.get("unique_candidate_rows") or 0)


async def candidate_resource_count(directory, context, resource_type):
    """Count exact active loading identities without exposing or preparing an input heap."""
    if not is_source_storage_required():
        return await _legacy_candidate_count(directory, context, resource_type)
    from process.provider_directory_source_checkpoint_control import require_checkpoint_executor

    async with source_transaction(directory.db):
        publisher = await require_checkpoint_executor(directory.db)
        candidate = await resolve_existing_source_candidate(directory.db, directory, context.dataset_id)
        identity = candidate.identity
        _require(
            identity["endpoint_id"] == context.endpoint_id
            and identity["acquisition_root_run_id"] == context.acquisition_root_run_id
            and identity["metadata"].get("source_ids") == list(context.source_ids)
            and type(resource_type) is str
            and resource_type in candidate.selected_resources,
            "count_scope_changed",
        )
        connection, policy = publisher.connection, publisher.policy
        await _locked(connection, policy, identity, phases=("loading",), lock="KEY SHARE")
        await _lock_resource_family(connection, policy, context.dataset_id, _resource_family(resource_type))
        binding = await _locked(connection, policy, identity, phases=("loading",), lock="KEY SHARE")
        await _verify_tables(connection, policy, binding)
        cutoff = max(binding["resets"].get("*", 0), binding["resets"].get(resource_type, 0))
        table = storage_relation(policy.schema, binding["tables"]["resource_input"]["name"])
        count = await connection.fetchval(
            f'SELECT count(DISTINCT resource_id COLLATE "C") FROM {table} '
            "WHERE dataset_id=$1 AND resource_type=$2 AND batch_ordinal>$3 AND batch_ordinal<=$4",
            context.dataset_id,
            resource_type,
            cutoff,
            binding["ordinal"],
        )
        _require(type(count) is int and 0 <= count < 2**63, "resource_count_invalid")
        _require(await require_checkpoint_executor(directory.db) is publisher, "count_transaction_changed")
        async with source_owner_transaction(directory.db) as current:
            _require(current is publisher, "count_transaction_changed")
        return count


async def source_retry_rows(directory, dataset_id, resource_type, *, payload_hash=None, resource_ids=None):
    """Set-based replay over the isolated input; no live/history resource writes."""
    _require(type(resource_type) is str and resource_type.startswith("LU:"), "retry_type_invalid")
    from process.provider_directory_generic_source_policy import bound_publisher

    async with source_transaction(directory.db):
        candidate = await resolve_existing_source_candidate(directory.db, directory, dataset_id)
        publisher = bound_publisher()
        connection, policy = publisher.connection, publisher.policy
        await _locked(connection, policy, candidate.identity, phases=("loading", "prepared"), lock="KEY SHARE")
        await _lock_resource_family(connection, policy, dataset_id, _resource_family(resource_type))
        binding = await _locked(
            connection, policy, candidate.identity, phases=("loading", "prepared"), lock="KEY SHARE"
        )
        await _verify_tables(connection, policy, binding)
        resets = binding["resets"]
        cutoff = max(resets.get("*", 0), resets.get(resource_type, 0), resets.get(resource_type.split(":")[1], 0))
        table = storage_relation(policy.schema, binding["tables"]["resource_input"]["name"])
        retry_rows = await connection.fetch(
            f'SELECT resource_id,payload_json FROM (SELECT DISTINCT ON (resource_id COLLATE "C") '
            f"resource_id,payload_json,payload_hash FROM {table} WHERE resource_type=$1 AND batch_ordinal>$2 "
            "AND batch_ordinal<=$5 "
            'ORDER BY resource_id COLLATE "C",batch_ordinal DESC,row_ordinal DESC) latest '
            "WHERE ($3::text IS NULL OR payload_hash=$3) AND ($4::text[] IS NULL OR resource_id=ANY($4)) "
            'ORDER BY resource_id COLLATE "C"',
            resource_type,
            cutoff,
            payload_hash,
            resource_ids,
            binding["ordinal"],
        )
        return [
            {"resource_id": retry_row["resource_id"], "payload_json": _json(retry_row["payload_json"])}
            for retry_row in retry_rows
        ]


def _family_callbacks(directory, candidate):
    prepare_content, merge_content = _resource_callbacks(directory, candidate)

    def prepare(resource_row):
        """Keep window marker hashes distinct from canonical content hashes."""
        if resource_row["resource_type"].startswith("LU:"):
            return {**resource_row, "payload_json": _json(resource_row["payload_json"])}
        return prepare_content(resource_row)

    def merge(previous, incoming):
        """Marker replay is latest observation; content retains the existing semantic union."""
        return incoming if incoming["resource_type"].startswith("LU:") else merge_content(previous, incoming)

    return prepare, merge


async def _family_observations(connection, policy, binding, resource_types):
    table = storage_relation(policy.schema, binding["tables"]["resource_input"]["name"])
    family = _resource_family(resource_types[0])
    query = (
        f"SELECT {','.join(storage_identifier(name) for name in RESOURCE_COLUMNS)} FROM {table} "
        "WHERE dataset_id=$1 AND resource_type=ANY($2::text[]) AND batch_ordinal<=$5 "
        "AND batch_ordinal>greatest($3::bigint,coalesce(($4::jsonb->>resource_type)::bigint,0)) "
        'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal'
    )
    resets = binding["resets"]
    async for resource_row in connection.cursor(
        query,
        binding["identity"]["dataset_id"],
        list(resource_types),
        max(resets.get("*", 0), resets.get(family, 0)),
        json.dumps(resets),
        binding["ordinal"],
        prefetch=4096,
    ):
        yield dict(resource_row)


async def _source_family_rows(directory, dataset_id, resource_types):
    """Verify one acquired family and its markers in one ordered private-input scan."""
    _require(
        type(resource_types) is tuple
        and bool(resource_types)
        and all(type(resource_type) is str and resource_type for resource_type in resource_types)
        and len({_resource_family(resource_type) for resource_type in resource_types}) == 1,
        "family_scope_changed",
    )
    async with source_transaction(directory.db):
        candidate = await resolve_existing_source_candidate(directory.db, directory, dataset_id)
        _require(
            _resource_family(resource_types[0])
            in set(candidate.selected_resources) | set(candidate.identity["metadata"].get("proof_resource_scope", ())),
            "family_scope_changed",
        )
        async with source_owner_transaction(directory.db) as publisher:
            connection, policy = publisher.connection, publisher.policy
            await _locked(connection, policy, candidate.identity, phases=("loading", "prepared"), lock="KEY SHARE")
            await _lock_resource_family(connection, policy, dataset_id, _resource_family(resource_types[0]))
            binding = await _locked(
                connection, policy, candidate.identity, phases=("loading", "prepared"), lock="KEY SHARE"
            )
            await _verify_tables(connection, policy, binding)
            prepare, merge = _family_callbacks(directory, candidate)
            observations = _family_observations(connection, policy, binding, resource_types)
            async for resource_row in _reduced_rows(observations, dataset_id=dataset_id, prepare=prepare, merge=merge):
                yield resource_row


async def source_family_rows(directory, dataset_id, resource_types):
    """Retain the existing v4 full-family proof interface after bounded reduction."""
    async with aclosing(_source_family_rows(directory, dataset_id, resource_types)) as projected_rows:
        return [resource_row async for resource_row in projected_rows]


async def source_partition_rows(directory, dataset_id, resource_type):
    """Project acquired content and exact twin-pass markers without closing intake."""
    types = (
        resource_type,
        directory._last_updated_partition_fingerprint_resource_type(resource_type, 1),
        directory._last_updated_partition_fingerprint_resource_type(resource_type, 2),
    )
    try:
        return await source_family_rows(directory, dataset_id, types)
    except GenericCandidateError as error:
        raise RuntimeError("provider_directory_last_updated_partition_staged_dataset_mismatch") from error


def _partition_marker_hash(resource_row):
    """Read the committed content digest without accepting a malformed marker payload."""
    payload = resource_row["payload_json"]
    return payload.get("candidate_payload_hash") if type(payload) is dict else None


async def source_partition_counts(directory, dataset_id, resource_type):
    """Stream historical proof checks while retaining only identity/hash comparisons."""
    types = (
        resource_type,
        directory._last_updated_partition_fingerprint_resource_type(resource_type, 1),
        directory._last_updated_partition_fingerprint_resource_type(resource_type, 2),
    )
    hashes_by_id, proof_hashes_by_id, pass1_count = {}, {}, 0
    try:
        async with aclosing(_source_family_rows(directory, dataset_id, types)) as projected_rows:
            async for resource_row in projected_rows:
                if resource_row["resource_type"] == resource_type:
                    hashes_by_id[resource_row["resource_id"]] = resource_row["payload_hash"]
                    continue
                if resource_row["resource_type"] == types[1]:
                    pass1_count += 1
                    continue
                proof_hashes_by_id[resource_row["resource_id"]] = _partition_marker_hash(resource_row)
    except GenericCandidateError as error:
        raise RuntimeError("provider_directory_last_updated_partition_staged_dataset_mismatch") from error
    return dict(
        candidate_count=len(hashes_by_id),
        pass1_count=pass1_count,
        pass2_count=len(proof_hashes_by_id),
        invalid_candidate_count=sum(
            proof_hashes_by_id.get(resource_id) != digest for resource_id, digest in hashes_by_id.items()
        ),
        orphan_proof_count=len(set(proof_hashes_by_id) - set(hashes_by_id)),
    )


async def retain_partition_references(directory, runner, resource_type, source_record, fetch_result):
    """Preserve requested linked/reverse seeds without invoking the deferred writer callback."""
    if (
        not is_source_storage_required()
        or not runner.is_streaming
        or not fetch_result.complete
        or fetch_result.error
        or fetch_result.fetch_mode != directory.LAST_UPDATED_PARTITION_FETCH_MODE
    ):
        return
    needed = runner.options.linked_resource_limit > 0 or (
        runner.options.scan_role_reverse_lookup_planned
        and resource_type in directory._practitioner_role_reverse_lookup_resources(runner.source_record)
    )
    if not needed:
        return
    context = source_record.get("_pagination_checkpoint_context")
    _require(type(context) is directory.PaginationCheckpointContext and bool(context.dataset_id), "context_required")
    references = runner.rows_by_resource.setdefault(resource_type, [])
    async with aclosing(_source_family_rows(directory, context.dataset_id, (resource_type,))) as projected_rows:
        async for resource_row in projected_rows:
            references.extend(directory._compact_linked_reference_rows(resource_type, [resource_row["payload_json"]]))


async def require_generic_source_layout(directory):
    """Startup/import helpers may inspect managed layout but never recreate it."""
    require_source_transaction()
    from process.provider_directory_generic_source_policy import _bind_isolated_storage, bound_publisher
    from process.provider_directory_source_storage_binding import require_storage_schema

    async with source_transaction(directory.db):
        publisher = bound_publisher()
        connection, policy = publisher.connection, publisher.policy
        if publisher.storage_binding is not None:
            require_storage_schema(publisher, directory._schema())
            await _bind_isolated_storage(publisher)
            return publisher.storage_binding
        _require(policy.schema == directory._schema(), "schema_changed")
        roots = [storage_relation(policy.schema, name) for name in RELATIONS]
        count = await connection.fetchval(
            "SELECT count(*) FROM pg_class c WHERE c.oid=ANY(ARRAY(SELECT to_regclass(name)::oid "
            "FROM unnest($1::text[]) n(name))) AND c.relkind='p'",
            roots,
        )
        present = await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass($1) "
            "AND attname='candidate_storage_json' AND atttypid='jsonb'::regtype AND NOT attisdropped)",
            storage_relation(policy.schema, HEADER),
        )
        legacy = await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) "
            "AND tgname=ANY(ARRAY['pd_dataset_storage','pd_dataset_generic_finalization']) AND NOT tgisinternal)",
            storage_relation(policy.schema, HEADER),
        )
        _require(
            policy.schema == directory._schema() and count == len(roots) and present is True and legacy is False,
            "administrator_cut_required",
        )


@asynccontextmanager
async def completed_source_scope(directory, candidate):
    """Existing release/relationship validators see one completed private snapshot."""
    require_source_transaction(repeatable_read=True)
    async with source_transaction(directory.db, isolation="repeatable read"):
        await complete_generic_source_candidate(directory.db, directory, candidate)
        async with generic_source_relation_scope(directory.db, directory, candidate) as state:
            yield state
