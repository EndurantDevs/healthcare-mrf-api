# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing FHIR/CMS call adapters for the explicit publisher snapshot contract."""

import hashlib
import json
from contextlib import aclosing, asynccontextmanager
from dataclasses import replace

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


def named_artifact_input_relation(directory, root, scoped_relation):
    """Allow an explicit input QName only for the ordinary artifact resource table."""
    if (
        type(scoped_relation) is not tuple
        or len(scoped_relation) != 2
        or root != directory.ProviderDirectoryDatasetResource.__tablename__
        or is_source_storage_required()
    ):
        raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_input_relation_invalid")
    return storage_relation(*scoped_relation)


async def lock_endpoint_dataset_candidate_admission(directory, connection, endpoint_id):
    """Serialize candidate intake within its configured storage scope without waiting."""
    from process.provider_directory_generic_source_policy import bound_publisher, current_source_storage_binding
    from process.provider_directory_source_storage_binding import require_storage_schema

    catalog_schema = directory._schema()
    binding = current_source_storage_binding()
    scope = endpoint_id
    if binding is not None:
        binding = require_storage_schema(bound_publisher(), catalog_schema)
        scope = f"provider-directory-source-candidate:{binding.catalog_schema}:{binding.storage_schema}:{endpoint_id}"
    acquired = await connection.scalar(
        "SELECT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(:admission_scope, 0));",
        admission_scope=scope,
    )
    if acquired is not True:
        raise RuntimeError("provider_directory_endpoint_dataset_candidate_admission_busy")
    relation = directory._qt(catalog_schema, directory.ProviderDirectoryAPIEndpoint.__tablename__)
    row_lock = " FOR UPDATE NOWAIT" if binding is None else ""
    return await connection.first(
        f"SELECT endpoint_id FROM {relation} WHERE endpoint_id = :endpoint_id{row_lock};",
        endpoint_id=endpoint_id,
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


@asynccontextmanager
async def _candidate_resource_scope(directory, context, resource_type):
    """Keep aggregate reads in the existing fenced candidate transaction."""
    from process.provider_directory_generic_source_policy import _publisher_scope, _require_active_source_session
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
        yield connection, table, cutoff, binding["ordinal"]
        current = await require_checkpoint_executor(directory.db)
        _require(_publisher_scope(current) == _publisher_scope(publisher), "count_transaction_changed")
        _require_active_source_session(connection)
        async with source_owner_transaction(directory.db) as current:
            _require(_publisher_scope(current) == _publisher_scope(publisher), "count_transaction_changed")


async def candidate_resource_count(directory, context, resource_type, *, distinct=True):
    """Count exact active loading identities, or with distinct=False every active staged row, without an input heap."""
    if not is_source_storage_required():
        return await _legacy_candidate_count(directory, context, resource_type)
    async with _candidate_resource_scope(directory, context, resource_type) as (connection, table, cutoff, ordinal):
        counted = 'DISTINCT resource_id COLLATE "C"' if distinct else "*"
        count = await connection.fetchval(
            f"SELECT count({counted}) FROM {table} "
            "WHERE dataset_id=$1 AND resource_type=$2 AND batch_ordinal>$3 AND batch_ordinal<=$4",
            context.dataset_id,
            resource_type,
            cutoff,
            ordinal,
        )
        _require(type(count) is int and 0 <= count < 2**63, "resource_count_invalid")
        return count


_TIMESTAMP_RECOVERY_STAMP_SQL = """CASE WHEN payload_json #>> '{fhir_meta,lastUpdated}'
          ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+]00:00)$'
          AND pg_catalog.pg_input_is_valid(payload_json #>> '{fhir_meta,lastUpdated}', 'timestamp with time zone')
          THEN (payload_json #>> '{fhir_meta,lastUpdated}')::timestamptz END"""


class _TimestampPrefixIneligible(ValueError):
    """Keep the ordinary cursor when its retained prefix cannot qualify."""


async def candidate_timestamp_recovery_summary(directory, context, resource_type):
    """Qualify retained page ranges and return only the bounded seam_rows payloads."""
    from process.provider_directory_timestamp_recovery import MAX_TIES

    _require(is_source_storage_required(), "timestamp_recovery_named_storage_required")
    async with _candidate_resource_scope(directory, context, resource_type) as (connection, table, cutoff, ordinal):
        if cutoff != 0:
            raise _TimestampPrefixIneligible("timestamp_recovery_reset_not_supported")
        summary_by_field = dict(
            await connection.fetchrow(
                f"""
            WITH observations AS MATERIALIZED (
                SELECT resource_id,batch_ordinal,row_ordinal,
                  {_TIMESTAMP_RECOVERY_STAMP_SQL} AS stamp
                FROM {table} WHERE dataset_id=$1 AND resource_type=$2
                  AND batch_ordinal>$3 AND batch_ordinal<=$4
            ), batches AS (
                SELECT batch_ordinal,min(stamp) AS low,max(stamp) AS high,count(*) AS n,
                  count(stamp)=count(*) AND count(DISTINCT row_ordinal)=count(*)
                    AND min(row_ordinal)=0 AND max(row_ordinal)=count(*)-1 AND count(*)<=20 AS valid
                FROM observations GROUP BY batch_ordinal
            ), ordered AS (
                SELECT *,lag(low) OVER(ORDER BY batch_ordinal) AS previous_low FROM batches
            ), boundary AS (SELECT min(stamp) AS lowest FROM observations)
            SELECT (SELECT count(*) FROM observations) AS observations,
              (SELECT count(DISTINCT resource_id) FROM observations) AS unique_ids,
              (SELECT count(*)-count(stamp) FROM observations) AS invalid_timestamps,
              count(*) AS pages,count(*) FILTER(WHERE NOT valid) AS bad_batches,
              count(*) FILTER(WHERE high>previous_low) AS inversions,
              (SELECT lowest FROM boundary) AS lowest,
              (SELECT min(stamp) FROM observations WHERE stamp>(SELECT lowest FROM boundary)) AS second_lowest,
              (SELECT count(*) FROM observations WHERE stamp=(SELECT lowest FROM boundary)) AS overlap_count
            FROM ordered
            """,
                context.dataset_id,
                resource_type,
                cutoff,
                ordinal,
                timeout=120,
            )
        )
        if summary_by_field["invalid_timestamps"] or not 0 < summary_by_field["overlap_count"] <= MAX_TIES:
            return summary_by_field, []
        seam_rows = await connection.fetch(
            f"SELECT resource_id,payload_json FROM {table} "
            "WHERE dataset_id=$1 AND resource_type=$2 AND batch_ordinal>$3 AND batch_ordinal<=$4 "
            f"AND ({_TIMESTAMP_RECOVERY_STAMP_SQL})=$5 LIMIT $6",
            context.dataset_id,
            resource_type,
            cutoff,
            ordinal,
            summary_by_field["lowest"],
            MAX_TIES + 1,
            timeout=120,
        )
        _require(len(seam_rows) == summary_by_field["overlap_count"], "timestamp_recovery_seam_changed")
        return summary_by_field, [dict(resource_row) for resource_row in seam_rows]


def timestamp_recovery_identity(context, resource_type, start_url_hash):
    """Bind recovery to the unchanged ordinary checkpoint's logical scope."""
    _require(len(context.source_ids) == 1 and context.dataset_id, "timestamp_recovery_scope_invalid")
    return {
        "canonical_api_base": context.canonical_api_base,
        "source_id": context.source_ids[0],
        "dataset_id": context.dataset_id,
        "acquisition_root_run_id": context.acquisition_root_run_id,
        "resource_type": resource_type,
        "start_url_hash": start_url_hash,
        "source_scope_hash": context.source_scope_hash,
    }


def timestamp_recovery_content_hash(directory, payload, resource_type):
    """Compare a replayed boundary's content without its changing transport URLs."""
    return directory.resource_payload_sha256_for_contract(
        payload, directory.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT, resource_type=resource_type
    )


async def _begin_timestamp_recovery(directory, context, resource_type, resume, identity):
    """Qualify every retained page before preparing the first overlapping window."""
    from process import provider_directory_timestamp_recovery as recovery

    summary_by_field, seam_rows = await candidate_timestamp_recovery_summary(directory, context, resource_type)
    if not (
        summary_by_field["observations"] == summary_by_field["unique_ids"] == resume.rows_processed
        and summary_by_field["pages"] == resume.pages_processed
        and summary_by_field["invalid_timestamps"]
        == summary_by_field["bad_batches"]
        == summary_by_field["inversions"]
        == 0
        and summary_by_field["second_lowest"] is not None
        and seam_rows
    ):
        raise _TimestampPrefixIneligible("timestamp_recovery_prefix_invalid")
    return recovery.begin(
        resume.completeness,
        identity=identity,
        original_url=resume.next_url,
        original_pages=resume.pages_processed,
        original_rows=resume.rows_processed,
        upper=summary_by_field["second_lowest"].isoformat(),
        overlap_timestamp=summary_by_field["lowest"].isoformat(),
        overlap_hashes={
            resource_row["resource_id"]: timestamp_recovery_content_hash(
                directory, directory._json_object(resource_row["payload_json"]), resource_type
            )
            for resource_row in seam_rows
        },
    )


def _is_timestamp_recovery_candidate(context, resume):
    """Limit recovery to an unreset, verified retained candidate."""
    return bool(
        is_source_storage_required()
        and context.dataset_id
        and context.lineage_verified
        and context.retry_of_run_id
        and not context.allow_retained_pagination_reset
        and resume.resumed
        and resume.rows_processed
        and not resume.complete
    )


async def prepare_timestamp_recovery(
    directory, context, resource_type, start_url, resume, *, is_failed_continuation=False
):
    """Preserve the original scan and fence a timestamp-only successor inside its existing checkpoint."""
    from process import provider_directory_timestamp_recovery as recovery

    proof = resume.completeness
    has_recovery = recovery.marked(proof)
    can_start = _is_timestamp_recovery_candidate(context, resume)
    if not has_recovery and not (can_start and is_failed_continuation):
        return resume
    identity = timestamp_recovery_identity(context, resource_type, hashlib.sha256(start_url.encode()).hexdigest())
    if has_recovery:
        state = recovery.validate_checkpoint(
            proof,
            identity=identity,
            next_url=resume.next_url,
            pages_processed=resume.pages_processed,
            rows_processed=resume.rows_processed,
            complete=resume.complete,
        )
        if resume.complete:
            _require(
                resume.recent_url_hashes
                and directory._pagination_url_hash(state["terminal_request_url"]) == resume.recent_url_hashes[-1],
                "timestamp_recovery_terminal_request_changed",
            )
            return resume
        _require(can_start, "timestamp_recovery_retained_lineage_required")
        if (
            not is_failed_continuation
            or state["action"] == "fresh_lt"
            or state["remaining_overlap"]
            or state["closed"] is None
        ):
            return resume
    try:
        async with source_transaction(directory.db):
            proof = (
                recovery.reanchor(proof, identity=identity)
                if has_recovery
                else await _begin_timestamp_recovery(directory, context, resource_type, resume, identity)
            )
            next_url = proof[recovery.KEY]["url"]
            await directory._save_pagination_checkpoint(
                context,
                resource_type,
                next_url=next_url,
                pages_processed=resume.pages_processed,
                rows_processed=resume.rows_processed,
                recent_url_hashes=list(resume.recent_url_hashes),
                completeness=proof,
            )
    except _TimestampPrefixIneligible:
        return resume
    return replace(resume, next_url=next_url, completeness=proof)


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


async def source_organization_reference_ids(directory, dataset_id, api_base, limit):
    """Find referenced IDs without a completed two-pass Organization row."""
    from process.provider_directory_generic_source_policy import bound_publisher

    async with source_transaction(directory.db):
        candidate = await resolve_existing_source_candidate(directory.db, directory, dataset_id)
        publisher = bound_publisher()
        connection, policy = publisher.connection, publisher.policy
        binding = await _locked(connection, policy, candidate.identity, phases=("loading",), lock="KEY SHARE")
        table = storage_relation(policy.schema, binding["tables"]["resource_input"]["name"])
        reference_rows = await connection.fetch(
            f"""
            WITH active AS NOT MATERIALIZED (
                SELECT resource_type, resource_id, payload_json FROM {table}
                WHERE dataset_id=$1 AND batch_ordinal<=$2
                  AND batch_ordinal>greatest($3::bigint,coalesce(($4::jsonb->>resource_type)::bigint,0))
            ), refs AS (
                SELECT DISTINCT reference FROM active
                CROSS JOIN LATERAL (VALUES
                    (CASE resource_type
                        WHEN 'OrganizationAffiliation' THEN payload_json->>'organization_ref'
                        WHEN 'PractitionerRole' THEN payload_json->>'organization_ref'
                        WHEN 'Location' THEN payload_json->>'managing_organization_ref'
                        WHEN 'HealthcareService' THEN payload_json->>'provided_by_ref'
                        WHEN 'InsurancePlan' THEN payload_json->>'owned_by_ref'
                    END),
                    (CASE resource_type
                        WHEN 'OrganizationAffiliation' THEN payload_json->>'participating_organization_ref'
                        WHEN 'InsurancePlan' THEN payload_json->>'administered_by_ref'
                    END)
                ) AS found(reference)
                WHERE resource_type IN ('OrganizationAffiliation','PractitionerRole','Location',
                    'HealthcareService','InsurancePlan') AND reference IS NOT NULL
            ), ids AS (
                SELECT (regexp_match(
                    CASE WHEN starts_with(reference,$5 || '/Organization/')
                         THEN substr(reference,length($5)+2) ELSE reference END,
                    '^(?:Organization/)?([A-Za-z0-9.-]{{1,64}})$'))[1]
                    AS resource_id FROM refs
            )
            SELECT resource_id FROM ids WHERE resource_id NOT IN ('.','..')
            EXCEPT SELECT resource_id FROM active WHERE resource_type='Organization'
            ORDER BY resource_id LIMIT $6
            """,
            dataset_id,
            binding["ordinal"],
            binding["resets"].get("*", 0),
            json.dumps(binding["resets"]),
            api_base,
            limit + 1,
        )
    return (
        tuple(reference_row["resource_id"] for reference_row in reference_rows) if len(reference_rows) <= limit else ()
    )


async def missing_saved_role_links(directory, dataset_id, target_type, role_field):
    """Read one committed role-reference projection while role acquisition can keep writing."""
    from process.provider_directory_generic_source_policy import bound_publisher

    _require(
        role_field in directory.LINKED_REFERENCE_FIELDS["PractitionerRole"].get(target_type, ()), "link_scope_changed"
    )
    async with source_transaction(directory.db):
        candidate = await resolve_existing_source_candidate(directory.db, directory, dataset_id)
        _require({"PractitionerRole", target_type} <= set(candidate.selected_resources), "family_scope_changed")
        publisher = bound_publisher()
        connection, policy = publisher.connection, publisher.policy
        binding = await _locked(connection, policy, candidate.identity, lock="KEY SHARE")
        table = storage_relation(policy.schema, binding["tables"]["resource_input"]["name"])
        resets = binding["resets"]
        acquired = connection.cursor(
            f"SELECT DISTINCT resource_id FROM {table} WHERE dataset_id=$1 AND resource_type=$2 "
            "AND batch_ordinal>$3 AND batch_ordinal<=$4",
            dataset_id,
            target_type,
            max(resets.get("*", 0), resets.get(target_type, 0)),
            binding["ordinal"],
            prefetch=4096,
        )
        acquired_ids = {acquired_row["resource_id"] async for acquired_row in acquired}
        references = connection.cursor(
            f"""
            SELECT DISTINCT item.value #>> '{{}}' AS reference FROM {table} r
            CROSS JOIN LATERAL (SELECT (r.payload_json->($2::text))::jsonb AS field) projected
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE jsonb_typeof(projected.field)
                    WHEN 'array' THEN projected.field
                    WHEN 'string' THEN jsonb_build_array(projected.field)
                    ELSE '[]'::jsonb
                END
            ) item(value)
            WHERE r.dataset_id=$1 AND r.resource_type='PractitionerRole'
              AND r.batch_ordinal>$3 AND r.batch_ordinal<=$4 AND jsonb_typeof(item.value)='string'
            """,
            dataset_id,
            role_field,
            max(resets.get("*", 0), resets.get("PractitionerRole", 0)),
            binding["ordinal"],
            prefetch=4096,
        )
        return {
            key
            async for reference_row in references
            if (key := directory._reference_resource_key(reference_row["reference"], target_type))
            and key[1] not in acquired_ids
        }


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


async def current_endpoint_dataset_record(directory, endpoint_id, *, exclude_dataset_id):
    """Read the incumbent header through the established ordinary catalog route."""
    from process.provider_directory_generic_source_policy import _FACTORY
    from process.provider_directory_named_edition import find_current_edition, read_edition_header
    from process.provider_directory_projection_stage import _copy_driver
    from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

    factory = _FACTORY.get()
    binding = getattr(factory, "storage_binding", None)
    if binding is None:
        relation = directory._qt(directory._schema(), directory.ProviderDirectoryEndpointDataset.__tablename__)
    else:
        factory._require_current_policy()
        if directory._schema() != binding.catalog_schema or directory.db._transaction_binding() is not None:
            raise RuntimeError("provider_directory_predecessor_catalog_reader_required")
        # The private header contains candidates, not the incumbent serving generation.
        relation = storage_relation(binding.catalog_schema, directory.ProviderDirectoryEndpointDataset.__tablename__)
    if isinstance(binding, NamedFHIRSourceStorage):
        async with directory.db.transaction() as session:
            connection = await _copy_driver(session)
            pointer = await find_current_edition(connection, binding.catalog_schema, endpoint_id)
            if pointer is not None:
                if pointer["dataset_id"] == exclude_dataset_id:
                    return {}
                header = await read_edition_header(connection, pointer["schema_name"], pointer["dataset_id"])
                _require(header is not None and header["endpoint_id"] == endpoint_id, "current_edition_changed")
                metadata = _json(header["publication_metadata_json"])
                return {
                    "dataset_id": header["dataset_id"],
                    "status": "published",
                    "resource_counts": metadata.get("source_summary_v1", {}).get("resource_counts"),
                    **(
                        {"delta_positive_observation_v1": metadata["delta_positive_observation_v1"]}
                        if "delta_positive_observation_v1" in metadata
                        else {}
                    ),
                }
    predecessor = await directory.db.first(
        f"""
        SELECT dataset_id, status,
               publication_metadata_json -> 'source_summary_v1' -> 'resource_counts' AS resource_counts
          FROM {relation}
         WHERE endpoint_id = :endpoint_id
           AND is_current = true
           AND dataset_id <> :exclude_dataset_id
         LIMIT 1;
        """,
        endpoint_id=endpoint_id,
        exclude_dataset_id=exclude_dataset_id,
    )
    if binding is not None:
        factory._require_current_policy()
    return directory._pagination_checkpoint_row_mapping(predecessor)


def resource_scan_types(directory, source_record, resource_types, options):
    """Include an independent role cursor only for deferred isolated storage."""
    from process.provider_directory_generic_source_policy import _FACTORY

    is_bulk = directory.bulk_parallel.is_bulk_scan(directory, source_record, options)
    supported = directory._source_supported_resource_types(source_record)
    factory = _FACTORY.get()
    include_role = (
        options.deferred_materialization
        and "PractitionerRole" in resource_types
        and directory.is_source_storage_required()
        and getattr(factory, "storage_binding", None) is not None
        and not directory._needs_practitioner_role_reverse_lookup(source_record, "PractitionerRole")
        and (
            directory.bulk_parallel.has_parallel_transport(directory, source_record, ("PractitionerRole",))
            if is_bulk
            else directory._has_parallel_resource_transport(
                source_record, ("PractitionerRole",), options.page_count, minimum_resources=1
            )
        )
    )
    if include_role:
        factory._require_current_policy()
    return tuple(
        resource_type
        for resource_type in directory._source_resource_fetch_order(source_record, list(resource_types))
        if (supported is None or resource_type in supported) and (resource_type != "PractitionerRole" or include_role)
    )


async def resource_scan_estimates(directory, source_record):
    """Prefer published counts to reviewed seed estimates for download ordering."""
    count_by_resource = directory._json_object(
        directory._source_metadata(source_record).get("provider_directory_resource_count_estimates")
    )
    endpoint_id = directory._clean_text(source_record.get("_endpoint_id"))
    if endpoint_id is not None:
        header_by_field = await current_endpoint_dataset_record(
            directory, endpoint_id, exclude_dataset_id=source_record.get("_endpoint_dataset_id") or ""
        )
        if header_by_field.get("status") == directory.ENDPOINT_DATASET_PUBLISHED:
            count_by_resource = directory._json_object(header_by_field.get("resource_counts"))
    return {
        resource: count
        for resource, count in count_by_resource.items()
        if resource in directory.DEFAULT_RESOURCES and type(count) is int and count >= 0
    }


async def lock_validated_candidate_sources(directory, connection, candidate):
    """Validate the declared source set and retain legacy artifact-cutover fencing."""

    from process.provider_directory_generic_source_policy import current_source_storage_binding

    source_ref = directory._qt(directory._schema(), directory.ProviderDirectorySource.__tablename__)
    expected_source_ids = tuple(sorted(set(candidate.source_ids)))
    row_lock = "FOR NO KEY UPDATE OF source" if current_source_storage_binding() is None else ""
    source_rows = await connection.all(
        f"""
        SELECT source.source_id
          FROM {source_ref} AS source
         WHERE source.source_id = ANY(CAST(:source_ids AS varchar[]))
         ORDER BY source.source_id
         {row_lock};
        """,
        source_ids=list(expected_source_ids),
    )
    locked_source_ids = tuple(
        source_id
        for source_row in source_rows
        if (
            source_id := directory._clean_text(
                directory._pagination_checkpoint_row_mapping(source_row).get("source_id")
            )
        )
    )
    if locked_source_ids != expected_source_ids:
        raise RuntimeError("provider_directory_endpoint_dataset_source_changed")


async def locked_endpoint_dataset_candidate_record(directory, connection, candidate):
    """Serialize within the configured storage scope and return the mutable candidate row."""

    endpoint_record = await lock_endpoint_dataset_candidate_admission(directory, connection, candidate.endpoint_id)
    if endpoint_record is None:
        raise RuntimeError("provider_directory_api_endpoint_missing_during_validation")
    dataset_ref = directory._qt(directory._schema(), directory.ProviderDirectoryEndpointDataset.__tablename__)
    candidate_record = await connection.first(
        f"""
        SELECT dataset_id, acquisition_root_run_id, is_current, status,
               previous_dataset_id, completion_proof_required_version,
               completion_proof_json, completion_proof_sha256,
               publication_metadata_json
          FROM {dataset_ref}
         WHERE dataset_id = :dataset_id
           AND endpoint_id = :endpoint_id
         FOR UPDATE;
        """,
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
    )
    if candidate_record is None:
        raise RuntimeError("provider_directory_endpoint_dataset_candidate_missing")
    return directory._pagination_checkpoint_row_mapping(candidate_record)
