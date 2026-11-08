# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finish isolated SOURCE keys/indexes and attach only a validated closed group."""

import hashlib
import json
from types import SimpleNamespace

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_candidate import (
    COPY_FRAME_BYTES,
    COPY_MAX_BYTES,
    COPY_MAX_ROWS,
    RESOURCE_COLUMNS,
    WITNESS_COLUMNS,
    copy_record_bytes,
    reduce_sorted_resources,
    reduce_sorted_witnesses,
)
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_copy_session,
    source_owner_transaction,
)
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff

_KEYS = {
    storage.RESOURCE: ("dataset_id", "resource_type", "resource_id"),
    storage.RELATIONS[1]: ("dataset_id", "resource_id"),
    storage.RELATIONS[2]: ("dataset_id", "network_resource_id", "insurance_plan_resource_id"),
    storage.RELATIONS[3]: ("dataset_id", "participating_organization_resource_id", "affiliation_resource_id"),
    storage.WITNESS: ("dataset_id", "resource_type", "resource_id"),
    storage.RELATIONSHIP: (
        "dataset_id",
        "resource_type",
        "resource_id",
        "reference_field",
        "parent_ordinal",
        "reference_ordinal",
    ),
}
_INDEXES = {
    storage.RESOURCE: (
        '(dataset_id,resource_type COLLATE "C",resource_id COLLATE "C")',
        "(resource_type,resource_id)",
        "(dataset_id,resource_id) WHERE resource_type='InsurancePlan'",
        "(payload_hash)",
        "((payload_json::jsonb->>'npi'),dataset_id,resource_type) "
        "WHERE resource_type IN ('Practitioner','PractitionerRole')",
    ),
    storage.RELATIONS[1]: ("(dataset_id,resource_id) INCLUDE (plan_identifier) WHERE plan_active",),
    storage.RELATIONS[2]: ("(dataset_id,insurance_plan_resource_id) INCLUDE (network_resource_id)",),
    storage.RELATIONSHIP: ("(dataset_id,target_type,target_resource_id)",),
}


def _payload(row):
    value = storage._json(row["payload_json"])
    storage._require(type(value) is dict, "payload_invalid")
    return value


def _resource_callbacks(directory, candidate):
    contract = candidate.resource_hash_contract
    is_semantic = directory.is_semantic_resource_hash_contract(contract)

    def prepare(resource_by_field):
        """Validate the existing canonical projection/hash/date before reduction."""
        resource_by_field = {name: resource_by_field.get(name) for name in RESOURCE_COLUMNS}
        resource_by_field["payload_json"] = _payload(resource_by_field)
        expected = directory.resource_payload_sha256_for_contract(
            resource_by_field["payload_json"], contract, resource_type=resource_by_field["resource_type"]
        )
        storage._require(resource_by_field["payload_hash"] == expected, "payload_hash_changed")
        if is_semantic:
            directory._validated_semantic_dataset_payload(
                resource_by_field, resource_by_field["resource_type"], contract
            )
            if resource_by_field["resource_type"] == "Practitioner":
                directory._assert_practitioner_semantic_projection_as_of(
                    directory.ProviderDirectoryPractitioner,
                    [resource_by_field["payload_json"]],
                    contract,
                    candidate.semantic_projection_as_of,
                )
        return resource_by_field

    def merge(previous, incoming):
        """Apply the existing pure family union or ordered last-observation rule."""
        if not is_semantic:
            return incoming
        merged = directory._merged_semantic_dataset_payload(
            incoming["resource_type"],
            previous["payload_json"],
            incoming["payload_json"],
            contract,
        )
        return {
            **incoming,
            "payload_json": merged,
            "payload_hash": directory.resource_payload_sha256_for_contract(
                merged, contract, resource_type=incoming["resource_type"]
            ),
        }

    return prepare, merge


def _witness(row):
    witness_by_field = {name: row[name] for name in WITNESS_COLUMNS}
    witness_by_field["raw_payload_json"] = storage._json(witness_by_field["raw_payload_json"])
    raw = witness_by_field["raw_payload_json"]
    storage._require(
        type(raw) is dict
        and raw.get("id") == witness_by_field["resource_id"]
        and raw.get("resourceType") == witness_by_field["resource_type"],
        "raw_identity_changed",
    )
    digest = hashlib.sha256(storage._encoded(raw).encode()).hexdigest()
    storage._require(witness_by_field["raw_payload_sha256"] == digest, "raw_hash_changed")
    return witness_by_field


def _active_where(key):
    """Select observations after the locked reset cutoffs; one predicate for every reader."""
    content_filter = "AND resource_type NOT LIKE 'LU:%:pass:%' " if key == "resource_input" else ""
    return "WHERE batch_ordinal>greatest($1::bigint,coalesce(($2::jsonb->>resource_type)::bigint,0)) " + content_filter


def _active_arguments(binding_by_field):
    resets = binding_by_field["resets"]
    return resets.get("*", 0), storage._encoded(resets)


async def _active_rows(connection, policy, binding_by_field, key, columns):
    table = storage_relation(policy.schema, binding_by_field["tables"][key]["name"])
    # Reset facts are fixed by the locked header. One indexed ordered scan replaces
    # every historical per-key SELECT/FOR UPDATE and every per-row retry DELETE.
    query = (
        f"SELECT {','.join(storage_identifier(name) for name in columns)} FROM {table} "
        + _active_where(key)
        + 'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal'
    )
    async for row in connection.cursor(query, *_active_arguments(binding_by_field), prefetch=4096):
        yield dict(row)


# Only identical payloads may take the last-observation shortcut: the contract hash covers the
# semantic subset alone, and the ordered semantic union decides between differing provenance.
_DIVERGENCE_FIELDS = {
    "resource_input": "payload_hash,payload_json::jsonb,coalesce(acquired_resource_sha256,'')",
    "witness_input": "source_id,release_id,raw_payload_sha256,normalized_payload_hash",
}
_SET_REDUCTION_WORK_MEM = "256MB"
_RECHECK_ROWS = 10_000


async def _observed_divergence(connection, policy, binding_by_field, key):
    """Report whether any complete key observed differing payload facts."""
    table = storage_relation(policy.schema, binding_by_field["tables"][key]["name"])
    return await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {table} {_active_where(key)}"
        f"GROUP BY resource_type,resource_id HAVING count(DISTINCT ({_DIVERGENCE_FIELDS[key]}))>1)",
        *_active_arguments(binding_by_field),
    )


async def _reduce_input_sql(connection, policy, binding_by_field, key, target, columns):
    """Keep the last observation of every complete key with one set-based statement.

    Identical duplicates make the pure semantic union a no-op, so this equals the
    ordered Python reduction whenever no key observed differing payload facts.
    """
    table = storage_relation(policy.schema, binding_by_field["tables"][key]["name"])
    fields = ",".join(storage_identifier(name) for name in columns)
    status = await connection.execute(
        f"INSERT INTO {storage_relation(policy.schema, target)} ({fields}) "
        f'SELECT DISTINCT ON (resource_type COLLATE "C",resource_id COLLATE "C") {fields} FROM {table} '
        + _active_where(key)
        + 'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal DESC,row_ordinal DESC',
        *_active_arguments(binding_by_field),
    )
    storage._require(status.startswith("INSERT 0 "), "copy_incomplete")
    return int(status.rsplit(" ", 1)[1])


async def _every_row(connection, policy, name, columns, validate):
    """Re-run a per-row validator on every row; raw witness hashes have no later complete check."""
    relation = storage_relation(policy.schema, name)
    fields = ",".join(storage_identifier(column) for column in columns)
    async for row in connection.cursor(f"SELECT {fields} FROM {relation}", prefetch=1024):
        validate(dict(row))


async def _recheck_reduced_rows(connection, policy, name, columns, validate, count):
    """Re-run the existing per-row validators on every row of a small heap or a bounded sample."""
    if count <= _RECHECK_ROWS:
        await _every_row(connection, policy, name, columns, validate)
        return
    relation = storage_relation(policy.schema, name)
    fields = ",".join(storage_identifier(column) for column in columns)
    # SYSTEM sampling is page-clustered and the heap is key-ordered: shuffle the sample before LIMIT.
    percent = min(100.0, max(0.01, 300.0 * _RECHECK_ROWS / count))
    query = (
        f"SELECT {fields} FROM (SELECT {fields} FROM {relation} TABLESAMPLE SYSTEM ({percent:.4f})) sampled "
        f"ORDER BY random() LIMIT {_RECHECK_ROWS}"
    )
    async for row in connection.cursor(query, prefetch=1024):
        validate(dict(row))


async def _reduce_payloads_sql(connection, policy, previous, candidate, directory, tables_by_root, release_id):
    """Set-based reduction of both input heaps with the same raw-identity guards."""
    prepare, _ = _resource_callbacks(directory, candidate)
    # Restored below: SET LOCAL would otherwise outlive the reductions for the whole validation transaction.
    previous_work_mem = await connection.fetchval("SHOW work_mem")
    await connection.execute(f"SET LOCAL work_mem='{_SET_REDUCTION_WORK_MEM}'")
    resource_name = tables_by_root[storage.RESOURCE]["name"]
    resource_count = await _reduce_input_sql(
        connection, policy, previous, "resource_input", resource_name, RESOURCE_COLUMNS
    )
    await _recheck_reduced_rows(connection, policy, resource_name, RESOURCE_COLUMNS, prepare, resource_count)
    witness_name = tables_by_root[storage.WITNESS]["name"]
    witness_count = await _reduce_input_sql(
        connection, policy, previous, "witness_input", witness_name, WITNESS_COLUMNS
    )
    if witness_count:
        storage._require(release_id is not None, "unexpected_witness")
        witness = storage_relation(policy.schema, witness_name)
        invalid = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {witness} WHERE source_id<>'cms-npd' OR release_id<>$1 "
            "OR raw_payload_json->>'id' IS DISTINCT FROM resource_id "
            "OR raw_payload_json->>'resourceType' IS DISTINCT FROM resource_type)",
            release_id,
        )
        storage._require(invalid is False, "raw_identity_changed")
        await _every_row(connection, policy, witness_name, WITNESS_COLUMNS, _witness)
    await connection.execute(f"SET LOCAL work_mem='{previous_work_mem}'")


async def _reduced_rows(rows, *, dataset_id, prepare, merge):
    previous_key, accumulated = None, None
    async for row in rows:
        key = (row["resource_type"], row["resource_id"])
        storage._require(previous_key is None or key >= previous_key, "resource_order_changed")
        # Reuse the same complete-key identity/hash and pure semantic contracts.
        if key == previous_key:
            accumulated = next(
                reduce_sorted_resources((accumulated, row), dataset_id, prepare_row=prepare, merge_rows=merge)
            )
        else:
            if accumulated is not None:
                yield accumulated
            accumulated = next(reduce_sorted_resources((row,), dataset_id, prepare_row=prepare, merge_rows=merge))
            previous_key = key
    if accumulated is not None:
        yield accumulated


async def _reduced_witnesses(rows, dataset_id, release_id):
    previous_key, accumulated = None, None
    async for raw in rows:
        row = _witness(raw)
        key = (row["resource_type"], row["resource_id"])
        if key == previous_key:
            accumulated = next(reduce_sorted_witnesses((accumulated, row), dataset_id, release_id))
        else:
            storage._require(previous_key is None or key > previous_key, "witness_order_changed")
            if accumulated is not None:
                yield accumulated
            accumulated = next(reduce_sorted_witnesses((row,), dataset_id, release_id))
            previous_key = key
    if accumulated is not None:
        yield accumulated


async def _copy_rows(connection, policy, name, columns, rows):
    # A complete key may span any number of input cursor pages. The output COPY
    # buffer stays bounded by both encoded bytes and row count independently.
    buffer, count, buffered_bytes = [], 0, COPY_FRAME_BYTES
    async for row in rows:
        copy_values = tuple(
            storage._encoded(row[field]) if field.endswith("_json") else row.get(field) for field in columns
        )
        size = copy_record_bytes(copy_values)
        if buffer and (len(buffer) == COPY_MAX_ROWS or buffered_bytes + size > COPY_MAX_BYTES):
            count += await storage._copy(connection, policy, name, columns, buffer)
            buffer, buffered_bytes = [], COPY_FRAME_BYTES
        buffer.append(copy_values)
        buffered_bytes += size
    if buffer:
        count += await storage._copy(connection, policy, name, columns, buffer)
    return count


async def _indexes(connection, policy, root, name):
    relation = storage_relation(policy.schema, name)
    columns = ",".join(storage_identifier(column) for column in _KEYS[root])
    await connection.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY ({columns})")
    for index, definition in enumerate(_INDEXES.get(root, ())):
        await connection.execute(
            f"CREATE INDEX {storage_identifier(name + '_i' + str(index))} ON {relation} {definition}"
        )
    await connection.execute(f"ANALYZE {relation}")


async def _order_indexes(connection, policy, binding_by_field):
    """Index both inputs for the ordered cursor reduction only when it runs."""
    for key in ("resource_input", "witness_input"):
        name = binding_by_field["tables"][key]["name"]
        await connection.execute(
            f"CREATE INDEX {storage_identifier(name + '_order')} ON {storage_relation(policy.schema, name)} "
            '(resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal)'
        )


async def _reduce_payloads_python(connection, policy, previous, candidate, directory, tables_by_root, release_id):
    """Ordered cursor reduction with the pure semantic union for differing observations."""
    prepare, merge = _resource_callbacks(directory, candidate)
    await _order_indexes(connection, policy, previous)
    resources = _reduced_rows(
        _active_rows(connection, policy, previous, "resource_input", RESOURCE_COLUMNS),
        dataset_id=candidate.dataset_id,
        prepare=prepare,
        merge=merge,
    )
    await _copy_rows(connection, policy, tables_by_root[storage.RESOURCE]["name"], RESOURCE_COLUMNS, resources)
    witnesses = _active_rows(connection, policy, previous, "witness_input", WITNESS_COLUMNS)
    if release_id is not None:
        await _copy_rows(
            connection,
            policy,
            tables_by_root[storage.WITNESS]["name"],
            WITNESS_COLUMNS,
            _reduced_witnesses(witnesses, candidate.dataset_id, release_id),
        )
    else:
        async for _ in witnesses:
            raise storage.GenericCandidateError("provider_directory_generic_source_unexpected_witness")


async def _complete_payloads(connection, policy, previous, candidate, directory, tables_by_root):
    """Reduce every complete payload key before finishing its native indexes.

    Keys that never observed differing payload facts reduce with one set-based
    statement per heap; witness conflicts stay a hard error, and resources with
    differing observations fall back to the ordered semantic union.
    """
    release_id = (candidate.source_release or {}).get("vector_sha256")
    await storage.refresh_statistics(
        connection, policy.schema, [previous["tables"][key]["name"] for key in ("resource_input", "witness_input")]
    )
    if await _observed_divergence(connection, policy, previous, "witness_input"):
        raise storage.GenericCandidateError("provider_directory_generic_candidate_witness_replay_conflict")
    if await _observed_divergence(connection, policy, previous, "resource_input"):
        await _reduce_payloads_python(connection, policy, previous, candidate, directory, tables_by_root, release_id)
    else:
        await _reduce_payloads_sql(connection, policy, previous, candidate, directory, tables_by_root, release_id)
    for root in (storage.RESOURCE, storage.WITNESS):
        await _indexes(connection, policy, root, tables_by_root[root]["name"])


async def complete_generic_source_candidate(database, directory, candidate):
    """Close intake, reduce all observations, finish resource/witness indexes.

    Derived heaps remain private and empty for the existing set-based serving
    builders. No attached history/index is rebuilt. A failed/canceled savepoint
    leaves the exact prior loading binding and all committed input COPY batches.
    """
    identity = storage.candidate_identity(directory, candidate)
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        storage._require_candidate_storage(publisher, directory, identity)
        previous = await storage._locked(connection, policy, identity, phases=("loading", "prepared"))
        storage._require(previous is not None, "preparation_required")
        await storage._verify_tables(connection, policy, previous)
        if previous["phase"] == "prepared":
            return previous
        tables_by_root = dict(previous["tables"])
        prefix = tables_by_root["resource_input"]["name"][:-3]
        for root in storage.RELATIONS:
            name = prefix + "_" + storage._SUFFIXES[root]
            await connection.execute(
                f"CREATE TABLE {storage_relation(policy.schema, name)} (LIKE {storage_relation(policy.schema, root)} "
                "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
                + storage.PRIVATE_HEAP_OPTIONS
            )
            await storage._close_grants(connection, policy, name)
            tables_by_root[root] = {"name": name}
        await _complete_payloads(connection, policy, previous, candidate, directory, tables_by_root)
        from process.provider_directory_generic_source_auxiliary import prepare_identity_candidate

        auxiliary = (
            None
            if previous["contract"] == storage.CMS_ISOLATED_CONTRACT
            else await prepare_identity_candidate(publisher, identity, prefix)
        )
        captured = await storage._capture(connection, policy, [table["name"] for table in tables_by_root.values()])
        binding_by_field = {
            **previous,
            "phase": "prepared",
            "auxiliary": auxiliary,
            "tables": {key: captured[table["name"]] for key, table in tables_by_root.items()},
        }
        await storage._store(connection, policy, identity, previous, binding_by_field)
        return binding_by_field


async def _validate_acquired_digest_markers(connection, policy, binding_by_field):
    """Preserve the subset acquisition marker contract with one closed-snapshot check."""
    resource = storage_relation(policy.schema, binding_by_field["tables"][storage.RESOURCE]["name"])
    header_name = binding_by_field["tables"].get(storage.HEADER, {}).get("name", storage.HEADER)
    header = storage_relation(policy.schema, header_name)
    mismatch = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {resource} r LEFT JOIN {header} h ON h.dataset_id=$1 "
        "WHERE r.dataset_id<>$1 OR h.dataset_id IS NULL OR (((h.completion_proof_required_version=3) IS TRUE) "
        "IS DISTINCT FROM (r.acquired_resource_sha256 IS NOT NULL)))",
        binding_by_field["identity"]["dataset_id"],
    )
    storage._require(mismatch is False, "acquired_digest_marker_changed")


async def _witness_relationships(connection, policy, binding_by_field):
    """Check complete candidate witness identity after its finished indexes."""
    relation = lambda root: storage_relation(policy.schema, binding_by_field["tables"][root]["name"])
    await _validate_acquired_digest_markers(connection, policy, binding_by_field)
    resource, witness = relation(storage.RESOURCE), relation(storage.WITNESS)
    missing = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {witness} w WHERE NOT EXISTS(SELECT FROM {resource} r "
        "WHERE (r.dataset_id,r.resource_type,r.resource_id,r.payload_hash)="
        "(w.dataset_id,w.resource_type,w.resource_id,w.normalized_payload_hash)))"
    )
    storage._require(missing is False, "witness_relationship_changed")
    release_id = binding_by_field["identity"]["metadata"].get("source_release", {}).get("vector_sha256")
    if release_id is not None:
        missing = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {resource} r WHERE NOT EXISTS(SELECT FROM {witness} w "
            "WHERE (r.dataset_id,r.resource_type,r.resource_id,r.payload_hash)="
            "(w.dataset_id,w.resource_type,w.resource_id,w.normalized_payload_hash) "
            "AND w.source_id='cms-npd' AND w.release_id=$1))",
            release_id,
        )
        storage._require(missing is False, "resource_witness_missing")
    return release_id


async def _relationships(connection, policy, binding_by_field):
    """Check full dataset, release, source and payload relationships as aggregates."""
    relation = lambda root: storage_relation(policy.schema, binding_by_field["tables"][root]["name"])
    dataset_id = binding_by_field["identity"]["dataset_id"]
    resource, witness = relation(storage.RESOURCE), relation(storage.WITNESS)
    release_id = await _witness_relationships(connection, policy, binding_by_field)
    missing = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {relation(storage.RELATIONSHIP)} edge "
        f"LEFT JOIN {resource} r ON (r.dataset_id,r.resource_type,r.resource_id,r.payload_hash)="
        "(edge.dataset_id,edge.resource_type,edge.resource_id,edge.source_payload_hash) "
        f"LEFT JOIN {witness} w ON (w.dataset_id,w.resource_type,w.resource_id,w.raw_payload_sha256)="
        "(edge.dataset_id,edge.resource_type,edge.resource_id,edge.raw_payload_sha256) "
        "WHERE edge.dataset_id<>$1 OR edge.source_id<>'cms-npd' OR edge.release_id IS DISTINCT FROM $2 "
        "OR r.resource_id IS NULL OR w.resource_id IS NULL OR w.release_id<>edge.release_id "
        "OR w.normalized_payload_hash<>edge.source_payload_hash OR (edge.resolution_status<>'unresolved' "
        f"AND NOT EXISTS(SELECT FROM {resource} target WHERE "
        "(target.dataset_id,target.resource_type,target.resource_id)="
        "(edge.dataset_id,edge.target_type,edge.target_resource_id))))",
        dataset_id,
        release_id,
    )
    storage._require(missing is False, "reference_relationship_changed")
    for root, references in (
        (storage.RELATIONS[1], (("resource_id", "InsurancePlan"),)),
        (storage.RELATIONS[2], (("insurance_plan_resource_id", "InsurancePlan"),)),
        (storage.RELATIONS[3], (("affiliation_resource_id", "OrganizationAffiliation"),)),
    ):
        for column, resource_type in references:
            missing = await connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {relation(root)} edge WHERE edge.dataset_id<>$1 OR NOT EXISTS("
                f"SELECT FROM {resource} r WHERE r.dataset_id=edge.dataset_id AND r.resource_type=$2 "
                f"AND r.resource_id=edge.{storage_identifier(column)}))",
                dataset_id,
                resource_type,
            )
            storage._require(missing is False, "derived_relationship_changed")


async def _prepare_final_tables(connection, policy, binding_by_field):
    """Finish bounds and reader grants on private heaps before any publication."""
    dataset_id = binding_by_field["identity"]["dataset_id"]
    literal = "'" + dataset_id.replace("'", "''") + "'"
    for root in storage.RELATIONS:
        name = binding_by_field["tables"][root]["name"]
        relation = storage_relation(policy.schema, name)
        await connection.execute(f"ALTER TABLE {relation} ADD CHECK (dataset_id={literal})")
        await connection.execute(f"ALTER TABLE {relation} RESET (autovacuum_enabled)")
        await storage._close_grants(
            connection, policy, name, readable=binding_by_field.get("contract") not in storage.ISOLATED_CONTRACTS
        )


async def _attach_final_tables(connection, policy, binding_by_field):
    """Attach the verified group without committing or moving any reader pointer."""
    storage._require(binding_by_field.get("contract") not in storage.ISOLATED_CONTRACTS, "isolated_attach_forbidden")
    await _prepare_final_tables(connection, policy, binding_by_field)
    dataset_id = binding_by_field["identity"]["dataset_id"]
    literal = "'" + dataset_id.replace("'", "''") + "'"
    roots = sorted(storage_relation(policy.schema, root) for root in storage.RELATIONS)
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + root for root in roots) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
    )
    for root in sorted(storage.RELATIONS):
        name = binding_by_field["tables"][root]["name"]
        relation = storage_relation(policy.schema, name)
        await connection.execute(
            f"ALTER TABLE {storage_relation(policy.schema, root)} ATTACH PARTITION {relation} FOR VALUES IN ({literal})"
        )


async def _finish_derived_indexes(connection, policy, binding_by_field):
    """Finish serving indexes and authenticate the already sealed CMS references."""
    for root in storage.RELATIONS[1:4]:
        await _indexes(connection, policy, root, binding_by_field["tables"][root]["name"])
    from process.provider_directory_generic_source_auxiliary import relationship_seal

    if binding_by_field["relationship_sha256"] is None:
        storage._require(
            not storage.is_cms_identity(binding_by_field["identity"])
            or binding_by_field["contract"] == storage.CMS_ISOLATED_CONTRACT,
            "relationship_seal_required",
        )
        await _indexes(
            connection, policy, storage.RELATIONSHIP, binding_by_field["tables"][storage.RELATIONSHIP]["name"]
        )
    else:
        storage._require(
            binding_by_field["relationship_sha256"]
            == relationship_seal(binding_by_field["identity"], binding_by_field["tables"][storage.RELATIONSHIP]),
            "relationship_seal_changed",
        )


async def _validated_header(publisher, binding_by_field):
    """Re-read the original protected row after complete content admission."""
    connection, policy = publisher.connection, publisher.policy
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    storage._require(
        actor["session"] == policy.publisher_role and actor["current"] == policy.owner_role,
        "owner_transaction_required",
    )
    header = await connection.fetchrow(
        f"SELECT status,is_current,dataset_hash,content_proof_admission_version,content_proof_admission_kind,"
        f"content_proof_admission_sha256,candidate_storage_json,resource_count,previous_dataset_id,"
        f"completion_proof_required_version,completion_proof_sha256,completion_proof_json,endpoint_id,acquisition_root_run_id,"
        f"publication_metadata_json "
        f"FROM {storage_relation(policy.schema, storage.HEADER)} "
        "WHERE dataset_id=$1 FOR UPDATE",
        binding_by_field["identity"]["dataset_id"],
    )
    storage._require(
        header is not None
        and header["status"] in {"validated", "verification_baseline", "verification_mismatch"}
        and header["is_current"] is False
        and header["content_proof_admission_version"] == 1
        and header["content_proof_admission_kind"] in {"generic", "uhc_canonical"}
        and type(header["dataset_hash"]) is str
        and len(header["dataset_hash"]) == 64
        and type(header["content_proof_admission_sha256"]) is str
        and len(header["content_proof_admission_sha256"]) == 64
        and storage._json(header["candidate_storage_json"]) == binding_by_field,
        "validated_admission_required",
    )
    return header


def _validate_handoff_acquisition(header, identity):
    """Require the selected endpoint, acquisition root and every declared metadata field."""
    metadata = storage._json(header["publication_metadata_json"])
    storage._require(
        header["endpoint_id"] == identity["endpoint_id"]
        and header["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
        and type(metadata) is dict
        and all(metadata.get(key) == expected_value for key, expected_value in identity["metadata"].items()),
        "acquisition_changed",
    )


async def prepare_generic_source_handoff(publisher, binding_by_field):
    """Prepare exact detached inputs; a separate publisher must close all writers."""
    storage._require(
        bound_publisher() is publisher and publisher.storage_binding is not None, "original_storage_publisher_required"
    )
    source_copy_session()
    storage._validate_binding(binding_by_field, binding_by_field["identity"], phases=("prepared", "ready_for_freeze"))
    storage._require(binding_by_field["contract"] in storage.ISOLATED_CONTRACTS, "isolated_storage_required")
    header = await _validated_header(publisher, binding_by_field)
    storage._require(
        header["status"] == "validated" and header["content_proof_admission_kind"] == "generic",
        "isolated_kind_unsupported",
    )
    storage.validate_handoff_completion(header)
    identity = binding_by_field["identity"]
    _validate_handoff_acquisition(header, identity)
    admission_by_field = {field: header[field] for field in storage.HANDOFF_HEADER_FIELDS}
    storage.validate_handoff_header(admission_by_field)
    connection, policy = publisher.connection, publisher.policy
    await storage._verify_tables(connection, policy, binding_by_field)
    if binding_by_field["phase"] == "ready_for_freeze":
        storage._require(binding_by_field["admission"] == admission_by_field, "admission_changed")
        await _verify_handoff_controls(connection, policy, binding_by_field)
        return FHIRSourceCandidateHandoff(storage._encoded(binding_by_field))
    await _finish_derived_indexes(connection, policy, binding_by_field)
    await _relationships(connection, policy, binding_by_field)
    if binding_by_field["contract"] == storage.CMS_ISOLATED_CONTRACT:
        from process.provider_directory_cms_source_handoff import validate_cms_handoff_rows

        await validate_cms_handoff_rows(connection, policy, binding_by_field)
    await _prepare_final_tables(connection, policy, binding_by_field)
    names_by_root = await _prepare_handoff_controls(connection, policy, binding_by_field)
    for key in ("resource_input", "witness_input"):
        await connection.execute(
            f"DROP TABLE {storage_relation(policy.schema, binding_by_field['tables'][key]['name'])}"
        )
    names_by_root.update({root: binding_by_field["tables"][root]["name"] for root in storage.RELATIONS})
    captured = await storage._capture(connection, policy, list(names_by_root.values()))
    handoff_by_field = {
        **binding_by_field,
        "phase": "ready_for_freeze",
        "admission": admission_by_field,
        "tables": {root: captured[name] for root, name in names_by_root.items()},
    }
    snapshot = storage_relation(policy.schema, names_by_root[storage.HEADER])
    updated = await connection.execute(
        f"UPDATE {snapshot} SET candidate_storage_json=$2::jsonb "
        "WHERE dataset_id=$1 AND candidate_storage_json IS NOT DISTINCT FROM $3::jsonb",
        identity["dataset_id"],
        storage._encoded(handoff_by_field),
        storage._encoded(binding_by_field),
    )
    storage._require(updated == "UPDATE 1", "header_snapshot_changed")
    await storage._store(connection, policy, binding_by_field["identity"], binding_by_field, handoff_by_field)
    await _verify_handoff_controls(connection, policy, handoff_by_field)
    return FHIRSourceCandidateHandoff(storage._encoded(handoff_by_field))


async def _prepare_handoff_controls(connection, policy, binding_by_field):
    """Copy the complete selected header/proofs without freezing their shared roots."""
    dataset_id = binding_by_field["identity"]["dataset_id"]
    prefix = binding_by_field["tables"][storage.RESOURCE]["name"].removesuffix("_r")
    names_by_root = {}
    literal = "'" + dataset_id.replace("'", "''") + "'"
    for root in storage.HANDOFF_CONTROL_ROOTS:
        name = prefix + "_" + storage._SUFFIXES[root]
        source, target = storage_relation(policy.schema, root), storage_relation(policy.schema, name)
        await connection.execute(
            f"CREATE TABLE {target} (LIKE {source} INCLUDING DEFAULTS INCLUDING GENERATED "
            "INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING INDEXES)"
        )
        await storage._close_grants(connection, policy, name)
        await connection.execute(f"ALTER TABLE {target} ADD CHECK (dataset_id={literal})")
        columns = await connection.fetch(
            "SELECT attname FROM pg_attribute WHERE attrelid=$1::regclass "
            "AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum",
            source,
        )
        storage._require(bool(columns), "control_snapshot_layout_changed")
        fields = ",".join(storage_identifier(column["attname"]) for column in columns)
        copied = await connection.execute(
            f"INSERT INTO {target} ({fields}) SELECT {fields} FROM {source} WHERE dataset_id=$1",
            dataset_id,
        )
        storage._require(root != storage.HEADER or copied == "INSERT 0 1", "header_snapshot_changed")
        names_by_root[root] = name
    return names_by_root


async def _verify_handoff_controls(connection, policy, binding_by_field):
    """Full row-set equality prevents a short admission seal from hiding evidence drift."""
    dataset_id = binding_by_field["identity"]["dataset_id"]
    for root in storage.HANDOFF_CONTROL_ROOTS:
        source = storage_relation(policy.schema, root)
        snapshot = storage_relation(policy.schema, binding_by_field["tables"][root]["name"])
        # The header's native json has no equality operator; preserve its serialized form.
        comparison = 'row_to_json(control_row)::text COLLATE "C"' if root == storage.HEADER else "control_row.*"
        changed = await connection.fetchval(
            f"SELECT EXISTS((SELECT {comparison} FROM {source} AS control_row WHERE dataset_id=$1 "
            f"EXCEPT SELECT {comparison} FROM {snapshot} AS control_row) "
            f"UNION ALL (SELECT {comparison} FROM {snapshot} AS control_row "
            f"EXCEPT SELECT {comparison} FROM {source} AS control_row WHERE dataset_id=$1))",
            dataset_id,
        )
        storage._require(changed is False, "control_snapshot_changed")


async def record_candidate_handoff(directory, candidate, handoffs):
    """Recover the exact committed candidate on initial completion and resume."""
    from process import provider_directory_generic_source_policy as source

    if handoffs is None or candidate is None or getattr(source._FACTORY.get(), "storage_binding", None) is None:
        return
    identity = storage.candidate_identity(directory, candidate)
    async with source.source_transaction(directory.db, isolation="repeatable read"):
        async with source_owner_transaction(directory.db) as publisher:
            binding_by_field = await storage._locked(
                publisher.connection, publisher.policy, identity, phases=("ready_for_freeze",)
            )
            storage._require(binding_by_field is not None, "handoff_missing")
            handoff = await prepare_generic_source_handoff(publisher, binding_by_field)
    receipt_by_field = {"document": handoff.document, "sha256": handoff.document_sha256}
    storage._require(handoffs.get(candidate.dataset_id, receipt_by_field) == receipt_by_field, "handoff_changed")
    handoffs[candidate.dataset_id] = receipt_by_field


class _FrozenSourceRows:
    """Use the existing named-SQL adapter on the publisher's original transaction."""

    def __init__(self, connection):
        from process.network_cms_registry_address_capture import NativeReceiptSession

        self.connection = connection
        self.session = NativeReceiptSession(connection)

    async def all(self, query, **parameters):
        """Read a bounded proof page without substituting the physical transaction."""
        from sqlalchemy import text

        storage._require(self.connection.is_in_transaction(), "transaction_required")
        return await self.session.execute(text(query), parameters)


async def _frozen_source_inventory(connection, policy, document):
    """Only the owner may change between the handoff and protected custody."""
    names = [table["name"] for table in document["tables"].values()]
    captured = await storage._capture(connection, policy, names)
    owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.owner_role)
    storage._require(owner_oid != document["storage"]["owner_oid"], "freeze_required")
    for table in document["tables"].values():
        storage._require(captured[table["name"]] == {**table, "owner": owner_oid}, "frozen_inventory_changed")
    return owner_oid


async def _frozen_handoff_header(connection, policy, document):
    table = storage_relation(policy.schema, document["tables"][storage.HEADER]["name"])
    header_rows = await _FrozenSourceRows(connection).all(f"SELECT * FROM {table}")
    storage._require(len(header_rows) == 1, "header_snapshot_changed")
    header, identity = dict(header_rows[0]), document["identity"]
    metadata = storage._json(header["publication_metadata_json"])
    storage._require(
        header["dataset_id"] == identity["dataset_id"]
        and header["endpoint_id"] == identity["endpoint_id"]
        and header["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
        and header["status"] == "validated"
        and header["is_current"] is False
        and storage._json(header["candidate_storage_json"]) == document
        and {key: header[key] for key in storage.HANDOFF_HEADER_FIELDS} == document["admission"]
        and type(metadata) is dict
        and all(metadata.get(key) == expected_fact for key, expected_fact in identity["metadata"].items()),
        "header_snapshot_changed",
    )
    storage.validate_handoff_completion(header)
    from process.provider_directory_admission_seal import admission_seal_from_validated_metadata

    seal = admission_seal_from_validated_metadata(metadata)
    storage._require(seal is not None, "validated_admission_required")
    actual = (
        header["content_proof_admission_version"],
        header["content_proof_admission_kind"],
        header["content_proof_admission_sha256"],
        header["content_proof_resource_types"],
        header["publication_metadata_sha256"],
        storage._json(header["publication_metadata_summary_json"]),
    )
    storage._require(
        actual
        == (
            seal.admission_version,
            seal.admission_kind,
            seal.proof_sha256,
            list(seal.resource_types),
            seal.metadata_sha256,
            seal.metadata_summary,
        )
        and seal.admission_kind == "generic",
        "admission_changed",
    )
    return header


async def _frozen_content_proof(connection, directory, policy, document, header):
    from process.provider_directory_proof_store import (
        PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
        PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
        ProviderDirectoryStoredProofOptions,
        build_stored_dataset_proof,
    )

    identity, reader = document["identity"], _FrozenSourceRows(connection)
    metadata = storage._json(header["publication_metadata_json"])
    contract = directory._dataset_resource_hash_contract(header)
    projection_date = directory._dataset_semantic_projection_as_of(header, contract)
    resources = tuple(identity["metadata"]["selected_resources"])
    scope = directory._dataset_proof_resource_scope(header, resources, contract)
    resource = storage_relation(policy.schema, document["tables"][storage.RESOURCE]["name"])
    invalid_identity = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {resource} WHERE resource_type<>ALL($1::text[]) "
        "OR resource_id IS DISTINCT FROM btrim(resource_id) OR resource_id='')",
        list(scope or resources),
    )
    storage._require(invalid_identity is False, "resource_identity_changed")
    stored = await build_stored_dataset_proof(
        reader,
        policy.schema,
        dataset_id=identity["dataset_id"],
        endpoint_id=identity["endpoint_id"],
        acquisition_root_run_id=identity["acquisition_root_run_id"],
        source_ids=identity["metadata"]["source_ids"],
        selected_resources=resources,
        options=ProviderDirectoryStoredProofOptions(
            scope,
            contract,
            projection_date,
            document["tables"][PROVIDER_DIRECTORY_PROOF_SHARD_TABLE]["name"],
        ),
    )
    storage._require(stored.metadata == metadata.get(PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY), "proof_changed")
    coordinates = (policy.schema, document["tables"][storage.RESOURCE]["name"])
    direct = await directory._endpoint_dataset_content_proof(
        reader,
        identity["dataset_id"],
        scope or resources,
        verify_payload_hashes=True,
        resource_hash_contract=contract,
        resource_relation=coordinates,
    )
    for field in ("dataset_hash", "resource_count", "resource_hashes", "resource_counts"):
        storage._require(getattr(direct, field) == getattr(stored, field), "content_proof_changed")
    storage._require(
        (direct.dataset_hash, direct.resource_count) == (header["dataset_hash"], header["resource_count"]),
        "content_proof_changed",
    )
    await _frozen_subset_proof(reader, directory, document, header, direct, coordinates)
    return stored


async def _frozen_subset_proof(reader, directory, document, header, proof, coordinates):
    completion = storage._json(header["completion_proof_json"])
    if completion is None:
        return
    candidate = SimpleNamespace(
        dataset_id=document["identity"]["dataset_id"],
        selected_resources=tuple(document["identity"]["metadata"]["selected_resources"]),
    )
    acquired = await directory._subset_acquired_resource_hashes(
        reader,
        candidate,
        proof.resource_counts,
        resource_relation=coordinates,
    )
    dataset = completion["dataset"]
    storage._require(
        dataset["resource_hashes"] == proof.resource_hashes
        and dataset["resource_counts"] == proof.resource_counts
        and dataset["acquired_resource_hashes"] == acquired,
        "completion_dataset_changed",
    )


async def _frozen_projection_exact(connection, directory, policy, document):
    relation = lambda root: storage_relation(policy.schema, document["tables"][root]["name"])
    header, resource = relation(storage.HEADER), relation(storage.RESOURCE)
    parameters_by_name = dict(dataset_id=document["identity"]["dataset_id"])
    reader = _FrozenSourceRows(connection)
    projection = (
        directory._dataset_insurance_plan_projection_source_ctes(
            header,
            resource,
            relation(storage.RELATIONS[1]),
            "",
        )
        + directory._dataset_insurance_plan_projection_proof_select()
    )
    proof_rows = await reader.all(projection, **parameters_by_name)
    storage._require(
        len(proof_rows) == 1
        and proof_rows[0]["target_dataset_count"] == 1
        and proof_rows[0]["insurance_plan_resource_count"] == proof_rows[0]["plan_projection_count"]
        and proof_rows[0]["missing_or_mismatched_plan_projection_count"] == 0
        and proof_rows[0]["unexpected_plan_projection_count"] == 0,
        "plan_projection_changed",
    )
    pattern = r"(?i)(?:^|/)Organization/([A-Za-z0-9.-]{1,64})(?:/_history/[A-Za-z0-9.-]{1,64})?/?(?:[?#].*)?$"
    for root, source_cte, edge_cte, fields in (
        (
            storage.RELATIONS[2],
            directory._network_plan_source_ctes,
            directory._network_plan_edge_ctes,
            "network_resource_id,insurance_plan_resource_id",
        ),
        (
            storage.RELATIONS[3],
            directory._affiliation_org_source_ctes,
            directory._affiliation_org_edge_ctes,
            "participating_organization_resource_id,affiliation_resource_id",
        ),
    ):
        expected = f"SELECT {fields} FROM expected_edges"
        actual = f"SELECT {fields} FROM {relation(root)}"
        query = (
            source_cte(header, resource, "")
            + edge_cte(pattern)
            + (f" SELECT EXISTS(({expected} EXCEPT {actual}) UNION ALL ({actual} EXCEPT {expected})) AS changed")
        )
        proof_rows = await reader.all(query, **parameters_by_name)
        storage._require(len(proof_rows) == 1 and proof_rows[0]["changed"] is False, "derived_projection_changed")


async def validate_frozen_source_handoff(connection, directory, handoff, *, protected_owner_role):
    """Scan a closed candidate using existing proof rules; no pointer or privilege writes.

    The protected preparation caller authenticates its issued manifest and fixed
    layout before calling. This data check never grants publication authority.
    """
    storage._require(type(handoff) is FHIRSourceCandidateHandoff, "handoff_required")
    storage._require(handoff.document.get("contract") == storage.ISOLATED_CONTRACT, "ordinary_handoff_required")
    return await _validate_frozen_source_handoff(
        connection, directory, handoff, protected_owner_role=protected_owner_role
    )


async def validate_frozen_cms_source_handoff(connection, directory, handoff, *, protected_owner_role):
    """Check complete frozen CMS intake without claiming identity or serving readiness."""
    from process.provider_directory_cms_source_handoff import validate_cms_handoff_rows

    storage._require(type(handoff) is FHIRSourceCandidateHandoff, "handoff_required")
    document = handoff.document
    storage._require(document.get("contract") == storage.CMS_ISOLATED_CONTRACT, "cms_handoff_required")
    storage._require(connection.is_in_transaction() is True, "transaction_required")
    transaction_id = await connection.fetchval("SELECT pg_current_xact_id()::text")
    result = await _validate_frozen_source_handoff(
        connection, directory, handoff, protected_owner_role=protected_owner_role
    )
    policy = SimpleNamespace(schema=document["storage"]["storage_schema"], owner_role=protected_owner_role)
    await validate_cms_handoff_rows(connection, policy, document)
    storage._require(
        connection.is_in_transaction() is True
        and await connection.fetchval("SELECT pg_current_xact_id()::text") == transaction_id,
        "transaction_changed",
    )
    return {**result, "source_release": document["identity"]["metadata"]["source_release"]}


async def _validate_frozen_source_handoff(connection, directory, handoff, *, protected_owner_role):
    storage._require(connection.is_in_transaction() is True, "transaction_required")
    document = handoff.document
    storage.validate_handoff_document(document, document.get("storage"))
    policy = SimpleNamespace(schema=document["storage"]["storage_schema"], owner_role=protected_owner_role)
    transaction_id = await connection.fetchval("SELECT pg_current_xact_id()::text")
    relations = sorted(storage_relation(policy.schema, table["name"]) for table in document["tables"].values())
    await connection.execute("LOCK TABLE " + ",".join(relations) + " IN ACCESS SHARE MODE NOWAIT")
    await _frozen_source_inventory(connection, policy, document)
    header = await _frozen_handoff_header(connection, policy, document)
    for table in document["tables"].values():
        invalid = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {storage_relation(policy.schema, table['name'])} WHERE dataset_id<>$1)",
            document["identity"]["dataset_id"],
        )
        storage._require(invalid is False, "dataset_changed")
    stored = await _frozen_content_proof(connection, directory, policy, document, header)
    await _relationships(connection, policy, document)
    await _every_row(connection, policy, document["tables"][storage.WITNESS]["name"], WITNESS_COLUMNS, _witness)
    await _frozen_projection_exact(connection, directory, policy, document)
    await _frozen_source_inventory(connection, policy, document)
    storage._require(
        connection.is_in_transaction() is True
        and await connection.fetchval("SELECT pg_current_xact_id()::text") == transaction_id,
        "transaction_changed",
    )
    return dict(
        handoff_sha256=handoff.document_sha256,
        dataset_hash=stored.dataset_hash,
        resource_count=stored.resource_count,
        resource_hashes=stored.resource_hashes,
        resource_counts=stored.resource_counts,
        proof_sha256=stored.metadata["proof_sha256"],
    )


async def attach_generic_source_candidate(publisher, binding_by_field):
    """Finish the existing validated owner scope using its issued storage contract.

    The isolated contract stops at a handoff while the Source owner retains write
    authority. Protected custody and read admission are required before activation.
    """
    if getattr(publisher, "storage_binding", None) is not None:
        return await prepare_generic_source_handoff(publisher, binding_by_field)
    connection, policy = publisher.connection, publisher.policy
    storage._require(binding_by_field["phase"] == "prepared", "completion_required")
    await _validated_header(publisher, binding_by_field)
    # Native identities remain fixed even though trusted serving builders filled
    # the derived heaps in this transaction. Authenticate before their indexes.
    await storage._verify_tables(connection, policy, binding_by_field)
    await _finish_derived_indexes(connection, policy, binding_by_field)
    await _relationships(connection, policy, binding_by_field)
    from process.provider_directory_generic_source_auxiliary import publish_identity_candidate

    published_auxiliary = await publish_identity_candidate(publisher, binding_by_field)
    await _attach_final_tables(connection, policy, binding_by_field)
    for key in ("resource_input", "witness_input"):
        await connection.execute(
            f"DROP TABLE {storage_relation(policy.schema, binding_by_field['tables'][key]['name'])}"
        )
    names = [binding_by_field["tables"][root]["name"] for root in storage.RELATIONS]
    captured = await storage._capture(connection, policy, names, private=False)
    published_by_field = {
        **binding_by_field,
        "phase": "published",
        "auxiliary": published_auxiliary,
        "tables": {root: captured[binding_by_field["tables"][root]["name"]] for root in storage.RELATIONS},
    }
    await storage._store(connection, policy, binding_by_field["identity"], binding_by_field, published_by_field)
