# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private CMS identity observations and atomic, UUID-preserving publication.

The common SOURCE owner creates, attests and enrolls these exact heaps. Global
key allocation remains private until the entire source candidate is admitted.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import asyncpg

from db.plan_release_storage import storage_identifier, storage_relation
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.provider_directory_generic_candidate import COPY_MAX_BYTES, COPY_MAX_ROWS, GenericCandidateError
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

CMS_IDENTITY_TABLES = (
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
)
CMS_IDENTITY_COLUMNS = {
    CMS_IDENTITY_TABLES[0]: ("source_id", "resource_type", "resource_id", "organization_id", "site_id", "created_at"),
    CMS_IDENTITY_TABLES[1]: (
        "source_id",
        "resource_type",
        "resource_id",
        "release_id",
        "payload_sha256",
        "payload_json",
        "observed_at",
    ),
    CMS_IDENTITY_TABLES[2]: ("source_id", "resource_type", "resource_id", "network_id", "created_at"),
    CMS_IDENTITY_TABLES[3]: (
        "source_id",
        "release_id",
        "network_resource_type",
        "network_resource_id",
        "insurance_plan_resource_id",
        "network_refs",
        "owned_by_ref",
        "administered_by_ref",
        "plan_payload_sha256",
        "plan_payload_json",
        "observed_at",
    ),
}
CMS_IDENTITY_KEYS = {
    CMS_IDENTITY_TABLES[0]: ("source_id", "resource_type", "resource_id"),
    CMS_IDENTITY_TABLES[1]: ("source_id", "resource_type", "resource_id", "release_id"),
    CMS_IDENTITY_TABLES[2]: ("source_id", "resource_id"),
    CMS_IDENTITY_TABLES[3]: ("source_id", "release_id", "network_resource_id", "insurance_plan_resource_id"),
}
_SECONDARY = {
    CMS_IDENTITY_TABLES[0]: (("organization_id",), ("site_id",)),
    CMS_IDENTITY_TABLES[1]: (("source_id", "release_id"),),
    CMS_IDENTITY_TABLES[2]: (("network_id",),),
    CMS_IDENTITY_TABLES[3]: (("source_id", "release_id", "insurance_plan_resource_id"),),
}
_ACTIVE = ContextVar("cms_identity_candidate", default=None)


class CMSIdentityReprepareRequired(GenericCandidateError):
    """Another accepted candidate won a global key; rebuild every dependent output."""


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_cms_identity_candidate_" + code)


def _decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def _identity(identity):
    metadata = identity.get("metadata", {})
    source = metadata.get("source_release", {})
    producer = metadata.get("network_bindings", {})
    _require(
        metadata.get("source_ids") in (["cms-npd"], ("cms-npd",))
        and source.get("source_id") == "cms-npd"
        and type(source.get("vector_sha256")) is str
        and len(source["vector_sha256"]) == 64
        and producer.get("producer_id") == "cms-npd-bulk-fhir-v1"
        and producer.get("alias_scope") == "cms-npd",
        "source_identity_changed",
    )
    return source["vector_sha256"]


def _names(receipts):
    _require(type(receipts) is dict and set(receipts) == set(CMS_IDENTITY_TABLES), "stages_changed")
    names_by_root = {}
    for root, receipt in receipts.items():
        _require(
            type(receipt) is dict and set(receipt) == {"name", "oid", "owner", "layout_sha256"}, "stage_receipt_changed"
        )
        _require(
            type(receipt["oid"]) is int
            and receipt["oid"] > 0
            and type(receipt["owner"]) is int
            and receipt["owner"] > 0,
            "stage_receipt_changed",
        )
        storage_identifier(receipt["name"])
        _require(receipt["name"] != root, "global_target_forbidden")
        names_by_root[root] = receipt["name"]
    _require(len(set(names_by_root.values())) == 4, "stages_aliased")
    return names_by_root


@dataclass(frozen=True)
class _IdentityScope:
    publisher: object
    identity: dict
    receipts: dict
    task: object


@asynccontextmanager
async def bind_cms_identity_candidate(publisher, identity, receipts):
    """Bind only common-owner-attested stages on the original physical session."""
    _require(bound_publisher() is publisher and _ACTIVE.get() is None, "publisher_changed")
    source_copy_session()
    _identity(identity)
    _names(receipts)
    token = _ACTIVE.set(_IdentityScope(publisher, identity, receipts, asyncio.current_task()))
    try:
        yield
    finally:
        _ACTIVE.reset(token)


async def identity_candidate(session, *, phases=("loading",)):
    """Recheck the protected native header before any batch statement or COPY."""
    scope = _ACTIVE.get()
    _require(
        scope is not None
        and scope.task is asyncio.current_task()
        and scope.publisher is bound_publisher()
        and session is source_copy_session(),
        "physical_session_required",
    )
    connection, policy = scope.publisher.connection, scope.publisher.policy
    _require(connection.is_in_transaction() is True, "transaction_required")
    await _protected_auxiliary(connection, policy, scope.identity, scope.receipts, phases)
    return scope.publisher, scope.identity, scope.receipts


async def _protected_auxiliary(connection, policy, identity, receipts, phases):
    raw = await connection.fetchval(
        f"SELECT candidate_storage_json FROM {storage_relation(policy.schema, 'provider_directory_endpoint_dataset')} "
        "WHERE dataset_id=$1 FOR UPDATE",
        identity["dataset_id"],
    )
    binding = _decoded(raw)
    from process.provider_directory_generic_source_storage import _validate_binding

    _validate_binding(binding, identity, phases=("prepared",))
    auxiliary = binding["auxiliary"]
    _require(
        auxiliary["phase"] in phases and {root: auxiliary["tables"][root] for root in CMS_IDENTITY_TABLES} == receipts,
        "protected_binding_changed",
    )


@dataclass(frozen=True)
class _EntityObservations:
    staged_bindings: dict
    published_bindings: dict
    staged_evidence: dict
    published_evidence: dict


async def _entity_observations(connection, policy, receipts, source_id, resource_type, release_id, resource_ids):
    """Four indexed header reads, independent of the number of requested keys."""
    return await read_entity_observations(
        connection,
        _relations(policy, receipts),
        _canonical_schema(policy),
        source_id,
        resource_type,
        release_id,
        resource_ids,
    )


async def read_entity_observations(
    connection, relations, canonical_schema, source_id, resource_type, release_id, resource_ids
):
    """Read qualified private and canonical coordinates; callers authenticate them."""
    results = []
    for root, is_stage in (
        (CMS_IDENTITY_TABLES[0], True),
        (CMS_IDENTITY_TABLES[0], False),
        (CMS_IDENTITY_TABLES[1], True),
        (CMS_IDENTITY_TABLES[1], False),
    ):
        relation = relations[root] if is_stage else storage_relation(canonical_schema, root)
        fields = (
            "resource_id,organization_id,site_id,created_at"
            if root == CMS_IDENTITY_TABLES[0]
            else "resource_id,payload_sha256,observed_at"
        )
        extra = " AND release_id=$4" if root == CMS_IDENTITY_TABLES[1] else ""
        arguments = (
            (source_id, resource_type, resource_ids, release_id) if extra else (source_id, resource_type, resource_ids)
        )
        rows = await connection.fetch(
            f"SELECT {fields} FROM {relation} WHERE source_id=$1 AND resource_type=$2 "
            f"AND resource_id=ANY($3::varchar[]){extra}",
            *arguments,
        )
        _require(len(rows) <= len(resource_ids), "lookup_ambiguous")
        results.append({row["resource_id"]: row for row in rows})
    return _EntityObservations(*results)


def _entity_binding_record(source_id, resource_type, resource_id, entity_id, created_at):
    return (
        source_id,
        resource_type,
        resource_id,
        entity_id if resource_type == "Organization" else None,
        entity_id if resource_type == "Location" else None,
        created_at,
    )


def _entity_evidence_record(
    source_id, resource_type, resource_id, release_id, payload_hash, resource_payload, timestamp
):
    return (
        source_id,
        resource_type,
        resource_id,
        release_id,
        payload_hash,
        json.dumps(resource_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False),
        timestamp,
    )


async def stage_entity_batch(session, source_id, release_id, resource_type, payloads):
    """Reuse staged/published opaque IDs; COPY only exact absent observations."""
    publisher, identity, receipts = await identity_candidate(session)
    _require(
        source_id == "cms-npd"
        and release_id == _identity(identity)
        and resource_type in {"Organization", "Location"}
        and 0 < len(payloads) <= 1000,
        "batch_scope_changed",
    )
    observations = await _entity_observations(
        publisher.connection, publisher.policy, receipts, source_id, resource_type, release_id, list(payloads)
    )
    ids, binding_rows, evidence_rows = build_entity_batch_rows(
        source_id, release_id, resource_type, payloads, observations
    )
    for root, copy_rows in ((CMS_IDENTITY_TABLES[0], binding_rows), (CMS_IDENTITY_TABLES[1], evidence_rows)):
        if copy_rows:
            await copy_identity_rows(publisher, receipts[root]["name"], CMS_IDENTITY_COLUMNS[root], copy_rows)
    return ids


def build_entity_batch_rows(source_id, release_id, resource_type, payloads, observations):
    """Preserve accepted UUIDs and timestamps, refusing conflicting release facts."""
    ids, binding_rows, evidence_rows = {}, [], []
    now = datetime.now(timezone.utc)
    id_column = "organization_id" if resource_type == "Organization" else "site_id"
    for resource_id, (payload_hash, resource_payload) in payloads.items():
        staged, incumbent = (
            observations.staged_bindings.get(resource_id),
            observations.published_bindings.get(resource_id),
        )
        if staged is not None and incumbent is not None and staged[id_column] != incumbent[id_column]:
            raise CMSIdentityReprepareRequired("provider_directory_cms_identity_whole_candidate_reprepare_required")
        prior = staged if staged is not None else incumbent
        entity_id = prior[id_column] if prior is not None else uuid4()
        ids[resource_id] = entity_id
        if staged is None:
            binding_rows.append(
                _entity_binding_record(
                    source_id,
                    resource_type,
                    resource_id,
                    entity_id,
                    incumbent["created_at"] if incumbent is not None else now,
                )
            )
        for evidence in (
            observations.staged_evidence.get(resource_id),
            observations.published_evidence.get(resource_id),
        ):
            _require(evidence is None or evidence["payload_sha256"] == payload_hash, "release_payload_conflict")
        if resource_id not in observations.staged_evidence:
            timestamp = (
                observations.published_evidence[resource_id]["observed_at"]
                if resource_id in observations.published_evidence
                else now
            )
            evidence_rows.append(
                _entity_evidence_record(
                    source_id, resource_type, resource_id, release_id, payload_hash, resource_payload, timestamp
                )
            )
    return ids, binding_rows, evidence_rows


def _copy_value_bytes(cell):
    _require(
        cell is None or type(cell) in (str, int, bool, bytes, datetime) or isinstance(cell, UUID), "copy_value_invalid"
    )
    if cell is None:
        return 0
    if type(cell) is str:
        return len(cell.encode()) + 1
    if type(cell) is bytes:
        return len(cell)
    return 16 if isinstance(cell, UUID) else 8


async def copy_identity_rows(publisher, name, columns, records):
    """Native typed COPY with complete framing/UUID/timestamp byte accounting."""
    _require(publisher is bound_publisher(), "publisher_changed")
    return await copy_identity_rows_native(
        publisher.connection, source_copy_session(), publisher.policy.schema, name, columns, records
    )


async def copy_identity_rows_native(connection, session, schema, name, columns, records):
    """COPY bounded rows on caller-qualified coordinates and the original session."""
    batch, size, total = [], 21, 0
    for copy_row in records:
        _require(type(copy_row) is tuple and len(copy_row) == len(columns), "copy_record_invalid")
        row_size = 2 + sum(4 + _copy_value_bytes(cell) for cell in copy_row)
        _require(21 + row_size <= COPY_MAX_BYTES, "copy_record_too_large")
        if batch and (len(batch) == COPY_MAX_ROWS or size + row_size > COPY_MAX_BYTES):
            total += await _copy_batch(connection, session, schema, name, columns, batch)
            batch, size = [], 21
        batch.append(copy_row)
        size += row_size
    if batch:
        total += await _copy_batch(connection, session, schema, name, columns, batch)
    return total


async def _copy_batch(connection, session, schema, name, columns, records):
    try:
        status = await connection.copy_records_to_table(name, schema_name=schema, columns=columns, records=records)
    except asyncio.CancelledError, TimeoutError:
        await close_interrupted_copy(connection)
        await invalidate_copy_owner(session, connection)
        raise
    _require(status == f"COPY {len(records)}", "copy_incomplete")
    return len(records)


async def prepare_identity_stages(publisher, identity, names):
    """Only essential map keys are maintained during stable-UUID preparation."""
    _require(publisher is bound_publisher() and publisher.connection.is_in_transaction() is True, "publisher_changed")
    source_copy_session()
    _identity(identity)
    _require(set(names) == set(CMS_IDENTITY_TABLES), "stages_changed")
    for root, name in names.items():
        _require(name != root, "global_target_forbidden")
        relation = storage_relation(publisher.policy.schema, name)
        keys = ",".join(storage_identifier(column) for column in CMS_IDENTITY_KEYS[root])
        await publisher.connection.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY ({keys})")


def _relations(policy, receipts):
    return {root: storage_relation(policy.schema, name) for root, name in _names(receipts).items()}


def _canonical_schema(policy):
    """Keep accepted identity observations outside the issued private namespace."""
    publisher = bound_publisher()
    _require(publisher.policy is policy, "publisher_changed")
    binding = getattr(publisher, "storage_binding", None)
    if binding is None:
        return policy.schema
    from process.provider_directory_source_storage_binding import require_storage_schema

    require_storage_schema(publisher, binding.catalog_schema)
    return binding.catalog_schema


def _same_coordinates(root):
    return " AND ".join(
        f"candidate.{storage_identifier(column)} IS NOT DISTINCT FROM incumbent.{storage_identifier(column)}"
        for column in CMS_IDENTITY_COLUMNS[root]
    )


async def _global_replay(connection, policy, receipts, *, complete=False):
    await verify_identity_replay(connection, _relations(policy, receipts), _canonical_schema(policy), complete=complete)


async def verify_identity_replay(connection, relations, canonical_schema, *, complete=False):
    """Compare every accepted key using explicit, caller-qualified coordinates."""
    for root, relation in relations.items():
        keys = ",".join(storage_identifier(column) for column in CMS_IDENTITY_KEYS[root])
        join = "LEFT JOIN" if complete else "JOIN"
        missing = "incumbent.source_id IS NULL OR " if complete else ""
        changed = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {relation} candidate {join} "
            f"{storage_relation(canonical_schema, root)} incumbent USING ({keys}) WHERE {missing}NOT ({_same_coordinates(root)}))"
        )
        if changed:
            raise CMSIdentityReprepareRequired("provider_directory_cms_identity_whole_candidate_reprepare_required")


async def _source_coordinates(connection, policy, identity, receipts):
    release = _identity(identity)
    for root, relation in _relations(policy, receipts).items():
        extra = " OR release_id<>$1" if "release_id" in CMS_IDENTITY_COLUMNS[root] else ""
        args = (release,) if extra else ()
        invalid = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {relation} WHERE source_id<>'cms-npd'{extra})", *args
        )
        _require(invalid is False, "source_coordinates_changed")


_NETWORK_INPUTS = """WITH source_plans AS MATERIALIZED (
 SELECT resource_id,raw_payload_sha256,raw_payload_json AS payload FROM {witness}
 WHERE dataset_id=$1 AND resource_type='InsurancePlan'
), fields AS (
 SELECT resource_id,0 AS family,0::bigint AS parent_ordinal,payload->'network' AS value FROM source_plans
 UNION ALL
 SELECT p.resource_id,s.family,parent.ordinal,parent.value->'network'
 FROM source_plans p CROSS JOIN (VALUES ('plan',1),('coverage',2)) s(field,family)
 CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(p.payload->s.field)='array'
   THEN p.payload->s.field ELSE '[]'::jsonb END) WITH ORDINALITY parent(value,ordinal)
), refs AS (
 SELECT resource_id,item.value->>'reference' AS reference,family,parent_ordinal,item.ordinal
 FROM fields CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(fields.value)='array'
   THEN fields.value ELSE jsonb_build_array(fields.value) END) WITH ORDINALITY item(value,ordinal)
 WHERE jsonb_typeof(item.value->'reference')='string'
), first_refs AS (
 SELECT DISTINCT ON(resource_id,reference) * FROM refs
 ORDER BY resource_id,reference,family,parent_ordinal,ordinal
), reference_arrays AS (
 SELECT resource_id,jsonb_agg(reference ORDER BY family,parent_ordinal,ordinal) AS refs
 FROM first_refs GROUP BY resource_id
), expected_pairs AS (
 SELECT DISTINCT r.resource_id AS plan_id,split_part(r.reference,'/',2) AS network_id
 FROM first_refs r JOIN {evidence} e ON e.source_id='cms-npd' AND e.release_id=$2
  AND e.resource_type='Organization' AND e.resource_id=split_part(r.reference,'/',2)
 WHERE r.reference ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
), expected_networks AS (
 SELECT resource_id FROM {evidence} WHERE resource_type='Organization'
  AND (payload_json @> '{{"type":[{{"text":"ntwk"}}]}}'::jsonb
    OR payload_json @> '{{"type":[{{"coding":[{{"code":"ntwk"}}]}}]}}'::jsonb)
 UNION SELECT network_id FROM expected_pairs
)
"""


_NETWORK_COMPLETENESS = """
 SELECT EXISTS(SELECT FROM expected_pairs x LEFT JOIN {plans} p
  ON p.insurance_plan_resource_id=x.plan_id AND p.network_resource_id=x.network_id
  WHERE p.source_id IS NULL)
 OR EXISTS(SELECT FROM {plans} p LEFT JOIN expected_pairs x
  ON x.plan_id=p.insurance_plan_resource_id AND x.network_id=p.network_resource_id
  LEFT JOIN source_plans source ON source.resource_id=p.insurance_plan_resource_id
  LEFT JOIN reference_arrays arrays ON arrays.resource_id=p.insurance_plan_resource_id
  WHERE x.plan_id IS NULL OR p.network_resource_type<>'Organization'
   OR p.plan_payload_sha256 IS DISTINCT FROM source.raw_payload_sha256
   OR p.plan_payload_json IS DISTINCT FROM source.payload
   OR p.network_refs IS DISTINCT FROM coalesce(arrays.refs,'[]'::jsonb)
   OR p.owned_by_ref IS DISTINCT FROM CASE WHEN jsonb_typeof(source.payload->'ownedBy'->'reference')='string'
     THEN source.payload->'ownedBy'->>'reference' END
   OR p.administered_by_ref IS DISTINCT FROM CASE WHEN jsonb_typeof(source.payload->'administeredBy'->'reference')='string'
     THEN source.payload->'administeredBy'->>'reference' END)
 OR EXISTS(SELECT FROM expected_networks x LEFT JOIN {networks} n ON n.resource_id=x.resource_id WHERE n.resource_id IS NULL)
 OR EXISTS(SELECT FROM {networks} n LEFT JOIN expected_networks x ON x.resource_id=n.resource_id
  WHERE x.resource_id IS NULL OR n.resource_type<>'Organization')
 """


async def _complete_source_facts(connection, policy, identity, receipts):
    binding = _decoded(
        await connection.fetchval(
            f"SELECT candidate_storage_json FROM {storage_relation(policy.schema, 'provider_directory_endpoint_dataset')} "
            "WHERE dataset_id=$1",
            identity["dataset_id"],
        )
    )
    from process.provider_directory_generic_source_storage import _validate_binding

    _validate_binding(binding, identity, phases=("prepared",))
    _require(
        binding["auxiliary"]["phase"] == "loading"
        and {root: binding["auxiliary"]["tables"][root] for root in CMS_IDENTITY_TABLES} == receipts,
        "protected_binding_changed",
    )
    witness = storage_relation(policy.schema, binding["tables"]["provider_directory_cms_npd_resource_witness"]["name"])
    relations = _relations(policy, receipts)
    await verify_identity_source_facts(connection, relations, identity, witness)
    await _canonical_authority(connection, policy, receipts)


async def verify_identity_source_facts(connection, relations, identity, witness):
    """Compare the full private entity/network evidence against frozen raw facts."""
    entities = relations[CMS_IDENTITY_TABLES[0]]
    evidence_relation = relations[CMS_IDENTITY_TABLES[1]]
    networks = relations[CMS_IDENTITY_TABLES[2]]
    plans = relations[CMS_IDENTITY_TABLES[3]]
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {witness} w LEFT JOIN {evidence_relation} e ON e.source_id=w.source_id "
        "AND e.release_id=w.release_id AND e.resource_type=w.resource_type AND e.resource_id=w.resource_id "
        "WHERE w.dataset_id=$1 AND w.resource_type IN ('Organization','Location') "
        "AND (e.resource_id IS NULL OR e.payload_sha256<>w.raw_payload_sha256 OR e.payload_json<>w.raw_payload_json)) "
        f"OR EXISTS(SELECT FROM {evidence_relation} e LEFT JOIN {witness} w ON w.dataset_id=$1 "
        "AND w.source_id=e.source_id AND w.release_id=e.release_id AND w.resource_type=e.resource_type "
        "AND w.resource_id=e.resource_id WHERE w.resource_id IS NULL OR e.resource_type NOT IN ('Organization','Location')) "
        f"OR EXISTS(SELECT FROM {entities} b WHERE NOT EXISTS(SELECT FROM {evidence_relation} e "
        "WHERE (e.source_id,e.resource_type,e.resource_id)=(b.source_id,b.resource_type,b.resource_id)))",
        identity["dataset_id"],
    )
    _require(invalid is False, "source_evidence_incomplete")
    query = _NETWORK_INPUTS.format(witness=witness, evidence=evidence_relation) + _NETWORK_COMPLETENESS.format(
        plans=plans, networks=networks
    )
    _require(
        await connection.fetchval(query, identity["dataset_id"], _identity(identity)) is False,
        "network_evidence_incomplete",
    )


async def _canonical_authority(connection, policy, receipts):
    await verify_canonical_identity_authority(connection, _relations(policy, receipts), _canonical_schema(policy))


async def verify_canonical_identity_authority(connection, relations, canonical_schema):
    """Refuse opaque UUID reuse across accepted keys on explicit coordinates."""
    for table, column, root in (
        ("provider_directory_organization_identity", "organization_id", CMS_IDENTITY_TABLES[0]),
        ("provider_directory_site_identity", "site_id", CMS_IDENTITY_TABLES[0]),
        ("provider_directory_insurance_network_identity", "network_id", CMS_IDENTITY_TABLES[2]),
    ):
        keys = ",".join(storage_identifier(key) for key in CMS_IDENTITY_KEYS[root])
        field = storage_identifier(column)
        invalid = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {relations[root]} candidate "
            f"LEFT JOIN {storage_relation(canonical_schema, table)} canonical ON canonical.{field}=candidate.{field} "
            f"LEFT JOIN {storage_relation(canonical_schema, root)} incumbent USING({keys}) "
            f"WHERE candidate.{field} IS NOT NULL AND ((canonical.{field} IS NOT NULL "
            f"AND incumbent.{field} IS DISTINCT FROM candidate.{field}) "
            f"OR (incumbent.{field} IS NOT NULL AND canonical.{field} IS NULL))) OR EXISTS("
            f"SELECT candidate.{field} FROM {relations[root]} candidate "
            f"LEFT JOIN {storage_relation(canonical_schema, root)} incumbent USING({keys}) "
            f"WHERE candidate.{field} IS NOT NULL GROUP BY candidate.{field} "
            f"HAVING count(*)>1 AND bool_or(incumbent.{field} IS DISTINCT FROM candidate.{field}))"
        )
        if invalid:
            # A committed winner can appear after the preceding read-committed replay check.
            await verify_identity_replay(connection, relations, canonical_schema)
        _require(invalid is False, "canonical_identity_reuse_forbidden")


async def finish_identity_stages(publisher, identity, receipts):
    """Finish lookup indexes and verify the complete closed immutable family."""
    connection, policy = publisher.connection, publisher.policy
    _require(publisher is bound_publisher() and connection.is_in_transaction() is True, "publisher_changed")
    source_copy_session()
    await _protected_auxiliary(connection, policy, identity, receipts, ("loading",))
    for root, name in _names(receipts).items():
        for index, columns in enumerate(_SECONDARY[root]):
            fields = ",".join(storage_identifier(column) for column in columns)
            await connection.execute(
                f"CREATE INDEX {storage_identifier(name + '_lookup' + str(index))} "
                f"ON {storage_relation(policy.schema, name)} ({fields})"
            )
        await connection.execute(f"ANALYZE {storage_relation(policy.schema, name)}")
    await _source_coordinates(connection, policy, identity, receipts)
    await _global_replay(connection, policy, receipts)
    relations = _relations(policy, receipts)
    await verify_identity_relationships(connection, relations)
    await _complete_source_facts(connection, policy, identity, receipts)


async def verify_identity_relationships(connection, relations):
    """Check every private evidence/map edge without an ambient Source context."""
    binding_relation = relations[CMS_IDENTITY_TABLES[0]]
    evidence_relation = relations[CMS_IDENTITY_TABLES[1]]
    network_relation = relations[CMS_IDENTITY_TABLES[2]]
    plans = relations[CMS_IDENTITY_TABLES[3]]
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {evidence_relation} e LEFT JOIN {binding_relation} b USING(source_id,resource_type,resource_id) "
        "WHERE b.resource_id IS NULL) OR EXISTS("
        f"SELECT FROM {network_relation} n LEFT JOIN {binding_relation} b ON b.source_id=n.source_id "
        "AND b.resource_type='Organization' AND b.resource_id=n.resource_id WHERE b.organization_id IS NULL) OR EXISTS("
        f"SELECT FROM {plans} p LEFT JOIN {network_relation} n ON n.source_id=p.source_id AND n.resource_id=p.network_resource_id "
        f"LEFT JOIN {evidence_relation} e ON e.source_id=p.source_id AND e.resource_type=p.network_resource_type "
        "AND e.resource_id=p.network_resource_id AND e.release_id=p.release_id "
        "WHERE n.network_id IS NULL OR e.resource_id IS NULL)"
    )
    _require(invalid is False, "relationships_changed")


async def _publish_canonical_ids(connection, policy, relations, table, column, root):
    """Keep Source coordinate authentication for existing callers."""
    await _publish_canonical_ids_native(connection, _canonical_schema(policy), relations, table, column, root)


async def _publish_canonical_ids_native(connection, canonical_schema, relations, table, column, root):
    """A new source key must win its native canonical UUID insert in this statement."""
    field = storage_identifier(column)
    keys = ",".join(storage_identifier(key) for key in CMS_IDENTITY_KEYS[root])
    conflict = await connection.fetchval(
        f"WITH needed AS MATERIALIZED (SELECT DISTINCT candidate.{field},candidate.created_at "
        f"FROM {relations[root]} candidate LEFT JOIN {storage_relation(canonical_schema, root)} incumbent "
        f"USING({keys}) WHERE candidate.{field} IS NOT NULL AND incumbent.source_id IS NULL), "
        f"inserted AS (INSERT INTO {storage_relation(canonical_schema, table)} ({field},created_at) "
        f"SELECT {field},created_at FROM needed ORDER BY {field} ON CONFLICT ({field}) DO NOTHING RETURNING {field}) "
        # Every needed id must have been inserted; counting avoids a statistics-free join of two CTEs.
        f"SELECT (SELECT count(DISTINCT {field}) FROM needed)<>(SELECT count(*) FROM inserted)"
    )
    if conflict:
        raise CMSIdentityReprepareRequired("provider_directory_cms_identity_whole_candidate_reprepare_required")


async def publish_identity_relations(connection, relations, canonical_schema):
    """Merge four qualified private maps and their three UUID roots set-wise.

    This data operation grants no authority. Its caller authenticates the exact
    relations, original writer and custody/fence, and owns rollback and commit.
    """
    _require(connection.is_in_transaction() is True, "transaction_required")
    _require(set(relations) == set(CMS_IDENTITY_TABLES), "stages_changed")
    await verify_identity_replay(connection, relations, canonical_schema)
    await verify_canonical_identity_authority(connection, relations, canonical_schema)
    for table, column, root in (
        ("provider_directory_organization_identity", "organization_id", CMS_IDENTITY_TABLES[0]),
        ("provider_directory_site_identity", "site_id", CMS_IDENTITY_TABLES[0]),
        ("provider_directory_insurance_network_identity", "network_id", CMS_IDENTITY_TABLES[2]),
    ):
        await _publish_canonical_ids_native(connection, canonical_schema, relations, table, column, root)
    for root, relation in relations.items():
        fields = ",".join(storage_identifier(column) for column in CMS_IDENTITY_COLUMNS[root])
        keys = ",".join(storage_identifier(column) for column in CMS_IDENTITY_KEYS[root])
        await connection.execute(
            f"INSERT INTO {storage_relation(canonical_schema, root)} ({fields}) "
            f"SELECT {fields} FROM {relation} ORDER BY {keys} ON CONFLICT ({keys}) DO NOTHING"
        )
    from process.provider_directory_generic_source_storage import refresh_statistics

    await refresh_statistics(connection, canonical_schema, relations)
    await verify_identity_replay(connection, relations, canonical_schema, complete=True)


async def publish_identity_stages(publisher, identity, receipts):
    """One savepoint: native uniqueness plus exact revalidation, never remap UUIDs."""
    connection, policy = publisher.connection, publisher.policy
    _require(connection.is_in_transaction() is True and publisher is bound_publisher(), "publisher_changed")
    source_copy_session()
    relations = _relations(policy, receipts)
    canonical_schema = _canonical_schema(policy)
    try:
        async with connection.transaction():
            await _protected_auxiliary(connection, policy, identity, receipts, ("sealed",))
            await _source_coordinates(connection, policy, identity, receipts)
            await publish_identity_relations(connection, relations, canonical_schema)
    except asyncpg.SerializationError as error:
        raise CMSIdentityReprepareRequired(
            "provider_directory_cms_identity_whole_candidate_reprepare_required"
        ) from error
