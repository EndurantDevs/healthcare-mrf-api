# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare retention before Profile resumes; bind only metadata at native commit."""

import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from uuid import UUID

from sqlalchemy import text

from process import provider_directory_cms_serving_receipt as serving
from process.entity_address_result_generation import RELATION_NAMES
from process.entity_address_snapshot_receipt import _normalize_receipt_session
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import address_clone_identity, native_driver
from process.network_cms_registry_source_pair import (
    _CMS_FIELDS,
    _COMMENT,
    RegistryCMSPublicationProof,
    RegistryCMSRetainedSourcePair,
    _digest,
    _json,
    require_registry_cms_source_pair,
)
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_epoch import (
    _LOOKUP_INDEXES,
    _PRIMARY_KEYS,
    _TABLES,
    MAX_COPY_BATCH_BYTES,
    MAX_COPY_BATCH_ROWS,
    capture_retained_cms_fhir_source_epoch,
    require_retained_cms_fhir_source_epoch,
)
from process.network_registry_cms_prepared_address import prepare_retained_registry_address, prepared_address_stages
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_address import CMSAddressPreparation
from process.provider_directory_cms_preparation import (
    NonprofileAdmission,
    PreparedServingArtifacts,
    RetainedRawRelation,
    remaining_build_seconds,
    retained_raw_policy,
)
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.uhc_flex_practitioner_async_safety import drain_operation

_PREPARED_COMMENT = "registry-cms-prepared-pair-v1:"
_PREPARED_COMMENT_V2 = "registry-cms-prepared-pair-v2:"


@dataclass(frozen=True)
class RegistryCMSRetentionRequest:
    """Native producer inputs whose complete retention policy must be signed."""

    source_pin: PinnedFHIRMembershipSource
    binding_coordinates: RegistryNetworkSourceCoordinates
    selection_proof_id: str
    expected_admission_sha256: str
    expected_metadata_sha256: str
    extra_data_upper_bound_bytes: int
    extra_wal_upper_bound_bytes: int

    def __post_init__(self):
        RegistrySourceMembershipRecipe(self.source_pin, self.binding_coordinates)
        if self.source_pin.source_id != "cms-npd" or self.source_pin.retained_epoch is not None:
            raise ValueError("registry_cms_retention_request_invalid")
        for digest in (self.selection_proof_id, self.expected_admission_sha256, self.expected_metadata_sha256):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError("registry_cms_retention_request_invalid")
        for amount in (self.extra_data_upper_bound_bytes, self.extra_wal_upper_bound_bytes):
            if type(amount) is not int or not 0 < amount < 2**63:
                raise ValueError("registry_cms_retention_request_invalid")

    def policy(self, capture_id, owner_role, runtime_roles):
        """Return the closed policy embedded in the signed native address input."""
        return {
            "version": 1,
            "capture_id": str(capture_id),
            "source_pin": self.source_pin.coordinates,
            "binding_coordinates": asdict(self.binding_coordinates),
            "selection_proof_id": self.selection_proof_id,
            "expected_admission_sha256": self.expected_admission_sha256,
            "expected_metadata_sha256": self.expected_metadata_sha256,
            "raw_tables": list(_TABLES),
            "address_tables": list(RELATION_NAMES),
            "owner_role": owner_role,
            "runtime_roles": list(runtime_roles),
            "extra_data_upper_bound_bytes": self.extra_data_upper_bound_bytes,
            "extra_wal_upper_bound_bytes": self.extra_wal_upper_bound_bytes,
        }


@dataclass(frozen=True)
class PreparedRegistryCMSSourcePair:
    request: RegistryCMSRetentionRequest
    recipe: RegistrySourceMembershipRecipe
    address_ownership: object
    address_receipt: object
    address_catalog_sha256: str
    address_stages: tuple
    owner_role: str
    runtime_roles: tuple
    capacity_geometry_hash: str
    lease_digest: str
    native_address_input_hash: str
    source_attempt: RegistryCMSSourceAttempt | None = None

    def __post_init__(self):
        if self.source_attempt is not None and type(self.source_attempt) is not RegistryCMSSourceAttempt:
            raise ValueError("registry_cms_source_attempt_invalid")

    def as_dict(self):
        """Persist closed prepublication evidence, with no receipt or commit claim."""
        from process.registry_source_recipe_store import canonical_registry_source_recipes

        document_dict = {
            "request": asdict(self.request),
            "recipe": json.loads(canonical_registry_source_recipes((self.recipe,)))[0],
            "address_ownership": self.address_ownership.as_dict(),
            "address_receipt": self.address_receipt.as_dict(),
            "address_catalog_sha256": self.address_catalog_sha256,
            "address_stages": [list(stage) for stage in self.address_stages],
            "owner_role": self.owner_role,
            "runtime_roles": list(self.runtime_roles),
            "capacity_geometry_hash": self.capacity_geometry_hash,
            "lease_digest": self.lease_digest,
            "native_address_input_hash": self.native_address_input_hash,
        }
        if self.source_attempt is not None:
            document_dict["source_attempt"] = self.source_attempt.as_dict()
        return document_dict


async def _retention_admission(prepared, factory, request, capture_id, owner_role, runtime_roles):
    """Reject absent signed policy and post-resume WAL placement before any copy."""
    if type(prepared) is not PreparedServingArtifacts:
        raise ValueError("registry_cms_retention_admission_required")
    admission = prepared.nonprofile_admission
    if (
        type(prepared) is not PreparedServingArtifacts
        or type(factory) is not CMSAddressPreparation
        or type(admission) is not NonprofileAdmission
        or not admission._started
        or prepared.profile_bundle is not None
        or factory.fhir is not prepared.fhir
        or factory.execution is not prepared.execution
        or factory.input_hash != admission.plan.native_address_input_hash
        or request.selection_proof_id != admission.plan.selection_proof_id
        or request.selection_proof_id != prepared.execution.attestation.proof_id
        or json.loads(factory.input_json).get("registry_source_retention")
        != request.policy(capture_id, owner_role, runtime_roles)
        or retained_raw_policy(admission.lease) != request.policy(capture_id, owner_role, runtime_roles)
    ):
        raise ValueError("registry_cms_retention_admission_required")
    await admission.assert_ready(prepared.fhir, prepared.address.db_schema)
    relations = await admission.measure(prepared.fhir, prepared.address.db_schema)
    if (
        sum(relation.total_bytes for relation in relations) + request.extra_data_upper_bound_bytes
        > dict(admission.plan.reservation_bytes)["data"]
        or request.extra_wal_upper_bound_bytes > admission.plan.logging_wal_upper_bound_bytes
    ):
        raise ValueError("registry_cms_retention_reservation_exceeded")
    return sum(relation.total_bytes for relation in relations)


def _validate_preparation_request(request, capture_id, owner_role, runtime_roles):
    """Keep the signed request and immutable custody roles closed."""
    if (
        type(request) is not RegistryCMSRetentionRequest
        or type(capture_id) is not UUID
        or not capture_id.int
        or type(runtime_roles) is not tuple
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or not runtime_roles
        or owner_role in runtime_roles
    ):
        raise ValueError("registry_cms_retention_request_invalid")
    for role in (owner_role, *runtime_roles):
        _identifier(role)


@dataclass
class RegistryCMSRawEpochAdmission:
    """Adapt exact empty raw heaps to the existing nonprofile batch admission."""

    prepared: PreparedServingArtifacts
    request: RegistryCMSRetentionRequest
    schema_name: str
    connection: object
    before_bytes: int
    wal_start: str
    policy_json: str
    _copy_phase_by_table: dict[str, str] = field(default_factory=dict, init=False)
    _indexes_by_table: dict[str, tuple[str, ...]] = field(default_factory=dict, init=False)
    _next_table: int = field(default=0, init=False)
    _next_index: int = field(default=0, init=False)
    _is_index_pending: bool = field(default=False, init=False)

    @property
    def copy_batch_rows(self):
        """Keep raw row batches within the signed producer batch geometry."""
        rows = self.prepared.nonprofile_admission.plan.batch_size
        if type(rows) is not int or rows < 1:
            raise ValueError("registry_cms_retention_admission_required")
        return min(rows, MAX_COPY_BATCH_ROWS)

    @property
    def copy_batch_bytes(self):
        """Cap binary input independently of the unchanged physical reservations."""
        return MAX_COPY_BATCH_BYTES

    def _phase_indexes(self, phase, logical):
        """Accept only the creator's complete copy sequence and fixed index order."""
        indexes = self._indexes_by_table.get(logical, ())
        if phase in {"created", "before_insert", "before_copy_batch", "after_copy_batch", "after_insert"}:
            predecessors_by_phase = {
                "created": (None,),
                "before_insert": ("created",),
                "before_copy_batch": ("before_insert", "after_copy_batch"),
                "after_copy_batch": ("before_copy_batch",),
                "after_insert": ("before_insert", "after_copy_batch"),
            }
            if (
                self._next_table >= len(_TABLES)
                or _TABLES[self._next_table] != logical
                or self._copy_phase_by_table.get(logical) not in predecessors_by_phase[phase]
                or indexes
            ):
                raise ValueError("registry_cms_retention_copy_phase_invalid")
            self._copy_phase_by_table[logical] = phase
            if phase == "after_insert":
                self._next_table += 1
            return indexes
        index_steps = tuple(("cms_epoch_pk_" + str(index), table) for index, (table, _) in enumerate(_PRIMARY_KEYS))
        index_steps += tuple((name, table) for name, table, _ in _LOOKUP_INDEXES)
        if self._next_table != len(_TABLES) or self._next_index >= len(index_steps):
            raise ValueError("registry_cms_retention_copy_phase_invalid")
        name, expected_table = index_steps[self._next_index]
        if logical != expected_table or self._is_index_pending != (phase == "after_index"):
            raise ValueError("registry_cms_retention_copy_phase_invalid")
        self._is_index_pending = phase == "before_index"
        if phase == "after_index":
            indexes += (name,)
            self._indexes_by_table[logical] = indexes
            self._next_index += 1
        return indexes

    async def capture(self, capture_id, owner_role, runtime_roles):
        """Retain the exact admitted raw tables through this batch accounting."""
        return await capture_retained_cms_fhir_source_epoch(
            self.connection,
            self.request.source_pin,
            epoch_id=capture_id,
            owner_role=owner_role,
            runtime_roles=runtime_roles,
            expected_admission_sha256=self.request.expected_admission_sha256,
            expected_metadata_sha256=self.request.expected_metadata_sha256,
            copy_admission=self,
        )

    async def __call__(self, phase, schema, logical, oid):
        if (
            schema != self.schema_name
            or logical not in _TABLES
            or type(oid) is not int
            or not 0 < oid < 2**32
            or phase
            not in {
                "created",
                "before_insert",
                "before_copy_batch",
                "after_copy_batch",
                "after_insert",
                "before_index",
                "after_index",
            }
        ):
            raise ValueError("registry_cms_retention_admission_required")
        admission = self.prepared.nonprofile_admission
        raw_relation = RetainedRawRelation(schema, logical, oid, self.policy_json, self._phase_indexes(phase, logical))
        if phase == "created":
            await admission.register_external_relation(
                self.prepared.fhir, schema, logical, oid, raw_relation=raw_relation
            )
        else:
            await admission.assert_external_relation(
                self.prepared.fhir, schema, logical, oid, raw_relation=raw_relation
            )
        if phase in {"after_copy_batch", "after_insert", "after_index"}:
            await admission.assert_ready(self.prepared.fhir, schema)
        await _assert_retained_bounds(self.connection, self.prepared, self.request, self.before_bytes, self.wal_start)


async def _raw_copy_admission(prepared, request, connection, before_bytes):
    """Bind the copy callback to the already verified signed capture policy."""
    policy = retained_raw_policy(prepared.nonprofile_admission.lease)
    return RegistryCMSRawEpochAdmission(
        prepared,
        request,
        "registry_cms_epoch_" + UUID(policy["capture_id"]).hex,
        connection,
        before_bytes,
        await connection.fetchval("SELECT pg_current_wal_insert_lsn()::text"),
        json.dumps(policy, sort_keys=True, separators=(",", ":")),
    )


async def prepare_registry_cms_source_pair(
    prepared,
    address_factory,
    request,
    *,
    capture_id,
    owner_role,
    runtime_roles,
    source_session_factory=None,
    source_attempt=None,
):
    """Prepare closed native copies before the owner resumes its Profile WAL meter."""
    _validate_preparation_request(request, capture_id, owner_role, runtime_roles)
    before_bytes = await _retention_admission(prepared, address_factory, request, capture_id, owner_role, runtime_roles)
    if source_attempt is not None and (
        type(source_attempt) is not RegistryCMSSourceAttempt or source_attempt.run_id != address_factory.run_id
    ):
        raise ValueError("registry_cms_source_attempt_invalid")
    prepared_address_stages(prepared.address)
    previous_registered_relations = set(prepared.nonprofile_admission._external_relations)
    try:
        async with _retention_transaction(prepared.fhir, source_session_factory, capture_id=capture_id) as session:
            await _limit_retention_sql(session, prepared)
            await _normalize_receipt_session(session, request.source_pin.schema_name)
            connection = await native_driver(session)
            copy_admission = await _raw_copy_admission(prepared, request, connection, before_bytes)
            wal_start = copy_admission.wal_start
            epoch = await copy_admission.capture(capture_id, owner_role, runtime_roles)
            ownership, receipt, catalog, stages = await prepare_retained_registry_address(
                session, prepared, capture_id=capture_id, owner_role=owner_role, runtime_roles=runtime_roles
            )
            admission = prepared.nonprofile_admission
            retained = PreparedRegistryCMSSourcePair(
                request,
                RegistrySourceMembershipRecipe(
                    replace(request.source_pin, retained_epoch=epoch), request.binding_coordinates
                ),
                ownership,
                receipt,
                catalog,
                stages,
                owner_role,
                runtime_roles,
                admission.plan.capacity_geometry_hash,
                admission.lease.lease_digest,
                admission.plan.native_address_input_hash,
                source_attempt,
            )
            await _stamp_prepared(connection, retained)
            await admission.assert_ready(prepared.fhir, prepared.address.db_schema)
            await _assert_retained_bounds(connection, prepared, request, before_bytes, wal_start)
        return retained
    except BaseException:
        await drain_operation(_retire_rolled_back(prepared, previous_registered_relations), preserve_cancellation=True)
        raise


@asynccontextmanager
async def _retention_transaction(fhir, source_session_factory, *, capture_id=None):
    """Choose the source owner's pool before borrowing its native transaction."""
    if source_session_factory is None:
        async with fhir.db.transaction() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            yield session
        return
    if not callable(source_session_factory) or fhir.db._transaction_binding() is not None:
        raise ValueError("registry_cms_retention_requires_own_transaction")
    if capture_id is not None:
        from process.network_registry_cms_capture_lock import registry_cms_capture_transaction

        async with registry_cms_capture_transaction(source_session_factory, capture_id=capture_id) as session:
            async with fhir.db.bind_existing_session(session):
                yield session
        return
    async with source_session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        async with fhir.db.bind_existing_session(session):
            yield session


async def _limit_retention_sql(session, prepared):
    """Bound copy, content and index work by the existing signed native reservation."""
    admission = prepared.nonprofile_admission
    limit = admission.plan.temp_file_limit_bytes_per_backend
    if type(limit) is not int or limit <= 0 or limit % 1024:
        raise ValueError("registry_cms_retention_admission_required")
    timeout_ms = max(1, int(await remaining_build_seconds(prepared.fhir, admission) * 1000))
    for name, setting in (
        ("temp_file_limit", f"{limit // 1024}kB"),
        ("max_parallel_workers_per_gather", "0"),
        ("max_parallel_maintenance_workers", "0"),
        ("statement_timeout", f"{timeout_ms}ms"),
        ("lock_timeout", f"{timeout_ms}ms"),
    ):
        await session.execute(text("SELECT set_config(:name,:setting,true)"), {"name": name, "setting": setting})


async def _assert_retained_bounds(connection, prepared, request, before_bytes, wal_start):
    """Enforce the signed extra-copy budget on observed native bytes and WAL."""
    relations = await prepared.nonprofile_admission.measure(prepared.fhir, prepared.address.db_schema)
    wal_bytes = await connection.fetchval(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::text::pg_lsn)::bigint", wal_start
    )
    if (
        sum(relation.total_bytes for relation in relations) - before_bytes > request.extra_data_upper_bound_bytes
        or wal_bytes > request.extra_wal_upper_bound_bytes
    ):
        raise ValueError("registry_cms_retention_reservation_exceeded")


async def _retire_rolled_back(prepared, registered_before):
    """Retire only registered copy OIDs whose creating transaction rolled back."""
    admission = prepared.nonprofile_admission
    for schema, logical in sorted(admission._external_relations - registered_before):
        oid = admission._relations[(schema, logical)]
        await admission.retire_external_relation(prepared.fhir, schema, logical, oid)


async def _stamp_prepared(connection, prepared_pair):
    document = _json(prepared_pair.as_dict())
    if len(document.encode()) > 65536:
        raise ValueError("registry_cms_prepared_pair_bound_exceeded")
    await connection.execute(
        f"COMMENT ON SCHEMA {_identifier(prepared_pair.address_ownership.schema_name)} IS '"
        + (_prepared_comment_prefix(prepared_pair) + document).replace("'", "''")
        + "'"
    )


async def _require_prepared_pair(session, prepared_pair):
    """Validate only exact closed OIDs, indexes, custody and stamped policy."""
    if type(prepared_pair) is not PreparedRegistryCMSSourcePair:
        raise ValueError("registry_cms_prepared_pair_invalid")
    connection = await native_driver(session)
    await require_retained_cms_fhir_source_epoch(
        connection,
        prepared_pair.request.source_pin,
        prepared_pair.recipe.source_pin.retained_epoch,
        verify_content=False,
    )
    _receipt, catalog = await address_clone_identity(
        session,
        prepared_pair.address_ownership,
        prepared_pair.owner_role,
        prepared_pair.runtime_roles,
        verify_content=False,
    )
    comment = await connection.fetchval(
        "SELECT obj_description($1::oid,'pg_namespace')", prepared_pair.address_ownership.schema_oid
    )
    if catalog != prepared_pair.address_catalog_sha256 or comment != (
        _prepared_comment_prefix(prepared_pair) + _json(prepared_pair.as_dict())
    ):
        raise ValueError("registry_cms_prepared_pair_invalid")
    return connection


def _prepared_comment_prefix(prepared_pair):
    return _PREPARED_COMMENT if prepared_pair.source_attempt is None else _PREPARED_COMMENT_V2


async def _require_retained_authority(connection, prepared_pair, payload):
    """Bind native acquisition and binding metadata from the closed raw edition."""
    request = prepared_pair.request
    epoch = prepared_pair.recipe.source_pin.retained_epoch
    row = await connection.fetchrow(
        f"SELECT acquisition_root_run_id,publication_metadata_summary_json FROM {_identifier(epoch.schema_name)}.provider_directory_endpoint_dataset WHERE dataset_id=$1",
        request.source_pin.dataset_id,
    )
    metadata = row["publication_metadata_summary_json"] if row is not None else None
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    declaration_by_field = {
        **asdict(request.binding_coordinates),
        "source_key_kind": "organization_resource_id",
        "alias_scope": request.source_pin.alias_scope,
    }
    if (
        row is None
        or row["acquisition_root_run_id"] != payload["cms"]["acquisition_root_run_id"]
        or type(metadata) is not dict
        or metadata.get("network_bindings") != declaration_by_field
        or metadata.get("semantic_projection_as_of") != request.source_pin.as_of
    ):
        raise ValueError("registry_cms_prepared_receipt_invalid")


async def bind_prepared_registry_cms_source_pair(session, prepared_pair, *, receipt_id, receipt_payload):
    """Bind a closed preparation to the actual own-transaction native receipt."""
    connection = await _require_prepared_pair(session, prepared_pair)
    request = prepared_pair.request
    namespace = _identifier(request.source_pin.schema_name)
    receipt_row = await connection.fetchrow(
        f"SELECT payload::text,publication_xid::text,profile_generation_id FROM {namespace}.provider_directory_cms_serving_receipt WHERE receipt_id=$1 AND publication_xid=pg_current_xact_id()",
        receipt_id,
    )
    publication_document = serving.validate_receipt_payload(receipt_payload)
    if receipt_row is None or json.loads(receipt_row["payload"]) != publication_document:
        raise ValueError("registry_cms_prepared_receipt_invalid")
    await _require_retained_authority(connection, prepared_pair, publication_document)
    current = await serving.capture_native_dependencies(session, request.source_pin.schema_name, lock=True)
    expected_oid_by_logical = {logical: oid for logical, _stage, oid in prepared_pair.address_stages}
    if (
        publication_document["address"] != current["address"]
        or publication_document["profile"] != current["profile"]
        or publication_document["address"]["relation_oids"]
        != [expected_oid_by_logical[logical] for logical in RELATION_NAMES]
        or publication_document["selection"]["proof_id"] != request.selection_proof_id
    ):
        raise ValueError("registry_cms_prepared_receipt_invalid")
    proof = RegistryCMSPublicationProof(
        request.source_pin,
        request.binding_coordinates,
        receipt_id,
        tuple(publication_document["cms"][field] for field in _CMS_FIELDS),
        receipt_row["publication_xid"],
        receipt_row["profile_generation_id"],
        request.selection_proof_id,
        request.expected_admission_sha256,
        request.expected_metadata_sha256,
    )
    pair = RegistryCMSRetainedSourcePair(
        proof,
        prepared_pair.recipe,
        PinnedAddressSource(
            prepared_pair.address_ownership.schema_name,
            "entity_address_unified",
            _digest(publication_document["address"]),
        ),
        prepared_pair.address_ownership,
        prepared_pair.address_receipt,
        prepared_pair.address_catalog_sha256,
        _json(publication_document),
        prepared_pair.owner_role,
        prepared_pair.runtime_roles,
    )
    await connection.execute(
        f"COMMENT ON SCHEMA {_identifier(pair.address_ownership.schema_name)} IS '"
        + (_COMMENT + _json(pair.as_dict())).replace("'", "''")
        + "'"
    )
    await require_registry_cms_source_pair(session, pair, verify_content=False)
    return pair
