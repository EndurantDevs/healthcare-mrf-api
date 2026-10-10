# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare full directory artifacts and an independently admitted Profile delta."""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import importlib
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, AsyncIterator

from process.provider_directory_cms_native_layout import RetainedNativeSourceLayout
from process.provider_directory_profile_capacity_attestation import (
    VerifiedDatabaseCapacityLease,
    assert_database_capacity_lease_reservation,
)


def _digest(value: Any) -> str:
    """Hash the closed, deterministic nonprofile reservation identity."""
    document = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(b"provider-directory-nonprofile-admission-v1\0" + document.encode("ascii")).hexdigest()


def desired_fence_hash(fence: Any) -> str:
    """Bind immutable data, source contracts and expected current pointers."""
    fields = (
        "source_id",
        "endpoint_id",
        "dataset_id",
        "evidence_run_id",
        "dataset_hash",
        "status",
        "is_current",
        "expected_incumbent_dataset_id",
        "promote_on_cutover",
        "source_verification_contract_hash",
        "admission_id",
        "semantic_projection_as_of",
    )
    return _digest(
        [
            {
                **{name: getattr(dataset, name, None) for name in fields},
                "artifact_resources": sorted(dataset.artifact_resources),
            }
            for dataset in sorted(fence.datasets, key=lambda item: item.source_id)
        ]
    )


@dataclass(frozen=True)
class NonprofileAdmissionPlan:
    """Closed geometry which the separate verified lease must sign."""

    selection_proof_id: str
    desired_profile_as_of: str
    desired_fence_hash: str
    artifact_scope_projection_hash: str
    publish_targets: tuple[str, ...]
    resource_types: tuple[str, ...]
    batch_size: int
    worker_count: int
    reservation_bytes: tuple[tuple[str, int], ...]
    minimum_remaining_bytes: int
    required_build_seconds: int
    native_address_targets: tuple[str, ...] = ()
    native_address_input_hash: str = ""
    temp_file_limit_bytes_per_backend: int = 0
    logging_wal_upper_bound_bytes: int = 0
    cutover_wal_upper_bound_bytes: int = 0
    paired_profile_lease_digest: str = ""

    @property
    def capacity_geometry_hash(self) -> str:
        """Return the signature-bound identity, including all resource budgets."""
        return _digest(self.payload)

    @property
    def payload(self) -> dict[str, Any]:
        """Return closed JSON geometry for the existing signed receipt envelope."""
        return json.loads(
            json.dumps(
                {
                    "contract_id": "provider-directory-cms-nonprofile-capacity.v1",
                    "admission_purpose": "cms_nonprofile",
                    **asdict(self),
                }
            )
        )


@dataclass(frozen=True)
class OwnedRelation:
    """Physical relation identity measured before logging or publication."""

    schema: str
    relation: str
    oid: int
    total_bytes: int
    persistence: str


@dataclass(frozen=True)
class RetainedRawRelation:
    """Bind an original raw heap to its signed policy and exact index phase."""

    schema: str
    relation: str
    oid: int
    policy_json: str
    index_names: tuple[str, ...]


@dataclass(frozen=True)
class RetainedNativeRelation:
    """Bind an original retained clone to signed custody and immutable native source evidence."""

    schema: str
    relation: str
    oid: int
    policy_json: str
    source_layout: RetainedNativeSourceLayout


def retained_raw_policy(lease: VerifiedDatabaseCapacityLease) -> dict[str, Any]:
    """Require the exact retention policy inside the verified native lease."""
    from process.provider_directory_cms_capacity_contract import validated_registry_source_retention_policy

    try:
        if not isinstance(lease, VerifiedDatabaseCapacityLease):
            raise ValueError
        policy = lease.signing_preflight_guard["healthcare_request"]["cms_nonprofile_admission"][
            "registry_source_retention"
        ]
        return validated_registry_source_retention_policy(policy)
    except KeyError, TypeError, ValueError:
        raise RuntimeError("provider_directory_nonprofile_raw_policy_required") from None


@dataclass(frozen=True)
class NonprofileAdmissionCheck:
    """Input to the authoritative aggregate reservation and runtime recheck."""

    phase: str
    lease: VerifiedDatabaseCapacityLease
    plan: NonprofileAdmissionPlan
    relations: tuple[OwnedRelation, ...]
    logging_relations: tuple[tuple[str, str], ...] = ()
    raw_relations: tuple[RetainedRawRelation, ...] = ()
    native_relations: tuple[RetainedNativeRelation, ...] = ()


@dataclass(frozen=True)
class NonprofileAdmissionReceipt:
    """Confirmation bound to the consumed reservation and exact phase input."""

    phase: str
    lease_digest: str
    reservation_id: str
    capacity_geometry_hash: str
    relations: tuple[OwnedRelation, ...]
    logging_relations: tuple[tuple[str, str], ...] = ()


@dataclass
class NonprofileAdmission:
    """Separate signed admission; no Profile lease or unsigned fallback exists."""

    lease: VerifiedDatabaseCapacityLease
    plan: NonprofileAdmissionPlan
    check_phase: Callable[[NonprofileAdmissionCheck], Awaitable[NonprofileAdmissionReceipt]]
    _relations: dict[tuple[str, str], int] = field(default_factory=dict, init=False)
    _logged_relations: set[tuple[str, str]] = field(default_factory=set, init=False)
    _external_relations: set[tuple[str, str]] = field(default_factory=set, init=False)
    _raw_relations: dict[tuple[str, str], RetainedRawRelation] = field(default_factory=dict, init=False)
    _native_relations: dict[tuple[str, str], RetainedNativeRelation] = field(default_factory=dict, init=False)
    _started: bool = field(default=False, init=False)
    profile_admission: Any = field(default=None, init=False)
    pause_profile: Callable[..., Awaitable[Any]] | None = field(default=None, repr=False)
    resume_profile: Callable[..., Awaitable[Any]] | None = field(default=None, repr=False)
    paired_profile_lease: VerifiedDatabaseCapacityLease | None = field(default=None, repr=False)
    cutover_operation: Callable[[], AbstractAsyncContextManager[None]] | None = field(default=None, repr=False)
    check_cutover: Callable[[tuple[OwnedRelation, ...] | None], Awaitable[None]] | None = field(
        default=None, repr=False
    )
    _cutover_active: bool = field(default=False, init=False)
    cleanup_preserved: list[tuple[str, str]] = field(default_factory=list, init=False)
    registry_source_job: Any = field(default=None, init=False, repr=False, compare=False)

    def _assert_lease(self) -> None:
        """Require signature-bound geometry and existing storage reservation checks."""
        if not isinstance(self.lease, VerifiedDatabaseCapacityLease) or not callable(self.check_phase):
            raise RuntimeError("provider_directory_nonprofile_admission_required")
        if self.lease.capacity_geometry_hash != self.plan.capacity_geometry_hash:
            raise RuntimeError("provider_directory_nonprofile_geometry_changed")
        reservation_by_class = dict(self.plan.reservation_bytes)
        if len(reservation_by_class) != len(self.plan.reservation_bytes):
            raise RuntimeError("provider_directory_nonprofile_reservation_invalid")
        assert_database_capacity_lease_reservation(
            self.lease,
            required_bytes_by_storage_class=reservation_by_class,
            minimum_remaining_bytes=self.plan.minimum_remaining_bytes,
            required_build_seconds=self.plan.required_build_seconds,
        )

    async def _check(
        self,
        phase: str,
        relations: tuple[OwnedRelation, ...] = (),
        *,
        logging_relations: tuple[tuple[str, str], ...] = (),
    ) -> None:
        """Recheck runtime, consumed ownership and aggregate physical costs."""
        self._assert_lease()
        if sum(relation.total_bytes for relation in relations) > dict(self.plan.reservation_bytes)["data"]:
            raise RuntimeError("provider_directory_nonprofile_data_budget_exceeded")
        expected = NonprofileAdmissionReceipt(
            phase,
            self.lease.lease_digest,
            self.lease.reservation_id,
            self.plan.capacity_geometry_hash,
            relations,
            logging_relations,
        )
        receipt = await self.check_phase(
            NonprofileAdmissionCheck(
                phase,
                self.lease,
                self.plan,
                relations,
                logging_relations,
                tuple(self._raw_relations.values()),
                tuple(self._native_relations.values()),
            )
        )
        if not isinstance(receipt, NonprofileAdmissionReceipt) or receipt != expected:
            raise RuntimeError("provider_directory_nonprofile_phase_receipt_changed")

    async def before_scratch(
        self, execution: Any, fence: Any, projection: Any, targets: set[str], types: frozenset[str]
    ) -> None:
        """Consume and verify the separate reservation before any scratch DDL."""
        plan = self.plan
        if self._started or (
            plan.selection_proof_id != execution.attestation.proof_id
            or plan.desired_profile_as_of != execution.attestation.desired_profile_as_of
            or plan.desired_fence_hash != desired_fence_hash(fence)
            or plan.artifact_scope_projection_hash != projection.projection_hash
            or plan.publish_targets != tuple(sorted(targets))
            or plan.resource_types != tuple(sorted(types))
            or type(plan.batch_size) is not int
            or plan.batch_size < 1
            or type(plan.worker_count) is not int
            or plan.worker_count < 1
        ):
            raise RuntimeError("provider_directory_nonprofile_scope_changed")
        await self._check("pre_scratch")
        self._started = True

    async def measure(self, fhir: Any, schema: str, names: tuple[str, ...] = ()) -> tuple[OwnedRelation, ...]:
        """Measure exact owned names, rejecting OID replacement or disappearance."""
        if not self._started:
            raise RuntimeError("provider_directory_nonprofile_admission_not_started")
        for name in names:
            self._relations.setdefault((schema, name), 0)
        measured_relations = []
        for (relation_schema, name), previous_oid in sorted(self._relations.items()):
            row = await fhir.db.first(
                "SELECT oid::bigint AS oid, pg_total_relation_size(oid)::bigint AS total_bytes, "
                "relpersistence::text AS persistence FROM pg_class WHERE oid=to_regclass(:relation_ref);",
                relation_ref=fhir._unscoped_qt(relation_schema, name),
            )
            values = fhir._pagination_checkpoint_row_mapping(row) if row is not None else {}
            oid, total_bytes = values.get("oid"), values.get("total_bytes")
            if type(oid) is not int or oid <= 0 or (previous_oid and previous_oid != oid):
                raise RuntimeError("provider_directory_nonprofile_relation_changed")
            if type(total_bytes) is not int or total_bytes < 0 or values.get("persistence") not in {"u", "p"}:
                raise RuntimeError("provider_directory_nonprofile_relation_storage_invalid")
            self._relations[(relation_schema, name)] = oid
            measured_relations.append(OwnedRelation(relation_schema, name, oid, total_bytes, values["persistence"]))
        return tuple(measured_relations)

    async def before_logging(self, fhir: Any, schema: str, name: str) -> None:
        """Reserve aggregate logging/WAL costs before ALTER SET LOGGED."""
        relations = await self.measure(fhir, schema, (name,))
        relation = next(value for value in relations if (value.schema, value.relation) == (schema, name))
        if relation.persistence == "p":
            await self._check("readiness", relations)
        else:
            await self._check("pre_logging", relations, logging_relations=((schema, name),))
        self._logged_relations.add((schema, name))

    async def register_address_stages(
        self, fhir: Any, schema: str, stages: tuple[tuple[str, str, int], ...], *, input_hash: str
    ) -> None:
        """Account captured native OIDs before logging without taking cleanup ownership."""
        if (
            input_hash != self.plan.native_address_input_hash
            or not input_hash
            or (
                tuple(sorted(target_relation for target_relation, _name, _oid in stages))
                != self.plan.native_address_targets
                or len({name for _target, name, _oid in stages}) != len(stages)
            )
        ):
            raise RuntimeError("provider_directory_nonprofile_address_scope_changed")
        for _target, name, oid in stages:
            if type(oid) is not int or oid <= 0 or self._relations.get((schema, name), oid) != oid:
                raise RuntimeError("provider_directory_nonprofile_address_identity_invalid")
            self._relations[(schema, name)] = oid
            self._external_relations.add((schema, name))
        relations = await self.measure(fhir, schema)
        stage_names = {name for _target, name, _oid in stages}
        logging_relations = tuple(
            (relation.schema, relation.relation)
            for relation in relations
            if relation.schema == schema and relation.relation in stage_names and relation.persistence == "u"
        )
        self._logged_relations.update(
            (relation.schema, relation.relation)
            for relation in relations
            if relation.schema == schema and relation.relation in stage_names and relation.persistence == "p"
        )
        await self._check(
            "pre_logging" if logging_relations else "readiness", relations, logging_relations=logging_relations
        )

    def _record_raw_relation(self, schema: str, name: str, oid: int, raw_relation: RetainedRawRelation | None) -> None:
        """Keep signed raw annotations separate from ordinary native ownership records."""
        if raw_relation is None:
            if (schema, name) in self._raw_relations:
                raise RuntimeError("provider_directory_nonprofile_raw_identity_invalid")
            return
        if type(raw_relation) is not RetainedRawRelation or (
            raw_relation.schema,
            raw_relation.relation,
            raw_relation.oid,
        ) != (schema, name, oid):
            raise RuntimeError("provider_directory_nonprofile_raw_identity_invalid")
        previous = self._raw_relations.get((schema, name))
        if previous is not None and (previous.oid != oid or previous.policy_json != raw_relation.policy_json):
            raise RuntimeError("provider_directory_nonprofile_raw_identity_invalid")
        self._raw_relations[(schema, name)] = raw_relation

    async def register_external_relation(
        self,
        fhir: Any,
        schema: str,
        name: str,
        oid: int,
        *,
        raw_relation: RetainedRawRelation | None = None,
        native_relation: RetainedNativeRelation | None = None,
    ) -> None:
        """Capture a native CREATE's original OID while native cleanup retains ownership."""
        if type(oid) is not int or oid <= 0 or self._relations.get((schema, name), oid) != oid:
            raise RuntimeError("provider_directory_nonprofile_external_identity_invalid")
        await self._record_native_relation(fhir, schema, name, oid, native_relation, raw_relation)
        self._relations[(schema, name)] = oid
        self._external_relations.add((schema, name))
        self._record_raw_relation(schema, name, oid, raw_relation)
        await self._check("readiness", await self.measure(fhir, schema))

    async def _record_native_relation(self, fhir, schema, name, oid, native_relation, raw_relation):
        """Enroll strict source evidence once; later checks never consult a renamed source heap."""
        from process.provider_directory_cms_native_layout import _retained_source_model, capture_retained_native_source

        coordinate = (schema, name)
        previous = self._native_relations.get(coordinate)
        if native_relation is None:
            if previous is not None:
                raise RuntimeError("provider_directory_nonprofile_native_identity_invalid")
            return
        self._assert_lease()
        policy = retained_raw_policy(self.lease)
        if (
            type(native_relation) is not RetainedNativeRelation
            or (native_relation.schema, native_relation.relation, native_relation.oid) != (schema, name, oid)
            or raw_relation is not None
            or coordinate in self._raw_relations
            or native_relation.policy_json != json.dumps(policy, sort_keys=True, separators=(",", ":"))
            or policy["selection_proof_id"] != self.plan.selection_proof_id
            or schema != "entity_address_archive_" + uuid.UUID(policy["capture_id"]).hex
            or name not in policy["address_tables"]
            or type(native_relation.source_layout) is not RetainedNativeSourceLayout
        ):
            raise RuntimeError("provider_directory_nonprofile_native_identity_invalid")
        source_layout = native_relation.source_layout
        model = _retained_source_model(source_layout, self.plan.native_address_targets)
        if (
            model.__tablename__ != name
            or source_layout.database_oid != self.lease.database_oid
            or source_layout.oid == oid
        ):
            raise RuntimeError("provider_directory_nonprofile_native_identity_invalid")
        if previous is not None:
            if previous != native_relation:
                raise RuntimeError("provider_directory_nonprofile_native_identity_invalid")
            return
        source_coordinate = (source_layout.schema, source_layout.relation)
        if (
            source_coordinate not in self._external_relations
            or self._relations.get(source_coordinate) != source_layout.oid
            or any(entry.source_layout.oid == source_layout.oid for entry in self._native_relations.values())
        ):
            raise RuntimeError("provider_directory_nonprofile_native_identity_invalid")
        source_relations = await self.measure(fhir, source_layout.schema)
        source_relation = next(
            (entry for entry in source_relations if (entry.schema, entry.relation) == source_coordinate), None
        )
        if (
            source_relation is None
            or await capture_retained_native_source(fhir, source_relation, self.plan.native_address_targets)
            != source_layout
        ):
            raise RuntimeError("provider_directory_nonprofile_native_identity_invalid")
        self._native_relations[coordinate] = native_relation

    async def assert_external_relation(
        self,
        fhir: Any,
        schema: str,
        name: str,
        oid: int,
        *,
        raw_relation: RetainedRawRelation | None = None,
        native_relation: RetainedNativeRelation | None = None,
    ) -> None:
        """Recheck fresh aggregate availability before growth, without inventing its bound."""
        if (schema, name) not in self._external_relations or self._relations.get((schema, name)) != oid:
            raise RuntimeError("provider_directory_nonprofile_external_identity_invalid")
        await self._record_native_relation(fhir, schema, name, oid, native_relation, raw_relation)
        self._record_raw_relation(schema, name, oid, raw_relation)
        await self._check("readiness", await self.measure(fhir, schema))

    async def retire_external_relation(self, fhir: Any, schema: str, name: str, oid: int) -> None:
        """Retire only an exact externally owned OID after native verified removal."""
        if (schema, name) not in self._external_relations or self._relations.get((schema, name)) != oid:
            raise RuntimeError("provider_directory_nonprofile_external_identity_invalid")
        actual_oid = await fhir.db.scalar(
            "SELECT to_regclass(:relation_ref)::oid::bigint;", relation_ref=fhir._unscoped_qt(schema, name)
        )
        if actual_oid is not None:
            raise RuntimeError("provider_directory_nonprofile_relation_changed")
        del self._relations[(schema, name)]
        self._external_relations.remove((schema, name))
        self._raw_relations.pop((schema, name), None)
        self._native_relations.pop((schema, name), None)
        self._logged_relations.discard((schema, name))

    async def rename_external_relation(self, fhir: Any, schema: str, old_name: str, new_name: str, oid: int) -> None:
        """Follow a verified rename of the original OID, never a replacement identity."""
        if (schema, old_name) in self._raw_relations or (schema, old_name) in self._native_relations:
            raise RuntimeError("provider_directory_nonprofile_raw_identity_invalid")
        if (schema, new_name) in self._relations:
            raise RuntimeError("provider_directory_nonprofile_external_name_conflict")
        actual_oid = await fhir.db.scalar(
            "SELECT to_regclass(:relation_ref)::oid::bigint;", relation_ref=fhir._unscoped_qt(schema, new_name)
        )
        if actual_oid != oid:
            raise RuntimeError("provider_directory_nonprofile_relation_changed")
        is_logged = (schema, old_name) in self._logged_relations
        await self.retire_external_relation(fhir, schema, old_name, oid)
        self._relations[(schema, new_name)] = oid
        self._external_relations.add((schema, new_name))
        if is_logged:
            self._logged_relations.add((schema, new_name))
        await self._check("readiness", await self.measure(fhir, schema))

    async def assert_ready(self, fhir: Any, schema: str, *, cutover: bool = False) -> None:
        """Revalidate physical identities, reservations and runtime at cutover."""
        relations = await self.measure(fhir, schema)
        if any(
            relation.persistence != "p"
            for relation in relations
            if (relation.schema, relation.relation) in self._logged_relations
        ):
            raise RuntimeError("provider_directory_nonprofile_stage_not_logged")
        if cutover and self._cutover_active:
            self._assert_lease()
            await self.check_cutover(relations)
        else:
            await self._check("cutover" if cutover else "readiness", relations)

    @asynccontextmanager
    async def publication(self, fhir: Any, schema: str):
        """Authorize one atomic publication before it takes any swap locks."""
        if self._cutover_active or not callable(self.cutover_operation) or not callable(self.check_cutover):
            raise RuntimeError("provider_directory_nonprofile_cutover_authorization_required")
        async with self.cutover_operation():
            await self.assert_ready(fhir, schema, cutover=True)
            self._cutover_active = True
            try:
                yield
            finally:
                self._cutover_active = False

    async def assert_cutover_complete(self) -> None:
        """Check local consumption, measured WAL and expiry before the owner commits."""
        if not self._cutover_active or not callable(self.check_cutover):
            raise RuntimeError("provider_directory_nonprofile_cutover_authorization_required")
        self._assert_lease()
        await self.check_cutover(None)


_ACTIVE: contextvars.ContextVar[NonprofileAdmission | None] = contextvars.ContextVar(
    "provider_directory_nonprofile_admission", default=None
)


async def remaining_build_seconds(fhir: Any, admission: NonprofileAdmission) -> float:
    """Use the database clock and the earlier independently verified build deadline."""
    deadline = admission.lease.max_build_deadline
    if admission.paired_profile_lease is not None:
        deadline = min(deadline, admission.paired_profile_lease.max_build_deadline)
    # The established authority clock is rounded down to seconds.
    remaining = (deadline - await fhir._profile_capacity_preflight_clock()).total_seconds() - 1
    if remaining <= 0:
        raise RuntimeError("provider_directory_nonprofile_build_deadline_reached")
    return remaining


@asynccontextmanager
async def nonprofile_sql_transaction(fhir: Any, admission: NonprofileAdmission):
    """Clamp and verify the actual backend used by every admitted full-artifact statement."""
    native = importlib.import_module("process.entity_address_unified")

    limit = admission.plan.temp_file_limit_bytes_per_backend
    if type(limit) is not int or limit <= 0 or limit % 1024:
        raise RuntimeError("provider_directory_nonprofile_temp_limit_invalid")
    remaining = await remaining_build_seconds(fhir, admission)
    timeout_ms = max(1, int(remaining * 1000))
    settings = (
        ("temp_file_limit", f"{limit // 1024}kB"),
        ("max_parallel_workers_per_gather", "0"),
        ("max_parallel_maintenance_workers", "0"),
        ("statement_timeout", f"{timeout_ms}ms"),
        ("lock_timeout", f"{timeout_ms}ms"),
    )
    async with native.entity_address_tuned_transaction(
        fhir.db, settings, native._sql_literal, native.logger, temp_file_limit_bytes=limit
    ):
        valid = await fhir.db.scalar(
            "SELECT current_setting('temp_file_limit') <> '-1' "
            "AND pg_size_bytes(current_setting('temp_file_limit'))=:limit "
            "AND current_setting('max_parallel_workers_per_gather')::int=0 "
            "AND current_setting('max_parallel_maintenance_workers')::int=0 "
            "AND current_setting('statement_timeout')::interval <= (:timeout_ms * interval '1 millisecond') "
            "AND current_setting('statement_timeout')::interval > interval '0' "
            "AND current_setting('lock_timeout')::interval <= (:timeout_ms * interval '1 millisecond') "
            "AND current_setting('lock_timeout')::interval > interval '0'",
            limit=limit,
            timeout_ms=timeout_ms,
        )
        if valid is not True:
            raise RuntimeError("provider_directory_nonprofile_effective_sql_limits_changed")
        yield


@asynccontextmanager
async def active_nonprofile_sql_transaction(fhir: Any):
    """Bound a complete CMS worker query group; preserve ordinary import behavior."""
    admission = _ACTIVE.get()
    if admission is None or fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None:
        yield
    else:
        async with nonprofile_sql_transaction(fhir, admission):
            yield


async def execute_nonprofile_status(fhir: Any, statement: str, **params: Any):
    """Reuse ordinary SQL outside CMS; constrain each admitted worker's own transaction."""
    admission = _ACTIVE.get()
    if admission is None:
        return await fhir.db.status(statement, **params)
    async with nonprofile_sql_transaction(fhir, admission):
        return await fhir.db.status(statement, **params)


async def check_stage_logging(fhir: Any, schema: str, stage_table: str) -> None:
    """Leave existing builders unchanged outside explicitly admitted preparation."""
    admission = _ACTIVE.get()
    if admission is not None and fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None:
        await admission.before_logging(fhir, schema, stage_table)


async def capture_nonprofile_stage(fhir: Any, schema: str, stage_table: str) -> None:
    """Capture the CREATE's original OID while its bounded transaction still owns it."""
    admission = _ACTIVE.get()
    if admission is None or fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None:
        return
    if fhir.db._transaction_binding() is None:
        raise RuntimeError("provider_directory_nonprofile_stage_capture_requires_transaction")
    await admission.measure(fhir, schema, (stage_table,))


async def is_stage_cleanup_handled(fhir: Any, stage: Any) -> bool:
    """Handle admitted cleanup internally; preserve absent or replaced identities."""
    admission = _ACTIVE.get()
    if admission is None or fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None:
        return False
    await _drop_owned_relation(fhir, admission, stage.schema, stage.stage_table)
    return True


async def _drop_owned_relation(fhir: Any, admission: NonprofileAdmission, schema: str, name: str) -> None:
    """Lock and recheck ownership before dropping any exact relation name."""
    expected_oid = admission._relations.get((schema, name))
    relation_ref = fhir._unscoped_qt(schema, name)
    async with fhir.db.transaction():
        actual_oid = await fhir.db.scalar("SELECT to_regclass(:relation_ref)::oid::bigint;", relation_ref=relation_ref)
        if actual_oid is None:
            return
        if not expected_oid or actual_oid != expected_oid:
            if (schema, name) not in admission.cleanup_preserved:
                admission.cleanup_preserved.append((schema, name))
            return
        await fhir.db.status(f"LOCK TABLE {relation_ref} IN ACCESS EXCLUSIVE MODE NOWAIT;")
        actual_oid = await fhir.db.scalar("SELECT to_regclass(:relation_ref)::oid::bigint;", relation_ref=relation_ref)
        if actual_oid != expected_oid:
            if (schema, name) not in admission.cleanup_preserved:
                admission.cleanup_preserved.append((schema, name))
            return
        await fhir.db.status(f"DROP TABLE {relation_ref};")


async def _drain_owned_cleanup(fhir: Any, admission: NonprofileAdmission, schema: str, names: list[str]) -> None:
    """Finish identity-checked scratch cleanup before propagating cancellation."""

    async def cleanup():
        """Drop only captured owned scratch identities."""
        for name in reversed(names):
            await _drop_owned_relation(fhir, admission, schema, name)

    task = asyncio.create_task(cleanup())
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    task.result()
    if cancellation is not None:
        raise cancellation


async def _create_owned_layout(fhir: Any, admission: NonprofileAdmission, schema: str, model: Any, name: str) -> None:
    """Create an empty heap and capture its OID before loading any payload."""
    async with nonprofile_sql_transaction(fhir, admission):
        await fhir.db.status(fhir._provider_directory_artifact_scope_table_sql(model, schema, name))
        await admission.measure(fhir, schema, (name,))


async def _index_owned_layout(fhir: Any, admission: NonprofileAdmission, schema: str, model: Any, name: str) -> None:
    """Bulk-build the existing scope indexes after loading and before exposing the scope."""

    async def execute_status(statement: str):
        """Account index growth while preserving the captured heap identity."""
        async with nonprofile_sql_transaction(fhir, admission):
            result = await fhir.db.status(statement)
        await admission.measure(fhir, schema, (name,))
        return result

    await fhir._build_artifact_scope_pk(model, schema, name, status_executor=execute_status)
    if model.__tablename__ in {
        fhir.ProviderDirectoryPractitionerRole.__tablename__,
        fhir.ProviderDirectoryOrganizationAffiliation.__tablename__,
    }:
        _index_name, statement = fhir._provider_directory_profile_bucket_index_sql(schema, name)
        await execute_status(statement)


def _scope_plan(fhir: Any) -> Any:
    """Use a unique scratch family outside generic Profile recovery/reaping."""
    owner = uuid.uuid4().hex
    names_by_table = {
        model.__tablename__: "cms_directory_scope_"
        + owner
        + "_"
        + hashlib.sha256(model.__tablename__.encode("ascii")).hexdigest()[:8]
        for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)
    }
    if len(set(names_by_table.values())) != len(names_by_table):
        raise RuntimeError("provider_directory_nonprofile_scope_name_collision")
    return fhir._ArtifactScopeMaterializationPlan(
        source_table=names_by_table[fhir.ProviderDirectorySource.__tablename__],
        created_tables=[],
        relation_by_table=names_by_table,
        resource_scope_jobs=tuple((model, names_by_table[model.__tablename__]) for model in fhir.RESOURCE_MODELS),
        model_by_table_name={
            model.__tablename__: model for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)
        },
    )


@asynccontextmanager
async def _full_scope(fhir: Any, fence: Any, types: frozenset[str], projection: Any, admission: NonprofileAdmission):
    """Keep full typed inputs alive without entering the Profile recovery guard."""
    schema, plan = fhir._schema(), _scope_plan(fhir)
    if await fhir._artifact_scope_relation_identities(schema, plan.relation_by_table.values()):
        raise RuntimeError("provider_directory_nonprofile_scope_already_exists")
    try:
        for base_name, name in sorted(plan.relation_by_table.items()):
            plan.created_tables.append(name)
            await _create_owned_layout(fhir, admission, schema, plan.model_by_table_name[base_name], name)
        await fhir._materialize_artifact_scope_payload(
            schema,
            plan,
            fence,
            fence,
            types,
            projection,
            admission.plan.batch_size,
            admission.plan.worker_count,
        )
        for base_name, name in sorted(plan.relation_by_table.items()):
            await _index_owned_layout(fhir, admission, schema, plan.model_by_table_name[base_name], name)
        await admission.measure(fhir, schema, tuple(plan.relation_by_table.values()))
        await admission.assert_ready(fhir, schema)
        oid_by_base = {base: admission._relations[(schema, name)] for base, name in plan.relation_by_table.items()}
        with fhir._artifact_scope_tokens(fence, fence, plan.relation_by_table, oid_by_base):
            yield dict(plan.relation_by_table)
    except BaseException as error:
        try:
            await _drain_owned_cleanup(
                fhir, admission, schema, _owned_cleanup_names(admission, schema, plan.created_tables)
            )
        except BaseException as cleanup_error:
            raise BaseExceptionGroup(
                "provider_directory_nonprofile_scope_and_cleanup_failed", [error, cleanup_error]
            ) from error
        raise
    else:
        await _drain_owned_cleanup(
            fhir, admission, schema, _owned_cleanup_names(admission, schema, plan.created_tables)
        )


def _owned_cleanup_names(admission: NonprofileAdmission, schema: str, created_tables: list[str]) -> list[str]:
    """Include captured stages whose builder failed before bundle registration."""
    return sorted(
        set(created_tables)
        | {
            name
            for relation_schema, name in admission._relations
            if relation_schema == schema and (relation_schema, name) not in admission._external_relations
        }
    )


@dataclass
class PreparedServingArtifacts:
    """Both live scopes; the common publication owner controls their commit."""

    fhir: Any
    fence: Any
    execution: Any
    nonprofile_admission: NonprofileAdmission | None
    nonprofile_bundle: Any
    profile_bundle: Any
    metrics: dict[str, Any]
    relation_overrides: dict[str, str]
    overlay_identity: OwnedRelation | None
    address: Any = None
    source_session_factory: Callable[[], Any] | None = field(default=None, repr=False, compare=False)
    registry_source_pair: Any = field(default=None, init=False, repr=False, compare=False)

    @property
    def stages(self) -> tuple[Any, ...]:
        """Return every ordinary stage ready for the common transaction."""
        full_stages = tuple(self.nonprofile_bundle.stages) if self.nonprofile_bundle is not None else ()
        return full_stages + tuple(self.profile_bundle.stages)

    @property
    def profile_delta(self) -> Any:
        """Return the existing exact source-delta preparation unchanged."""
        return self.profile_bundle.profile_delta

    @property
    def archive_delta(self) -> Any:
        """Carry the exact sealed archive prepared by the full bundle owner."""
        return getattr(self.nonprofile_bundle, "archive_delta", None)

    async def assert_ready(self, *, cutover: bool = False, archive_applied: bool = False) -> None:
        """Require the separate nonprofile reservation at final publication."""
        if self.nonprofile_admission is not None:
            await self.nonprofile_admission.assert_ready(self.fhir, self.fhir._schema(), cutover=cutover)
        if self.archive_delta is not None:
            async with nonprofile_sql_transaction(self.fhir, self.nonprofile_admission):
                if archive_applied:
                    await self.archive_delta.assert_applied_backend(self.fhir.db)
                else:
                    await self.archive_delta.assert_read_identity(
                        self.fhir, self.fhir.db._transaction_binding().session
                    )

    async def mark_committed(self, *, profile_result: Mapping[str, Any]) -> None:
        """Consume stages using the owner's verified immutable historical result."""
        if self.fhir.db._transaction_binding() is not None:
            raise RuntimeError("provider_directory_artifact_bundle_commit_pending")
        _assert_committed_profile(self.execution, self.profile_delta, profile_result)
        profile_metrics = self.metrics.get("profile")
        if not isinstance(profile_metrics, dict):
            raise RuntimeError("provider_directory_profile_metrics_missing")
        if self.nonprofile_bundle is not None:
            await self.nonprofile_bundle.mark_promoted()
        await self.profile_bundle.mark_promoted()
        for name in ("evidence_inserted", "evidence_deleted", "profile_inserted", "profile_deleted"):
            profile_metrics.pop(name, None)
        profile_metrics.update(profile_result)
        profile_metrics["selected_evidence_rows"] = profile_result["evidence_rows"]


def _assert_committed_profile(execution: Any, delta: Any, result: Mapping[str, Any]) -> None:
    """Check the historical result against the exact prepared selection identity."""
    attestation = execution.attestation
    if not isinstance(result, Mapping) or (
        result.get("selection_proof_id") != attestation.proof_id
        or result.get("operation") != attestation.operation
        or (delta is not None and result.get("generation_id") != delta.generation_id)
        or (
            attestation.desired_profile_as_of is not None
            and result.get("profile_as_of") != attestation.desired_profile_as_of
        )
        or result.get("status") != ("purged" if attestation.operation == "purge" else "published")
        or any(type(result.get(name)) is not int or result[name] < 0 for name in ("evidence_rows", "profile_rows"))
    ):
        raise RuntimeError("provider_directory_profile_committed_result_changed")


@asynccontextmanager
async def prepare_serving_artifacts(
    fhir: Any,
    execution: Any,
    fence: Any,
    **options: Any,
) -> AsyncIterator[PreparedServingArtifacts]:
    """Bound the complete paired preparation and cutover, including native workers."""
    admission = options.get("nonprofile_admission")
    if admission is not None and not isinstance(admission, NonprofileAdmission):
        raise RuntimeError("provider_directory_nonprofile_admission_required")
    if admission is None:
        async with _prepare_serving_artifacts(fhir, execution, fence, **options) as prepared:
            yield prepared
        return
    async with asyncio.timeout(await remaining_build_seconds(fhir, admission)):
        async with _prepare_serving_artifacts(fhir, execution, fence, **options) as prepared:
            yield prepared


@asynccontextmanager
async def _prepare_serving_artifacts(
    fhir: Any,
    execution: Any,
    fence: Any,
    *,
    run_id: str,
    control_run_id: str | None,
    metrics: dict[str, Any],
    nonprofile_admission: NonprofileAdmission | None = None,
    address_preparation: Callable[..., Any] | None = None,
) -> AsyncIterator[PreparedServingArtifacts]:
    """Prepare full nonprofile serving data and the separately signed Profile delta."""
    if execution.attestation.operation == "purge":
        async with _prepare_purge(fhir, execution, fence, run_id, control_run_id, metrics) as prepared:
            yield prepared
        return
    publish_targets, types, projection = await _admit_full_preparation(
        fhir, execution, fence, run_id, control_run_id, nonprofile_admission, address_preparation
    )
    execution_token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    capacity_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    admission_token = _ACTIVE.set(nonprofile_admission)
    try:
        async with _full_scope(fhir, fence, types, projection, nonprofile_admission) as overrides:
            async with fhir._provider_directory_artifact_bundle_scope() as full_bundle:
                full_metrics = await _prepare_full_bundle(fhir, fence, run_id, metrics, publish_targets, full_bundle)
                serving_override_by_relation = {**overrides, **full_bundle.relation_overrides}
                overlay = await _overlay_identity(fhir, full_bundle, nonprofile_admission)
                await nonprofile_admission.assert_ready(fhir, fhir._schema())
                prepared = PreparedServingArtifacts(
                    fhir,
                    fence,
                    execution,
                    nonprofile_admission,
                    full_bundle,
                    None,
                    full_metrics,
                    serving_override_by_relation,
                    overlay,
                )
                async with _prepare_address_and_profile(
                    prepared,
                    run_id=run_id,
                    control_run_id=control_run_id,
                    metrics=metrics,
                    address_preparation=address_preparation,
                ):
                    yield prepared
    finally:
        _ACTIVE.reset(admission_token)
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)


async def _admit_full_preparation(
    fhir, execution, fence, run_id, control_run_id, nonprofile_admission, address_preparation
):
    """Validate the desired workload and consume both reservations before any scratch."""
    if execution.attestation.operation != "publish":
        raise RuntimeError("provider_directory_nonprofile_operation_invalid")
    if not isinstance(nonprofile_admission, NonprofileAdmission):
        raise RuntimeError("provider_directory_nonprofile_admission_required")
    if not callable(address_preparation):
        raise RuntimeError("provider_directory_nonprofile_address_preparation_required")
    if getattr(execution.attestation, "desired_cms_dataset", None) is None or not run_id:
        raise RuntimeError("provider_directory_nonprofile_desired_selection_required")
    if fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None or _ACTIVE.get() is not None:
        raise RuntimeError("provider_directory_nonprofile_admission_already_active")
    fhir._assert_profile_selection_matches_artifact_fence(execution, fence)
    publish_targets = set(fhir.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS) - {"profile", "corroboration"}
    types = fhir._provider_directory_artifact_resource_types(publish_targets, publish_corroboration=False)
    async with nonprofile_sql_transaction(fhir, nonprofile_admission):
        projection = await fhir._provider_directory_artifact_scope_exact_projection(
            fhir._schema(), fence, types, source_fence=fence, batch_size=nonprofile_admission.plan.batch_size
        )
    fhir._assert_provider_directory_artifact_scope_exact_capacity(projection)
    await nonprofile_admission.before_scratch(execution, fence, projection, publish_targets, types)
    await _admit_profile_before_full_build(fhir, execution, fence, run_id, control_run_id, nonprofile_admission)
    return publish_targets, types, projection


@asynccontextmanager
async def _prepare_address_and_profile(
    prepared: PreparedServingArtifacts,
    *,
    run_id: str,
    control_run_id: str | None,
    metrics: dict[str, Any],
    address_preparation: Callable[..., Any],
):
    """Finish address logging before resuming the unchanged Profile workload."""
    admission = prepared.nonprofile_admission
    archive_options = {"archive": prepared.archive_delta} if prepared.archive_delta is not None else {}
    async with address_preparation(
        prepared.fence, prepared.relation_overrides, prepared.overlay_identity, admission, **archive_options
    ) as address:
        if address is None:
            raise RuntimeError("provider_directory_nonprofile_address_preparation_incomplete")
        prepared.address = address
        await _prepare_registry_source_pair(prepared, address_preparation)
        async with _profile_scope(
            prepared.fhir, prepared.execution, prepared.fence, run_id, control_run_id, metrics, admission
        ) as pair:
            prepared.profile_bundle, profile_metrics = pair
            prepared.metrics["profile"] = profile_metrics["profile"]
            await prepared.assert_ready()
            metrics.update(prepared.metrics)
            await _emit_prepared_manifest(prepared, run_id, control_run_id)
            yield


async def _prepare_registry_source_pair(prepared, address_preparation):
    """Retain the complete address and raw source before Profile WAL resumes."""
    from process.network_registry_cms_prepared_pair import prepare_registry_cms_source_pair
    from process.provider_directory_cms_source_runtime import BoundCMSRegistrySourceJob

    job = getattr(prepared.nonprofile_admission, "registry_source_job", None)
    if job is None:
        return
    if type(job) is not BoundCMSRegistrySourceJob:
        raise RuntimeError("cms_registry_source_runtime_invalid")
    prepared.source_session_factory = job.source_session_factory
    prepared.registry_source_pair = await prepare_registry_cms_source_pair(
        prepared,
        address_preparation,
        job.retention_request,
        capture_id=job.capture_id,
        owner_role=job.owner_role,
        runtime_roles=job.runtime_roles,
        source_session_factory=job.source_session_factory,
        source_attempt=job.source_attempt,
    )


def _manifest_required_indexes(fhir, target, name):
    """Reuse the serving builders' required index names without parsing their SQL."""
    if target == fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE:
        return [
            fhir._address_overlay_index_name(name, suffix)
            for suffix in fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_INDEX_SUFFIXES
        ]
    if target == fhir.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE:
        return [
            fhir._network_catalog_index_name(name, suffix)
            for suffix in fhir.PROVIDER_DIRECTORY_NETWORK_CATALOG_INDEX_SUFFIXES
        ]
    profile = fhir.profile_artifact
    if target not in {profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE}:
        raise RuntimeError("provider_directory_prepared_manifest_target_invalid")
    suffixes = (
        profile.PROFILE_EVIDENCE_INDEX_SUFFIXES
        if target == profile.PROFILE_EVIDENCE_TABLE
        else profile.PROFILE_INDEX_SUFFIXES
    )
    return [profile.profile_index_name(name, suffix) for suffix in suffixes]


def _manifest_relation(relations, schema, name, *, target, role, required_indexes=(), primary_key=False, oid=None):
    """Bind a prepared role to its original owned heap, never a name-only discovery."""
    relation = relations.get((schema, name))
    if relation is None or (oid is not None and relation["oid"] != oid) or "role" in relation:
        raise RuntimeError("provider_directory_prepared_manifest_ownership_changed")
    relation.update(
        target=target, role=role, required_indexes=sorted(required_indexes), primary_key_required=primary_key
    )


async def _manifest_profile_relations(prepared, relations, run_id):
    """Retain the separately admitted Profile ownership instead of charging it to CMS."""
    fhir, bundle = prepared.fhir, prepared.profile_bundle
    delta = prepared.profile_delta
    if delta is not None:
        if delta.owner_run_id != run_id or delta.selection_proof_id != prepared.execution.attestation.proof_id:
            raise RuntimeError("provider_directory_prepared_manifest_profile_changed")
        stages = [
            (getattr(delta, field), getattr(delta, field + "_oid"), target_relation)
            for field, target_relation in (
                ("evidence_stage", fhir.profile_artifact.PROFILE_EVIDENCE_TABLE),
                ("profile_stage", fhir.profile_artifact.PROFILE_TABLE),
                ("affected_npi_stage", None),
            )
        ]
        schema = delta.schema
    else:
        build = fhir.profile_initial.build_from_stages(fhir, bundle.stages)
        if (
            build is None
            or build.owner_run_id != run_id
            or build.selection_proof_id != prepared.execution.attestation.proof_id
        ):
            raise RuntimeError("provider_directory_prepared_manifest_profile_changed")
        # Preparation already verified the ready checkpoint; the immutable initial
        # receipt will bind these observed replacement OIDs after the guarded swap.
        schema = build.schema
        identities = await fhir._artifact_scope_relation_identities(
            schema, [stage.stage_table for stage in bundle.stages]
        )
        stages = [
            (stage.stage_table, identities.get(stage.stage_table, (None,))[0], stage.target_relation)
            for stage in bundle.stages
        ]
    for name, oid, target_relation in stages:
        if type(oid) is not int or oid <= 0 or (schema, name) in relations:
            raise RuntimeError("provider_directory_prepared_manifest_ownership_changed")
        relations[(schema, name)] = {"schema": schema, "relation": name, "oid": oid}
        required = _manifest_required_indexes(fhir, target_relation, name) if target_relation is not None else ()
        _manifest_relation(
            relations, schema, name, target=target_relation, role="profile", required_indexes=required, primary_key=True
        )


async def _prepared_manifest_relations(prepared, run_id):
    """Inventory the complete prepared scopes using only their actual owner objects."""
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    relation_by_coordinate = {
        (relation.schema, relation.relation): asdict(relation)
        for relation in await admission.measure(fhir, fhir._schema())
    }
    for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS):
        name = prepared.relation_overrides.get(model.__tablename__)
        if name is None:
            raise RuntimeError("provider_directory_prepared_manifest_ownership_changed")
        required_indexes = [fhir._artifact_scope_pk_names(name)[1]] if model.__table__.primary_key.columns else []
        if model in (fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation):
            required_indexes.append(fhir._provider_directory_profile_bucket_index_sql(fhir._schema(), name)[0])
        _manifest_relation(
            relation_by_coordinate,
            fhir._schema(),
            name,
            target=model.__tablename__,
            role="resource_scope",
            required_indexes=required_indexes,
        )
    for stage in prepared.nonprofile_bundle.stages:
        _manifest_relation(
            relation_by_coordinate,
            stage.schema,
            stage.stage_table,
            target=stage.target_relation,
            role="serving_stage",
            required_indexes=_manifest_required_indexes(fhir, stage.target_relation, stage.stage_table),
        )
    address = prepared.address
    native = importlib.import_module("process.entity_address_unified")
    by_stage = {swap.stage_cls.__tablename__: swap.stage_cls for swap in address.swaps}
    if {target_relation for target_relation, _name, _oid in address.stage_oids} != set(
        admission.plan.native_address_targets
    ):
        raise RuntimeError("provider_directory_prepared_manifest_native_changed")
    for target_relation, name, oid in address.stage_oids:
        required_indexes = [
            native._stage_index_name(name, index.get("name", "_".join(index["index_elements"])))
            for index in by_stage[name].__my_additional_indexes__
        ]
        _manifest_relation(
            relation_by_coordinate,
            address.db_schema,
            name,
            target=target_relation,
            role="native_stage",
            required_indexes=required_indexes,
            oid=oid,
        )
    await _manifest_profile_relations(prepared, relation_by_coordinate, run_id)
    for relation in relation_by_coordinate.values():
        if "role" not in relation:
            relation.update(role="nonprofile_scratch", target=None, required_indexes=[], primary_key_required=False)
    return [relation_by_coordinate[key] for key in sorted(relation_by_coordinate)]


async def _prepared_manifest_catalog(fhir, relations):
    """Read bounded metadata only, refusing missing heaps or unfinished required indexes."""
    if not 1 <= len(relations) <= 64 or len({relation["oid"] for relation in relations}) != len(relations):
        raise RuntimeError("provider_directory_prepared_manifest_inventory_invalid")
    for schema in sorted({relation["schema"] for relation in relations}):
        selected_relations = [relation for relation in relations if relation["schema"] == schema]
        identities = await fhir._artifact_scope_relation_identities(
            schema, [relation["relation"] for relation in selected_relations]
        )
        for relation in selected_relations:
            identity = identities.get(relation["relation"])
            if identity is None or identity[:2] != (relation["oid"], "r") or identity[2] not in {"u", "p"}:
                raise RuntimeError("provider_directory_prepared_manifest_ownership_changed")
            if relation.get("role") in {"profile", "serving_stage", "native_stage"} and identity[2] != "p":
                raise RuntimeError("provider_directory_prepared_manifest_stage_not_logged")
            relation["persistence"] = identity[2]
    attributes, indexes, _constraints, _triggers = await fhir._profile_capacity_relation_catalog(
        [relation["oid"] for relation in relations]
    )
    for relation in relations:
        relation["columns"] = [
            {"number": catalog_row["attnum"], "name": catalog_row["attname"]}
            for catalog_row in attributes
            if catalog_row["relation_oid"] == relation["oid"]
        ]
        relation["indexes"] = [catalog_row for catalog_row in indexes if catalog_row["relation_oid"] == relation["oid"]]
        required_indexes = set(relation.get("required_indexes", ()))
        if (
            not relation["columns"]
            or not required_indexes <= {catalog_row["index_name"] for catalog_row in relation["indexes"]}
            or (
                relation.get("primary_key_required")
                and not any(catalog_row["indisprimary"] for catalog_row in relation["indexes"])
            )
            or any(
                not all(catalog_row[field] is True for field in ("indisvalid", "indisready", "indislive"))
                for catalog_row in relation["indexes"]
            )
        ):
            raise RuntimeError("provider_directory_prepared_manifest_indexes_incomplete")


async def _emit_prepared_manifest(prepared, run_id, control_run_id):
    """Retain the complete pre-swap catalog witness in the existing private worker log."""
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    if fhir.db._transaction_binding() is not None:
        raise RuntimeError("provider_directory_prepared_manifest_requires_no_transaction")
    async with nonprofile_sql_transaction(fhir, admission):
        relations = await _prepared_manifest_relations(prepared, run_id)
        await _prepared_manifest_catalog(fhir, relations)
    selection = prepared.execution.attestation
    manifest_by_field = {
        "contract_id": "provider-directory-cms-prepared-layout-v1",
        "run_id": run_id,
        "control_run_id": control_run_id,
        "observed_at": (await fhir._profile_capacity_preflight_clock()).isoformat(),
        "selection_proof_id": selection.proof_id,
        "selection_fingerprint": selection.selection_fingerprint,
        "desired_fence_hash": desired_fence_hash(prepared.fence),
        "source_vector": sorted((dataset.source_id, dataset.dataset_id) for dataset in prepared.fence.datasets),
        "capacity_geometry_hash": admission.plan.capacity_geometry_hash,
        "lease_digest": admission.lease.lease_digest,
        "reservation_id": admission.lease.reservation_id,
        "paired_profile_lease_digest": admission.plan.paired_profile_lease_digest,
        "database_system_identifier": admission.lease.database_system_identifier,
        "database_oid": admission.lease.database_oid,
        "database_name": admission.lease.database_name,
        "relations": relations,
    }
    encoded = json.dumps(manifest_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    if len(encoded) > 1024 * 1024:
        raise RuntimeError("provider_directory_prepared_manifest_too_large")
    print(
        "PROVIDER_DIRECTORY_CMS_PREPARED_LAYOUT\t"
        + json.dumps(
            {
                "prepared_manifest": manifest_by_field,
                "manifest_sha256": hashlib.sha256(encoded.encode("ascii")).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ),
        flush=True,
    )


async def _prepare_full_bundle(
    fhir: Any, fence: Any, run_id: str, metrics: dict[str, Any], targets: set[str], bundle: Any
):
    """Reuse full artifact dependency gates and deferred cutover collectors."""
    fhir._assert_provider_directory_artifact_target_dependencies(
        fence,
        publish_artifacts_targets=targets,
        publish_corroboration=False,
    )
    fhir._attach_artifact_fence_metrics(metrics, fence)
    request = fhir.ProviderDirectoryArtifactPublishRequest(
        run_id=run_id,
        metrics=dict(metrics),
        source_ids=list(fence.source_ids),
        publish_corroboration=False,
        publish_artifacts_targets=targets,
        publish_scope_run_id=None,
        address_key_run_id=None,
        full_address_artifact_rebuild=True,
    )
    published = await fhir._publish_provider_directory_artifacts(request)
    fhir._assert_candidate_artifact_bundle_complete(
        fence,
        published,
        bundle,
        publish_corroboration=False,
        publish_artifacts_targets=targets,
    )
    return published


async def _admit_profile_before_full_build(
    fhir: Any, execution: Any, fence: Any, run_id: str, control_run_id: str | None, admission: NonprofileAdmission
) -> None:
    """Consume the exact Profile lease while its signed observation remains fresh."""
    types = fhir._provider_directory_artifact_resource_types({"profile"}, publish_corroboration=False)
    resource_fence = await fhir._provider_directory_profile_resource_scope_fence(fence, {"profile"})
    capacity = await fhir._admit_provider_directory_profile_capacity(
        run_id=run_id,
        control_run_id=control_run_id,
        execution=execution,
        fence=fence,
        resource_fence=resource_fence,
        artifact_resource_types=types,
    )
    if (
        capacity.lease.reservation_id == admission.lease.reservation_id
        or capacity.lease.lease_digest == admission.lease.lease_digest
    ):
        raise RuntimeError("provider_directory_nonprofile_profile_reservation_reused")
    admission.profile_admission = (
        await admission.pause_profile(capacity) if admission.pause_profile is not None else capacity
    )


@asynccontextmanager
async def _profile_scope(
    fhir: Any,
    execution: Any,
    fence: Any,
    run_id: str,
    control_run_id: str | None,
    metrics: dict[str, Any],
    admission: NonprofileAdmission | None,
):
    """Keep the exact original changed-source workload and lease intact."""
    profile_targets = {"profile"}
    types = fhir._provider_directory_artifact_resource_types(profile_targets, publish_corroboration=False)
    resource_fence = await fhir._provider_directory_profile_resource_scope_fence(fence, profile_targets)
    if admission is not None:
        capacity = admission.profile_admission
        if capacity is None:
            raise RuntimeError("provider_directory_nonprofile_profile_admission_missing")
        if admission.resume_profile is not None:
            capacity = await admission.resume_profile(capacity, resource_fence, types)
    else:
        capacity = await fhir._admit_provider_directory_profile_capacity(
            run_id=run_id,
            control_run_id=control_run_id,
            execution=execution,
            fence=fence,
            resource_fence=resource_fence,
            artifact_resource_types=types,
        )
    if admission is not None and (
        capacity.lease.reservation_id == admission.lease.reservation_id
        or capacity.lease.lease_digest == admission.lease.lease_digest
    ):
        raise RuntimeError("provider_directory_nonprofile_profile_reservation_reused")
    capacity_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(capacity)
    admission_token = _ACTIVE.set(None)
    try:
        request = fhir.ProviderDirectoryArtifactPublishRequest(
            run_id=run_id,
            metrics=dict(metrics),
            source_ids=list(fence.source_ids),
            publish_corroboration=False,
            publish_artifacts_targets=profile_targets,
        )
        from process.provider_directory_cms_nonprofile_capacity import original_profile_input_scope

        with original_profile_input_scope(fhir):
            async with fhir._prepare_artifact_bundle_from_fence(
                fence,
                request,
                artifact_resource_types=types,
                resource_fence=resource_fence,
            ) as prepared:
                yield prepared
    finally:
        _ACTIVE.reset(admission_token)
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)


@asynccontextmanager
async def _prepare_purge(
    fhir: Any, execution: Any, fence: Any, run_id: str, control_run_id: str | None, metrics: dict[str, Any]
):
    """Preserve the existing admitted empty-fence purge without nonprofile work."""
    if (
        fence.datasets
        or fence.source_ids
        or not run_id
        or fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None
        or _ACTIVE.get() is not None
    ):
        raise RuntimeError("provider_directory_profile_purge_scope_invalid")
    fhir._assert_profile_selection_matches_artifact_fence(execution, fence)
    token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    try:
        async with _profile_scope(fhir, execution, fence, run_id, control_run_id, metrics, None) as (
            bundle,
            prepared_metrics,
        ):
            prepared = PreparedServingArtifacts(fhir, fence, execution, None, None, bundle, prepared_metrics, {}, None)
            metrics.update(prepared.metrics)
            yield prepared
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)


async def _overlay_identity(fhir: Any, bundle: Any, admission: NonprofileAdmission) -> OwnedRelation:
    """Expose the exact logged overlay stage to native address preparation."""
    name = bundle.relation_overrides.get(fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    if name is None:
        raise RuntimeError("provider_directory_nonprofile_overlay_missing")
    identities = await admission.measure(fhir, fhir._schema(), (name,))
    return next(identity for identity in identities if identity.schema == fhir._schema() and identity.relation == name)
