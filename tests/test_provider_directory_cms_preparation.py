# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Separate signed nonprofile costs from the exact Profile delta scope."""

import asyncio
import contextvars
import datetime
import hashlib
import importlib
import json
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_preparation as preparation
from tests.provider_directory_profile_capacity_signing_guard_test_support import capacity_signing_guard
from tests.test_provider_directory_profile_capacity_attestation import (
    VALIDATION_TIME,
    _signed_envelope,
    _verify,
)

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture(autouse=True)
def emulate_only_in_memory_sql_backend(monkeypatch):
    """Keep statement-order stubs separate from the actual PostgreSQL limit checks."""
    original = preparation.nonprofile_sql_transaction
    original_manifest = preparation._emit_prepared_manifest

    @asynccontextmanager
    async def transaction(fhir, admission):
        if isinstance(fhir, PreparationFakeFHIR):
            yield
        else:
            async with original(fhir, admission):
                yield

    monkeypatch.setattr(preparation, "nonprofile_sql_transaction", transaction)

    async def emit_manifest(prepared, run_id, control_run_id):
        """Legacy statement-order stubs lack catalogs; full boundary cases below retain the real emitter."""
        if not isinstance(prepared.fhir, PreparationFakeFHIR):
            await original_manifest(prepared, run_id, control_run_id)

    monkeypatch.setattr(preparation, "_emit_prepared_manifest", emit_manifest)


def _inputs():
    """Return a replacement CMS selection retaining an unrelated current source."""
    datasets = tuple(
        SimpleNamespace(source_id=source, dataset_id=dataset, artifact_resources=("Practitioner",))
        for source, dataset in (("cms-npd", "cms-new"), ("retained", "retained-current"))
    )
    fence = SimpleNamespace(datasets=datasets, source_ids=("cms-npd", "retained"))
    execution = SimpleNamespace(
        attestation=SimpleNamespace(
            proof_id="proof-a",
            desired_profile_as_of="2026-09-29",
            desired_cms_dataset={"dataset_id": "cms-new"},
            operation="publish",
        )
    )
    projection = SimpleNamespace(projection_hash="ab" * 32)
    return execution, fence, projection


def _signed_lease(plan):
    """Verify a synthetic signature over the exact nonprofile plan identity."""

    def mutate(body):
        body["capacity_geometry_hash"] = plan.capacity_geometry_hash
        timestamp_by_field = {
            field: datetime.datetime.fromisoformat(body[field].replace("Z", "+00:00"))
            for field in ("observed_at", "issued_at", "expires_at", "max_build_deadline")
        }
        guard, guard_hash, receipt_hash = capacity_signing_guard(
            capacity_geometry_hash=plan.capacity_geometry_hash,
            **timestamp_by_field,
        )
        body.update(signing_preflight_guard=guard, signing_preflight_guard_sha256=guard_hash, nonce=receipt_hash)

    return _verify(_signed_envelope(body_mutator=mutate), expected_capacity_geometry_hash=plan.capacity_geometry_hash)


def _admission(*, artifact_targets=None, types=None, check=None):
    """Build a separately verified reservation with an explicit phase authority."""
    execution, fence, projection = _inputs()
    artifact_targets = artifact_targets or {"address_overlay", "network_catalog"}
    types = types or frozenset({"Practitioner"})
    plan = preparation.NonprofileAdmissionPlan(
        execution.attestation.proof_id,
        execution.attestation.desired_profile_as_of,
        preparation.desired_fence_hash(fence),
        projection.projection_hash,
        tuple(sorted(artifact_targets)),
        tuple(sorted(types)),
        1000,
        2,
        (("data", 100_000), ("temp", 100_000), ("wal", 100_000)),
        1000,
        60,
        ("entity_address_unified",),
        "ef" * 32,
        1024,
        50_000,
        1000,
        "ab" * 32,
    )

    async def approve(request):
        return preparation.NonprofileAdmissionReceipt(
            request.phase,
            request.lease.lease_digest,
            request.lease.reservation_id,
            request.plan.capacity_geometry_hash,
            request.relations,
            request.logging_relations,
        )

    return preparation.NonprofileAdmission(_signed_lease(plan), plan, check or approve)


def _database():
    """Track exact fake relation names and their physical identities."""
    relation_by_ref = {}

    async def first(query, *, relation_ref):
        return relation_by_ref.get(relation_ref)

    async def scalar(query, *, relation_ref):
        return relation_by_ref.get(relation_ref, {}).get("oid")

    async def status(query):
        if query.startswith("DROP TABLE "):
            relation_by_ref.pop(query.removeprefix("DROP TABLE ").removesuffix(";"))

    @asynccontextmanager
    async def transaction():
        yield

    database = SimpleNamespace(
        first=first,
        scalar=scalar,
        status=status,
        transaction=transaction,
        relations=relation_by_ref,
        transaction_active=False,
    )
    database._transaction_binding = lambda: object() if database.transaction_active else None
    return database


@pytest.mark.parametrize("persistence", ["u", "p"])
async def test_address_registration_reserves_only_pending_logging(persistence):
    admission = _admission()
    requests = []
    approve = admission.check_phase

    async def observe(request):
        requests.append(request)
        return await approve(request)

    admission.check_phase = observe
    execution, fence, projection = _inputs()
    await admission.before_scratch(
        execution, fence, projection, set(admission.plan.publish_targets), frozenset(admission.plan.resource_types)
    )
    database = _database()
    database.relations["test_schema.address_stage"] = {"oid": 42, "total_bytes": 200, "persistence": persistence}
    fhir = SimpleNamespace(
        db=database,
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _pagination_checkpoint_row_mapping=lambda row: row,
    )
    await admission.register_address_stages(
        fhir,
        "test_schema",
        (("entity_address_unified", "address_stage", 42),),
        input_hash=admission.plan.native_address_input_hash,
    )
    request = requests[-1]
    assert request.phase == ("pre_logging" if persistence == "u" else "readiness")
    assert request.logging_relations == ((("test_schema", "address_stage"),) if persistence == "u" else ())
    assert (("test_schema", "address_stage") in admission._logged_relations) == (persistence == "p")


class PreparationFakeFHIR:
    """Track independent scope contexts and exact fake database ownership."""

    _ArtifactScopeMaterializationPlan = importer._ArtifactScopeMaterializationPlan
    ProviderDirectoryArtifactPublishRequest = importer.ProviderDirectoryArtifactPublishRequest
    PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS = ("profile", "address_overlay", "network_catalog", "corroboration")
    PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE = "provider_directory_address_overlay"
    ProviderDirectoryPractitionerRole = importer.ProviderDirectoryPractitionerRole
    ProviderDirectoryOrganizationAffiliation = importer.ProviderDirectoryOrganizationAffiliation

    def __init__(self, events):
        self.events = events
        self.db = _database()
        self.execution, self.fence, self.projection = _inputs()
        self.RESOURCE_MODELS = tuple(
            SimpleNamespace(__tablename__="provider_directory_" + name) for name in ("practitioner", "location")
        )
        self.ProviderDirectorySource = SimpleNamespace(__tablename__="provider_directory_source")
        self._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION = contextvars.ContextVar("test_capacity", default=None)
        self._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION = contextvars.ContextVar("test_selection", default=None)
        self.scope_relations = contextvars.ContextVar("test_relations", default={})
        self.bundles = []
        self.materialized = False
        self.indexed_names = set()
        self._provider_directory_artifact_scope_exact_projection = AsyncMock(return_value=self.projection)
        self._refresh_bundle_profile_delta_metrics = AsyncMock()
        self._profile_capacity_preflight_clock = AsyncMock(return_value=VALIDATION_TIME)
        original_status = self.db.status

        async def status(query):
            if query.startswith("CREATE UNLOGGED TABLE "):
                if not self.db.relations:
                    self.events.append("full-create")
                name = query.removeprefix("CREATE UNLOGGED TABLE ")
                self.db.relations[name] = {"oid": len(self.db.relations) + 100, "total_bytes": 100, "persistence": "u"}
            if query.startswith("DROP TABLE test_schema.cms_directory_scope_") and "full-cleanup" not in events:
                events.append("full-cleanup")
            await original_status(query)

        self.db.status = status

    @staticmethod
    def _schema():
        return "test_schema"

    @staticmethod
    def _unscoped_qt(schema, name):
        return schema + "." + name

    @staticmethod
    def _pagination_checkpoint_row_mapping(row):
        return row

    @staticmethod
    def _provider_directory_artifact_resource_types(*args, **kwargs):
        return frozenset({"Practitioner"})

    _assert_provider_directory_artifact_scope_exact_capacity = staticmethod(lambda *args: None)
    _assert_profile_selection_matches_artifact_fence = staticmethod(lambda *args: None)
    _attach_artifact_fence_metrics = staticmethod(lambda *args: None)
    _assert_provider_directory_artifact_target_dependencies = staticmethod(lambda *args, **kwargs: None)
    _assert_candidate_artifact_bundle_complete = staticmethod(lambda *args, **kwargs: None)

    async def _artifact_scope_relation_identities(self, schema, names):
        return {
            name: (self.db.relations[schema + "." + name]["oid"], "r")
            for name in names
            if schema + "." + name in self.db.relations
        }

    @staticmethod
    def _provider_directory_artifact_scope_table_sql(model, schema, name):
        return "CREATE UNLOGGED TABLE " + schema + "." + name

    async def _build_artifact_scope_pk(self, model, schema, name, *, status_executor):
        assert self.materialized
        await status_executor("CREATE INDEX")
        self.indexed_names.add(name)

    async def _materialize_artifact_scope_payload(self, *arguments):
        schema, plan, full_fence, resource_fence, types, exact, batch, workers = arguments
        assert self._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
        assert full_fence is self.fence and resource_fence is self.fence and exact is self.projection
        assert len(plan.created_tables) == 3
        assert not self.indexed_names
        self.materialized = True

    @contextmanager
    def _artifact_scope_tokens(self, full_fence, resource_fence, overrides, oids):
        assert set(overrides.values()) <= self.indexed_names
        token = self.scope_relations.set(overrides)
        self.events.append("full-enter")
        try:
            yield
        finally:
            self.scope_relations.reset(token)

    @asynccontextmanager
    async def _provider_directory_artifact_bundle_scope(self):
        bundle = SimpleNamespace(stages=[], profile_delta=None, promoted=False, relation_overrides={})

        async def mark():
            if self.db.transaction_active:
                raise RuntimeError("provider_directory_artifact_bundle_commit_pending")
            bundle.promoted = True

        bundle.mark_promoted = mark
        self.bundles.append(bundle)
        try:
            yield bundle
        finally:
            assert self._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
            self.events.append("full-bundle-exit")
            for stage in bundle.stages:
                await preparation.is_stage_cleanup_handled(self, stage)

    @contextmanager
    def _provider_directory_artifact_relation_scope(self, overrides):
        merged_overrides_by_relation = dict(self.scope_relations.get())
        merged_overrides_by_relation.update(overrides)
        token = self.scope_relations.set(merged_overrides_by_relation)
        try:
            yield
        finally:
            self.scope_relations.reset(token)

    async def _publish_provider_directory_artifacts(self, request):
        assert request.source_ids == list(self.fence.source_ids)
        assert request.full_address_artifact_rebuild and request.publish_scope_run_id is None
        assert self._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
        for target in ("provider_directory_address_overlay", "provider_directory_network_catalog"):
            name = target + "_stage"
            self.db.relations["test_schema." + name] = {
                "oid": len(self.db.relations) + 100,
                "total_bytes": 200,
                "persistence": "u",
            }
            await preparation.check_stage_logging(self, "test_schema", name)
            self.db.relations["test_schema." + name]["persistence"] = "p"
            self.bundles[-1].stages.append(
                SimpleNamespace(schema="test_schema", stage_table=name, target_relation=target)
            )
            self.bundles[-1].relation_overrides[target] = name
        return {"address_overlay": {"prepared": True}, "network_catalog": {"prepared": True}}

    async def _provider_directory_profile_resource_scope_fence(self, full_fence, targets):
        assert full_fence is self.fence and targets == {"profile"}
        return SimpleNamespace(source_ids=() if self.execution.attestation.operation == "purge" else ("cms-npd",))

    async def _admit_provider_directory_profile_capacity(self, **options):
        assert options["resource_fence"].source_ids == (
            () if self.execution.attestation.operation == "purge" else ("cms-npd",)
        )
        assert options["fence"] is self.fence
        self.events.append("profile-admit")
        return SimpleNamespace(lease=SimpleNamespace(reservation_id="profile-distinct", lease_digest="cd" * 32))

    @asynccontextmanager
    async def _prepare_artifact_bundle_from_fence(self, full_fence, request, **options):
        assert self._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None
        assert preparation._ACTIVE.get() is None
        refresh_sources = () if self.execution.attestation.operation == "purge" else ("cms-npd",)
        assert full_fence is self.fence and options["resource_fence"].source_ids == refresh_sources
        token = self.scope_relations.set({"provider_directory_practitioner": "profile_scope_practitioner"})
        profile_bundle = SimpleNamespace(
            stages=[],
            profile_delta=SimpleNamespace(refresh_source_ids=refresh_sources, generation_id="generation-a"),
            promoted=False,
        )

        async def mark():
            profile_bundle.promoted = True

        profile_bundle.mark_promoted = mark
        self.events.append("profile-enter")
        try:
            yield profile_bundle, {"profile": {"prepared": True}}
        finally:
            self.events.append("profile-exit")
            self.scope_relations.reset(token)


@asynccontextmanager
async def _address_scope(fence, overrides, overlay, admission):
    """Hold a synthetic address result in the full scope before Profile work."""
    assert overlay.persistence == "p" and overrides
    assert admission.profile_admission is not None
    yield SimpleNamespace(prepared=True)


def _prepare(*args, **options):
    """Supply the required native preparation seam in focused fake tests."""
    options.setdefault("address_preparation", _address_scope)
    return preparation.prepare_serving_artifacts(*args, **options)


def _fake_fhir(events):
    """Return one bounded synthetic preparation surface."""
    fake = PreparationFakeFHIR(events)
    return fake, fake.execution, fake.fence, fake.projection


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [None, object(), SimpleNamespace()])
async def test_missing_admission_stops_before_scratch(missing):
    fake, execution, fence, _ = _fake_fhir([])
    with pytest.raises(RuntimeError, match="nonprofile_admission_required"):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=missing
        ):
            pytest.fail("unadmitted preparation yielded")
    fake._provider_directory_artifact_scope_exact_projection.assert_not_awaited()
    assert not fake.db.relations


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [False, True])
async def test_two_scopes_preserve_full_inputs_and_exact_profile_delta(abort):
    events = []
    fake, execution, fence, _ = _fake_fhir(events)
    checks = []
    admission = _admission()
    original_check = admission.check_phase

    async def check(request):
        checks.append(request.phase)
        return await original_check(request)

    admission.check_phase = check
    try:
        async with _prepare(
            fake,
            execution,
            fence,
            run_id="run-a",
            control_run_id="control-a",
            metrics={},
            nonprofile_admission=admission,
        ) as prepared:
            assert events == ["profile-admit", "full-create", "full-enter", "profile-enter"]
            assert prepared.profile_delta.refresh_source_ids == ("cms-npd",)
            assert len(prepared.stages) == 2
            assert prepared.relation_overrides["provider_directory_practitioner"].startswith("cms_directory_scope_")
            assert fake.scope_relations.get()["provider_directory_practitioner"] == "profile_scope_practitioner"
            assert prepared.overlay_identity.persistence == "p"
            assert len(fake.db.relations) == 5
            assert not prepared.nonprofile_bundle.promoted and not prepared.profile_bundle.promoted
            await prepared.assert_ready(cutover=True)
            if abort:
                raise RuntimeError("abort")
            await prepared.mark_committed(
                profile_result={
                    "selection_proof_id": "proof-a",
                    "profile_as_of": "2026-09-29",
                    "operation": "publish",
                    "status": "published",
                    "generation_id": "generation-a",
                    "evidence_rows": 21,
                    "profile_rows": 12,
                }
            )
            assert prepared.metrics["profile"]["evidence_rows"] == 21
            assert prepared.nonprofile_bundle.promoted and prepared.profile_bundle.promoted
    except RuntimeError as error:
        assert abort and str(error) == "abort"
    assert events[-3:] == ["profile-exit", "full-bundle-exit", "full-cleanup"]
    assert fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
    assert fake._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None
    assert preparation._ACTIVE.get() is None
    assert not fake.db.relations
    assert checks[0] == "pre_scratch" and checks.count("pre_logging") == 2
    assert "cutover" in checks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("desired_profile_as_of", "2026-09-30"),
        ("artifact_scope_projection_hash", "cd" * 32),
        ("desired_fence_hash", "ef" * 32),
        ("publish_targets", ("profile",)),
    ],
)
async def test_signed_plan_mismatch_stops_before_scratch(field, value):
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    admission.plan = replace(admission.plan, **{field: value})
    with pytest.raises(RuntimeError, match="nonprofile_scope_changed"):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ):
            pytest.fail("changed signed plan yielded")
    assert not fake.db.relations


@pytest.mark.asyncio
async def test_unbound_geometry_and_invalid_phase_receipt_fail_closed():
    execution, fence, projection = _inputs()
    admission = _admission()
    admission.lease = replace(admission.lease, capacity_geometry_hash="ef" * 32)
    with pytest.raises(RuntimeError, match="nonprofile_geometry_changed"):
        await admission.before_scratch(
            execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
        )
    admission = _admission(check=AsyncMock(return_value=True))
    with pytest.raises(RuntimeError, match="nonprofile_phase_receipt_changed"):
        await admission.before_scratch(
            execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
        )


@pytest.mark.asyncio
async def test_logging_gate_blocks_alter_and_default_is_inactive(monkeypatch):
    database = _database()
    database.status = AsyncMock()
    monkeypatch.setattr(importer, "db", database)
    monkeypatch.setattr(importer, "_assert_provider_directory_logged_relation", AsyncMock())
    await importer._prepare_provider_directory_artifact_stage("test_schema", "stage_a")
    database.status.assert_awaited_once()
    database.status.reset_mock()
    admission = _admission()
    token = preparation._ACTIVE.set(admission)
    try:
        with pytest.raises(RuntimeError, match="admission_not_started"):
            await importer._prepare_provider_directory_artifact_stage("test_schema", "stage_b")
        database.status.assert_not_awaited()
    finally:
        preparation._ACTIVE.reset(token)


def _mutate_relation(relation_by_ref, name, change):
    """Inject one independent physical drift into a synthetic stage."""
    if change == "missing":
        relation_by_ref.pop(name)
    elif change == "oid":
        relation_by_ref[name]["oid"] += 1
    elif change == "data":
        relation_by_ref[name]["total_bytes"] = 100_001
    else:
        relation_by_ref[name]["persistence"] = "u"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["oid", "missing", "data", "unlogged"])
async def test_relation_drift_and_aggregate_budget_reject_readiness(change):
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    with pytest.raises(RuntimeError, match="nonprofile_(relation_changed|data_budget_exceeded|stage_not_logged)"):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ) as prepared:
            name = "test_schema." + prepared.overlay_identity.relation
            _mutate_relation(fake.db.relations, name, change)
            await prepared.assert_ready()
    if change == "oid":
        assert list(fake.db.relations) == [name]
        assert admission.cleanup_preserved == [("test_schema", prepared.overlay_identity.relation)]
    else:
        assert not fake.db.relations


@pytest.mark.asyncio
async def test_purge_reuses_admitted_empty_profile_scope_without_nonprofile_work():
    events = []
    fake, execution, fence, _ = _fake_fhir(events)
    execution.attestation.operation = "purge"
    execution.attestation.desired_cms_dataset = None
    execution.attestation.desired_profile_as_of = None
    fence.datasets, fence.source_ids = (), ()
    async with _prepare(fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}) as prepared:
        assert prepared.nonprofile_admission is None and prepared.nonprofile_bundle is None
        assert prepared.overlay_identity is None and prepared.relation_overrides == {}
        assert prepared.stages == () and prepared.profile_delta.refresh_source_ids == ()
        await prepared.assert_ready(cutover=True)
        await prepared.mark_committed(
            profile_result={
                "selection_proof_id": "proof-a",
                "operation": "purge",
                "status": "purged",
                "generation_id": "generation-a",
                "profile_as_of": "2026-09-29",
                "evidence_rows": 0,
                "profile_rows": 0,
            }
        )
    assert events == ["profile-admit", "profile-enter", "profile-exit"]
    fake._provider_directory_artifact_scope_exact_projection.assert_not_awaited()
    assert not fake.db.relations


@pytest.mark.asyncio
async def test_historical_profile_result_consumes_without_reading_current_pointer():
    fake, execution, fence, _ = _fake_fhir([])
    async with _prepare(
        fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=_admission()
    ) as prepared:
        fake.db.transaction_active = True
        with pytest.raises(RuntimeError, match="commit_pending"):
            await prepared.mark_committed(profile_result={})
        fake.db.transaction_active = False
        with pytest.raises(RuntimeError, match="committed_result_changed"):
            await prepared.mark_committed(profile_result={"selection_proof_id": "other"})
        assert not prepared.nonprofile_bundle.promoted and not prepared.profile_bundle.promoted
        fake._refresh_bundle_profile_delta_metrics.side_effect = AssertionError("must not read current pointer")
        await prepared.mark_committed(
            profile_result={
                "selection_proof_id": "proof-a",
                "profile_as_of": "2026-09-29",
                "operation": "publish",
                "status": "published",
                "generation_id": "generation-a",
                "evidence_rows": 20,
                "profile_rows": 10,
            }
        )
        fake._refresh_bundle_profile_delta_metrics.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["materialize", "logging", "cancel"])
async def test_preparation_failures_clean_all_captured_relations(phase):
    events = []
    fake, execution, fence, _ = _fake_fhir(events)
    admission = _admission()
    expected_error = asyncio.CancelledError if phase == "cancel" else RuntimeError
    if phase == "materialize":
        fake._materialize_artifact_scope_payload = AsyncMock(side_effect=RuntimeError("materialize"))
    elif phase == "logging":
        original = admission.check_phase

        async def reject(request):
            if request.phase == "pre_logging":
                raise RuntimeError("logging")
            return await original(request)

        admission.check_phase = reject
    with pytest.raises(expected_error):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ):
            raise asyncio.CancelledError()
    assert not fake.db.relations and not admission.cleanup_preserved
    assert fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
    assert preparation._ACTIVE.get() is None


@pytest.mark.asyncio
async def test_layout_index_failure_cleans_oid_captured_at_create():
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    build = fake._build_artifact_scope_pk

    async def fail_index(model, schema, name, *, status_executor):
        await build(model, schema, name, status_executor=status_executor)
        raise RuntimeError("index creation failed")

    fake._build_artifact_scope_pk = fail_index
    with pytest.raises(RuntimeError, match="index creation failed"):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ):
            pytest.fail("failed layout yielded")
    assert not fake.db.relations and not admission.cleanup_preserved
    assert fake.materialized


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate", [False, True])
async def test_native_scope_loads_all_heaps_before_indexes_and_exposure(monkeypatch, duplicate):
    """Real indexes preserve the old layout and duplicate failure removes every owned heap."""
    from tests.provider_directory_profile_delta_test_support import _delta_database

    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        monkeypatch.setattr(importer, "_profile_capacity_preflight_clock", AsyncMock(return_value=VALIDATION_TIME))
        execution, fence, projection = _inputs()
        admission = _admission()
        admission.plan = replace(
            admission.plan, reservation_bytes=(("data", 1_000_000), ("temp", 100_000), ("wal", 100_000))
        )
        admission.lease = _signed_lease(admission.plan)
        await admission.before_scratch(
            execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
        )

        monkeypatch.setattr(
            importer,
            "_materialize_artifact_scope_payload",
            partial(_load_native_scope_heaps, database, schema, duplicate),
        )
        if duplicate:
            from sqlalchemy.exc import IntegrityError

            with pytest.raises(IntegrityError):
                async with preparation._full_scope(importer, fence, frozenset({"Practitioner"}), projection, admission):
                    pytest.fail("duplicate full scope was exposed")
        else:
            async with preparation._full_scope(
                importer, fence, frozenset({"Practitioner"}), projection, admission
            ) as overrides:
                await _assert_native_scope_layouts(database, schema, overrides)
        assert not importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
        assert not admission.cleanup_preserved
        for relation_schema, name in admission._relations:
            assert await database.scalar("SELECT to_regclass(:relation)", relation=f"{relation_schema}.{name}") is None


async def _native_scope_index_shape(database, schema, name):
    """Read real primary-key and bucket-index definitions without comparing generated names."""
    rows = await database.all(
        "SELECT i.indisprimary, i.indisunique, i.indkey::text, am.amname, "
        "pg_get_expr(i.indexprs,i.indrelid) AS expression "
        "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam "
        "WHERE i.indrelid=to_regclass(:relation) ORDER BY i.indisprimary DESC, i.indkey::text;",
        relation=f"{schema}.{name}",
    )
    return [tuple(row) for row in rows]


async def _load_native_scope_heaps(database, schema, duplicate, *arguments):
    """Verify every native heap has no indexes until all synthetic payloads are loaded."""
    _schema, plan, *_rest = arguments
    assert len(plan.relation_by_table) == 9
    for base, name in plan.relation_by_table.items():
        assert await _native_scope_index_shape(database, schema, name) == []
        if base == "provider_directory_source":
            await database.status(
                f"INSERT INTO {schema}.{name} "
                "(source_id,org_name,requires_registration,requires_api_key) VALUES ('cms-npd','Example',false,false)"
            )
        else:
            await database.status(f"INSERT INTO {schema}.{name} (source_id,resource_id) VALUES ('cms-npd','example')")
    if duplicate:
        name = plan.relation_by_table["provider_directory_practitioner"]
        await database.status(f"INSERT INTO {schema}.{name} (source_id,resource_id) VALUES ('cms-npd','example')")


async def _assert_native_scope_layouts(database, schema, overrides):
    """Compare post-load indexes with the untouched Profile layout on all nine real models."""
    for index, model in enumerate((importer.ProviderDirectorySource, *importer.RESOURCE_MODELS)):
        reference = "reference_scope_" + str(index)
        await importer._create_provider_directory_artifact_scope_layout(
            model, schema, reference, status_executor=database.status
        )
        name = overrides[model.__tablename__]
        assert await _native_scope_index_shape(database, schema, name) == await _native_scope_index_shape(
            database, schema, reference
        )
        assert await database.scalar(f"SELECT count(*) FROM {schema}.{name}") == 1


@pytest.mark.asyncio
async def test_replaced_typed_scope_is_preserved_during_cleanup():
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    with pytest.raises(RuntimeError, match="nonprofile_relation_changed"):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ) as prepared:
            name = "test_schema." + prepared.relation_overrides["provider_directory_practitioner"]
            fake.db.relations[name]["oid"] += 1
            await prepared.assert_ready()
    assert list(fake.db.relations) == [name]
    assert admission.cleanup_preserved == [("test_schema", name.removeprefix("test_schema."))]


@pytest.mark.asyncio
@pytest.mark.parametrize("desired", [False, True])
async def test_capacity_preflight_routes_only_explicit_desired_selection(monkeypatch, desired):
    desired_module = importlib.import_module("process.provider_directory_cms_desired_fence")
    execution = SimpleNamespace(attestation=SimpleNamespace(operation="publish"))
    if desired:
        execution.attestation.desired_cms_dataset = {"dataset_id": "cms-new"}
    desired_resolver = AsyncMock(return_value="desired-fence")
    current_resolver = AsyncMock(return_value="current-fence")
    monkeypatch.setattr(desired_module, "resolve_desired_fence", desired_resolver)
    monkeypatch.setattr(importer, "_resolve_provider_directory_artifact_datasets", current_resolver)
    monkeypatch.setattr(importer, "_assert_profile_selection_matches_artifact_fence", lambda *args: None)
    monkeypatch.setattr(
        importer, "_provider_directory_artifact_resource_types", lambda *args, **kwargs: frozenset({"Practitioner"})
    )
    monkeypatch.setattr(
        importer, "_provider_directory_profile_resource_scope_fence", AsyncMock(return_value="delta-fence")
    )
    backfill_check = AsyncMock()
    monkeypatch.setattr(importer, "_assert_no_provider_directory_resource_id_npi_backfill_candidates", backfill_check)
    fence, delta, resource_types = await importer._profile_capacity_preflight_fences(execution, ["cms-npd"])
    assert fence == ("desired-fence" if desired else "current-fence") and delta == "delta-fence"
    backfill_check.assert_awaited_once_with(importer._schema(), ["cms-npd"])
    if desired:
        desired_resolver.assert_awaited_once_with(importer, execution)
        current_resolver.assert_not_awaited()
    else:
        current_resolver.assert_awaited_once_with(["cms-npd"], should_select_validated_candidates=False)
        desired_resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_cleanup_preserves_replacement_oid(monkeypatch):
    from tests.test_provider_directory_dataset_artifact_db import _dataset_database

    async with _dataset_database(monkeypatch) as (database, schema):
        await database.status(f"CREATE UNLOGGED TABLE {schema}.owned_stage (value integer);")
        admission = _admission()
        execution, fence, projection = _inputs()
        await admission.before_scratch(
            execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
        )
        await admission.measure(importer, schema, ("owned_stage",))
        original_oid = admission._relations[(schema, "owned_stage")]
        await database.status(f"DROP TABLE {schema}.owned_stage;")
        await database.status(f"CREATE UNLOGGED TABLE {schema}.owned_stage (value integer);")
        replacement_oid = await database.scalar(
            "SELECT to_regclass(:relation)::oid::bigint;", relation=schema + ".owned_stage"
        )
        assert replacement_oid != original_oid
        token = preparation._ACTIVE.set(admission)
        try:
            with pytest.raises(RuntimeError, match="nonprofile_relation_changed"):
                await admission.assert_ready(importer, schema)
            await importer._remove_provider_directory_artifact_stage(
                SimpleNamespace(schema=schema, stage_table="owned_stage")
            )
        finally:
            preparation._ACTIVE.reset(token)
        assert (
            await database.scalar("SELECT to_regclass(:relation)::oid::bigint;", relation=schema + ".owned_stage")
            == replacement_oid
        )


def test_scratch_names_are_unique_bounded_and_outside_profile_recovery():
    fake, _, _, _ = _fake_fhir([])
    first, second = preparation._scope_plan(fake), preparation._scope_plan(fake)
    assert not set(first.relation_by_table.values()) & set(second.relation_by_table.values())
    assert all(len(name) <= 63 for name in first.relation_by_table.values())
    assert not first.created_tables
    for model in (fake.ProviderDirectorySource, *fake.RESOURCE_MODELS):
        assert not first.relation_by_table[model.__tablename__].startswith(
            importer._provider_directory_artifact_scope_table_prefix(model.__tablename__)
        )


@pytest.mark.asyncio
async def test_missing_native_callback_rejects_before_any_admission_or_scratch():
    fake, execution, fence, _ = _fake_fhir([])
    with pytest.raises(RuntimeError, match="address_preparation_required"):
        async with preparation.prepare_serving_artifacts(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=_admission()
        ):
            pytest.fail("native preparation was omitted")
    assert fake.events == [] and not fake.db.relations


@pytest.mark.asyncio
async def test_address_scope_stays_alive_and_finishes_before_profile_build():
    events = []
    fake, execution, fence, _ = _fake_fhir(events)

    @asynccontextmanager
    async def native_scope(selected_fence, overrides, overlay, admission):
        """Observe the full logged scope while its already-consumed Profile is paused."""
        assert selected_fence is fence and overlay.persistence == "p"
        assert fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
        assert admission.profile_admission is not None and overrides
        events.append("address-enter")
        try:
            yield "native-address"
        finally:
            events.append("address-exit")

    async with preparation.prepare_serving_artifacts(
        fake,
        execution,
        fence,
        run_id="run-a",
        control_run_id=None,
        metrics={},
        nonprofile_admission=_admission(),
        address_preparation=native_scope,
    ) as prepared:
        assert prepared.address == "native-address"
        assert events == ["profile-admit", "full-create", "full-enter", "address-enter", "profile-enter"]
    assert events[-4:] == ["profile-exit", "address-exit", "full-bundle-exit", "full-cleanup"]


@pytest.mark.asyncio
async def test_native_intermediate_registry_tracks_exact_rename_and_drop():
    fake, execution, fence, projection = _fake_fhir([])
    admission = _admission()
    await admission.before_scratch(
        execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
    )
    original_by_field = {"oid": 321, "total_bytes": 100, "persistence": "u"}
    fake.db.relations["test_schema.raw_stage"] = original_by_field
    await admission.register_external_relation(fake, "test_schema", "raw_stage", 321)
    assert preparation._owned_cleanup_names(admission, "test_schema", []) == []
    await admission.assert_external_relation(fake, "test_schema", "raw_stage", 321)
    fake.db.relations["test_schema.compact_stage"] = fake.db.relations.pop("test_schema.raw_stage")
    await admission.rename_external_relation(fake, "test_schema", "raw_stage", "compact_stage", 321)
    assert admission._relations == {("test_schema", "compact_stage"): 321}
    fake.db.relations.pop("test_schema.compact_stage")
    await admission.retire_external_relation(fake, "test_schema", "compact_stage", 321)
    assert not admission._relations and not admission._external_relations


@pytest.mark.asyncio
async def test_native_registry_rejects_replacement_and_preserves_native_ownership():
    fake, execution, fence, projection = _fake_fhir([])
    admission = _admission()
    await admission.before_scratch(
        execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
    )
    fake.db.relations["test_schema.native_stage"] = {"oid": 321, "total_bytes": 100, "persistence": "u"}
    await admission.register_external_relation(fake, "test_schema", "native_stage", 321)
    fake.db.relations["test_schema.native_stage"]["oid"] = 654
    with pytest.raises(RuntimeError, match="relation_changed"):
        await admission.assert_external_relation(fake, "test_schema", "native_stage", 321)
    with pytest.raises(RuntimeError, match="relation_changed"):
        await admission.retire_external_relation(fake, "test_schema", "native_stage", 321)
    assert admission._relations == {("test_schema", "native_stage"): 321}
    assert preparation._owned_cleanup_names(admission, "test_schema", []) == []


def _manifest_add_heap(fixture, name, required_indexes, *, persistence="p", primary_key=False):
    """Supply catalog-shaped rows independently of the production manifest collector."""
    oid = len(fixture.heaps) + 100
    fixture.heaps[name] = {"oid": oid, "persistence": persistence, "total_bytes": 100}
    fixture.attributes.append({"relation_oid": oid, "attnum": 1, "attname": "npi"})
    names = list(required_indexes) + ([name + "_pkey"] if primary_key else [])
    for index_name in names:
        fixture.indexes.append(
            {
                "relation_oid": oid,
                "index_oid": 1000 + len(fixture.indexes),
                "index_name": index_name,
                "indisvalid": True,
                "indisready": True,
                "indislive": True,
                "indisprimary": index_name == name + "_pkey",
                "index_am": "btree",
                "indkey": "1",
                "index_expressions": "",
                "index_predicate": "",
            }
        )
    return oid


def _manifest_scope_heaps(fixture):
    """Represent all eight actual resource models and their source heap."""
    fhir = fixture.fhir
    for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS):
        name = "scope_" + model.__tablename__
        required_indexes = [fhir._artifact_scope_pk_names(name)[1]]
        if model in (fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation):
            required_indexes.append(fhir._provider_directory_profile_bucket_index_sql("test_schema", name)[0])
        oid = _manifest_add_heap(fixture, name, required_indexes, persistence="u")
        fixture.prepared.relation_overrides[model.__tablename__] = name
        fixture.admission._relations[("test_schema", name)] = oid
    for target, suffixes, naming in (
        (
            fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
            fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_INDEX_SUFFIXES,
            fhir._address_overlay_index_name,
        ),
        (
            fhir.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
            fhir.PROVIDER_DIRECTORY_NETWORK_CATALOG_INDEX_SUFFIXES,
            fhir._network_catalog_index_name,
        ),
    ):
        name = target + "_stage"
        oid = _manifest_add_heap(fixture, name, [naming(name, suffix) for suffix in suffixes])
        fixture.admission._relations[("test_schema", name)] = oid
        fixture.prepared.nonprofile_bundle.stages.append(
            SimpleNamespace(schema="test_schema", stage_table=name, target_relation=target)
        )


def _manifest_native_heaps(fixture):
    """Use the native seven-model declaration and exact prepared stage tuples."""
    native = importlib.import_module("process.entity_address_unified")
    models = importlib.import_module("process.entity_address_result_generation").ENTITY_ADDRESS_RESULT_MODELS
    fixture.admission.plan = replace(
        fixture.admission.plan, native_address_targets=tuple(model.__tablename__ for model in models)
    )
    fixture.admission.lease = _signed_lease(fixture.admission.plan)
    fixture.address = SimpleNamespace(db_schema="test_schema", stage_oids=[], swaps=[])
    for model in models:
        name = model.__tablename__ + "_cms" + "a" * 20
        required_indexes = [
            native._stage_index_name(name, index.get("name", "_".join(index["index_elements"])))
            for index in model.__my_additional_indexes__
        ]
        oid = _manifest_add_heap(fixture, name, required_indexes)
        fixture.admission._relations[("test_schema", name)] = oid
        fixture.address.stage_oids.append((model.__tablename__, name, oid))
        fixture.address.swaps.append(
            SimpleNamespace(
                stage_cls=SimpleNamespace(__tablename__=name, __my_additional_indexes__=model.__my_additional_indexes__)
            )
        )


def _manifest_profile_heaps(fixture, mode):
    """Retain separate initial-checkpoint and ordinary delta ownership contracts."""
    profile = importer.profile_artifact
    build = SimpleNamespace(
        schema="test_schema",
        evidence_stage="profile_evidence_stage",
        profile_stage="profile_stage",
        owner_run_id="run-a",
        selection_proof_id="proof-a",
        materialization_mode="full_swap",
    )
    bundle = SimpleNamespace(stages=[], profile_delta=None)
    for name, target_relation, suffixes in (
        (build.evidence_stage, profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_EVIDENCE_INDEX_SUFFIXES),
        (build.profile_stage, profile.PROFILE_TABLE, profile.PROFILE_INDEX_SUFFIXES),
    ):
        _manifest_add_heap(
            fixture, name, [profile.profile_index_name(name, suffix) for suffix in suffixes], primary_key=True
        )
        bundle.stages.append(
            SimpleNamespace(
                schema=build.schema,
                stage_table=name,
                target_relation=target_relation,
                profile_initial_build=build,
                build_fence=object(),
            )
        )
    if mode == "delta":
        affected_oid = _manifest_add_heap(fixture, "affected_npi_stage", [], primary_key=True)
        bundle.profile_delta = SimpleNamespace(
            schema=build.schema,
            owner_run_id=build.owner_run_id,
            selection_proof_id=build.selection_proof_id,
            evidence_stage=build.evidence_stage,
            evidence_stage_oid=fixture.heaps[build.evidence_stage]["oid"],
            profile_stage=build.profile_stage,
            profile_stage_oid=fixture.heaps[build.profile_stage]["oid"],
            affected_npi_stage="affected_npi_stage",
            affected_npi_stage_oid=affected_oid,
        )
        bundle.stages = []
    fixture.profile_bundle = bundle


def _manifest_fixture(monkeypatch, mode):
    """Exercise the real final preparation boundary with catalog and worker-log seams only."""
    execution, fence, _projection = _inputs()
    execution.attestation.selection_fingerprint = "ef" * 32
    fixture = SimpleNamespace(heaps={}, indexes=[], attributes=[], admission=_admission())
    names = (
        "ProviderDirectorySource",
        "RESOURCE_MODELS",
        "ProviderDirectoryPractitionerRole",
        "ProviderDirectoryOrganizationAffiliation",
        "PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE",
        "PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE",
        "PROVIDER_DIRECTORY_ADDRESS_OVERLAY_INDEX_SUFFIXES",
        "PROVIDER_DIRECTORY_NETWORK_CATALOG_INDEX_SUFFIXES",
        "_artifact_scope_pk_names",
        "_provider_directory_profile_bucket_index_sql",
        "_address_overlay_index_name",
        "_network_catalog_index_name",
        "profile_artifact",
        "profile_initial",
    )
    fixture.fhir = SimpleNamespace(
        **{name: getattr(importer, name) for name in names},
        db=_database(),
        _schema=lambda: "test_schema",
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _pagination_checkpoint_row_mapping=lambda row: row,
        _profile_capacity_preflight_clock=AsyncMock(return_value=VALIDATION_TIME),
        _assert_provider_directory_profile_checkpoint_ready=AsyncMock(),
    )
    fixture.prepared = preparation.PreparedServingArtifacts(
        fixture.fhir, fence, execution, fixture.admission, SimpleNamespace(stages=[]), None, {}, {}, None
    )
    _manifest_scope_heaps(fixture)
    _manifest_native_heaps(fixture)
    _manifest_profile_heaps(fixture, mode)
    _manifest_install_seams(monkeypatch, fixture)
    return fixture


def _manifest_install_seams(monkeypatch, fixture):
    """Keep real ownership and readiness code while replacing physical host I/O."""
    fixture.admission._started = True
    fixture.fhir.db.relations.update({"test_schema." + name: heap for name, heap in fixture.heaps.items()})

    async def identities(_schema, names):
        return {
            name: (fixture.heaps[name]["oid"], "r", fixture.heaps[name]["persistence"])
            for name in names
            if name in fixture.heaps
        }

    async def catalog(oids):
        assert set(oids) == {heap["oid"] for heap in fixture.heaps.values()}
        assert fixture.fhir.db._transaction_binding() is not None
        assert fixture.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is fixture.resumed_profile
        assert preparation._ACTIVE.get() is None
        return fixture.attributes, fixture.indexes, [], []

    @asynccontextmanager
    async def profile_scope(*_args):
        token = fixture.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(fixture.resumed_profile)
        try:
            yield fixture.profile_bundle, {"profile": {"prepared": True}}
        finally:
            fixture.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)

    fixture.fhir._artifact_scope_relation_identities = identities
    fixture.fhir._profile_capacity_relation_catalog = catalog
    monkeypatch.setattr(preparation, "_profile_scope", profile_scope)
    _manifest_sql_backend(fixture)


def _manifest_sql_backend(fixture):
    """Run the real paired-deadline and SQL-setting helpers on a local settings stub."""
    fixture.resumed_profile = object()
    fixture.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION = contextvars.ContextVar(
        "manifest_profile", default=None
    )
    fixture.admission.paired_profile_lease = SimpleNamespace(
        max_build_deadline=VALIDATION_TIME + datetime.timedelta(seconds=15)
    )
    fixture.settings_by_name = {
        name: "0"
        for name in (
            "temp_file_limit",
            "max_parallel_workers_per_gather",
            "max_parallel_maintenance_workers",
            "statement_timeout",
            "lock_timeout",
        )
    }
    fixture.initial_settings = dict(fixture.settings_by_name)
    fixture.limits_observed = []

    async def scalar(_query, **params):
        if "has_parameter_privilege" in _query:
            return True
        if "pg_size_bytes" in _query and not params:
            return int(fixture.settings_by_name["temp_file_limit"].removesuffix("kB")) * 1024
        if "setting_name" in params:
            return fixture.settings_by_name[params["setting_name"]]
        fixture.limits_observed.append(dict(params))
        assert params == {"limit": 1024, "timeout_ms": 14000}
        return fixture.settings_by_name == {
            "temp_file_limit": "1kB",
            "max_parallel_workers_per_gather": "0",
            "max_parallel_maintenance_workers": "0",
            "statement_timeout": "14000ms",
            "lock_timeout": "14000ms",
        }

    async def status(statement):
        assert statement.startswith("SET LOCAL ") and statement.endswith("';")
        name, value = statement.removeprefix("SET LOCAL ").removesuffix(";").split(" = ")
        fixture.settings_by_name[name] = value.strip("'")

    @asynccontextmanager
    async def transaction():
        previous = fixture.fhir.db.transaction_active
        owner_settings_by_name = dict(fixture.settings_by_name)
        fixture.fhir.db.transaction_active = True
        try:
            yield
        finally:
            fixture.fhir.db.transaction_active = previous
            if not previous:
                fixture.settings_by_name.update(owner_settings_by_name)

    fixture.fhir.db.scalar, fixture.fhir.db.status, fixture.fhir.db.transaction = scalar, status, transaction


@asynccontextmanager
async def _manifest_boundary(fixture):
    """Call the actual boundary after native and Profile preparation and before publication."""

    @asynccontextmanager
    async def address_scope(*_args):
        yield fixture.address

    async with preparation._prepare_address_and_profile(
        fixture.prepared, run_id="run-a", control_run_id="run-a", metrics={}, address_preparation=address_scope
    ):
        yield


@pytest.mark.parametrize("mode", ["initial", "delta"])
@pytest.mark.asyncio
async def test_prepared_manifest_is_complete_before_publication(monkeypatch, capsys, mode):
    fixture = _manifest_fixture(monkeypatch, mode)
    async with _manifest_boundary(fixture):
        records = capsys.readouterr().out.splitlines()
        assert len(records) == 1
        prefix, encoded_record = records[0].split("\t", 1)
        assert prefix == "PROVIDER_DIRECTORY_CMS_PREPARED_LAYOUT"
        record = json.loads(encoded_record)
        manifest = record["prepared_manifest"]
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        assert record["manifest_sha256"] == hashlib.sha256(encoded.encode("ascii")).hexdigest()
        assert manifest["run_id"] == manifest["control_run_id"] == "run-a"
        assert manifest["selection_proof_id"] == "proof-a"
        assert manifest["source_vector"] == [["cms-npd", "cms-new"], ["retained", "retained-current"]]
        assert manifest["desired_fence_hash"] == preparation.desired_fence_hash(fixture.prepared.fence)
        relations = manifest["relations"]
        assert {row["relation"] for row in relations} == set(fixture.heaps)
        assert len(relations) == (20 if mode == "initial" else 21)
        assert {row["target"] for row in relations if row["role"] == "resource_scope"} == {
            model.__tablename__ for model in (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS)
        }
        assert all(row["columns"] and row["indexes"] for row in relations)
        assert all(
            set(row["required_indexes"]) <= {index["index_name"] for index in row["indexes"]} for row in relations
        )
    fixture.fhir._assert_provider_directory_profile_checkpoint_ready.assert_not_awaited()
    assert fixture.limits_observed == [{"limit": 1024, "timeout_ms": 14000}]
    assert fixture.settings_by_name == fixture.initial_settings
    assert fixture.fhir.db._transaction_binding() is None


@pytest.mark.parametrize(
    "corruption,reason",
    [
        ("missing-index", "prepared_manifest_indexes_incomplete"),
        ("invalid-index", "prepared_manifest_indexes_incomplete"),
        ("wrong-oid", "nonprofile_relation_changed"),
        ("missing-scope", "prepared_manifest_ownership_changed"),
        ("wrong-profile-owner", "prepared_manifest_profile_changed"),
        ("unlogged-serving", "prepared_manifest_stage_not_logged"),
    ],
)
@pytest.mark.asyncio
async def test_prepared_manifest_refuses_incomplete_or_foreign_layout(monkeypatch, capsys, corruption, reason):
    fixture = _manifest_fixture(monkeypatch, "initial")
    first_name = "scope_" + importer.ProviderDirectorySource.__tablename__
    match corruption:
        case "missing-index":
            fixture.indexes.pop(0)
        case "invalid-index":
            fixture.indexes[0]["indisready"] = False
        case "wrong-oid":
            fixture.heaps[first_name]["oid"] += 500
        case "missing-scope":
            fixture.prepared.relation_overrides.pop(importer.ProviderDirectorySource.__tablename__)
        case "wrong-profile-owner":
            fixture.profile_bundle.stages[0].profile_initial_build.owner_run_id = "run-other"
        case "unlogged-serving":
            fixture.heaps[importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE + "_stage"]["persistence"] = "u"
    with pytest.raises(RuntimeError, match=reason):
        async with _manifest_boundary(fixture):
            pytest.fail("incomplete layout reached publication")
    assert capsys.readouterr().out == ""
