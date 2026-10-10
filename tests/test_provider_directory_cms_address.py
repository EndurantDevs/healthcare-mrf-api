# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify the real address factory's semantic binding and native preparation lifetime."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import cms_doctors_preparation as doctors_preparation
from process import provider_directory_cms_address as address
from process import provider_directory_cms_native_inputs as native_inputs
from process.cms_doctors_source_provenance import mint_doctors_source_provenance
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES
from tests.test_cms_doctors_source_provenance import source_metrics


def _incumbent_geo_bindings():
    """Describe the complete closed geo family with distinct synthetic physical identities."""
    return {
        f"{namespace or 'fixture'}.{name}": {
            "schema_name": namespace or "fixture",
            "table_name": name,
            "relation_oid": index + 1,
            "relfilenode": index + 101,
        }
        for index, (namespace, name) in enumerate(address.source_dependencies.projection._PROJECTION_DEPENDENCIES)
    }


@asynccontextmanager
async def _address_mock_session():
    yield SimpleNamespace(execute=AsyncMock())


@pytest.fixture
def address_build_case(monkeypatch):
    """Bind complete synthetic native inputs to the actual address preparation factory."""
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "true")
    for name in (
        "LIMIT_PER_SOURCE",
        "REUSE_STAGE",
        "REUSE_RAW_STAGE",
        "KEEP_RAW_STAGE",
        "COMPACT_SOURCE_RECORD_IDS_BY_REWRITE",
    ):
        monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_" + name, raising=False)
    monkeypatch.setattr(address, "_mapping_digest", lambda **_options: "d" * 64)
    datasets = tuple(
        SimpleNamespace(
            source_id=source_id,
            endpoint_id=source_id + "-endpoint",
            dataset_id=source_id + "-dataset",
            dataset_hash="a" * 64,
            evidence_run_id="run_" + "1" * 32,
            artifact_resources=("Location",),
        )
        for source_id in ("cms-npd", "retained")
    )
    execution = SimpleNamespace(
        attestation=SimpleNamespace(
            operation="publish",
            source_context_digest="b" * 64,
            desired_profile_as_of="2026-01-02",
        )
    )

    fhir = SimpleNamespace(
        db=SimpleNamespace(session=_address_mock_session, _transaction_binding=lambda: None), _schema=lambda: "fixture"
    )
    monkeypatch.setattr(address._native(), "db", fhir.db)
    native_input_fence_by_field = {
        "version": 1,
        "schema": "fixture",
        "relations": {
            f'"{schema}"."{name}"': {"relation_oid": None} for schema, name in native_inputs._relations("fixture")
        },
        "geo_bindings": _incumbent_geo_bindings(),
        "reference_authorities": {name: {} for name in ("cms-doctors", "geo", "tiger", "mrf")},
        "npi": {"input_revision": 1},
        "cms_office_read_relations": {name: {"relation_oid": None} for name in CMS_OFFICE_READ_TABLES},
    }
    monkeypatch.setattr(
        address, "capture_native_address_input_fence", AsyncMock(return_value=native_input_fence_by_field)
    )
    monkeypatch.setattr(address.source_dependencies, "capture_dependencies", AsyncMock())
    return (
        fhir,
        execution,
        SimpleNamespace(datasets=datasets),
        {"alias_generation": 2, "doctors": {"local_generation": 1}},
        native_input_fence_by_field,
    )


def _factory(address_build_case, **kwargs):
    factory = address.cms_address_preparation(
        *address_build_case[:4],
        native_input_fence=address_build_case[4],
        run_id="run_" + "2" * 32,
        worker_count=2,
        temp_file_limit_bytes_per_backend=1024,
    )
    return factory.with_prepared_doctors(**kwargs) if kwargs else factory


def _admission(factory):
    return SimpleNamespace(
        plan=SimpleNamespace(
            native_address_input_hash=factory.input_hash,
            native_address_targets=factory.native_targets,
            worker_count=2,
            temp_file_limit_bytes_per_backend=1024,
        )
    )


def _retention_policy(address_build_case):
    """Construct the real policy against the factory's actual synthetic CMS fence."""
    _fhir, execution, fence = address_build_case[:3]
    execution.attestation.proof_id = "e" * 64
    cms = fence.datasets[0]
    request = RegistryCMSRetentionRequest(
        PinnedFHIRMembershipSource(
            "fixture",
            cms.source_id,
            cms.endpoint_id,
            cms.dataset_id,
            cms.dataset_hash,
            "release-1",
            123,
            "cms-npd",
            execution.attestation.desired_profile_as_of,
        ),
        RegistryNetworkSourceCoordinates("fhir", cms.source_id, "fixture", cms.dataset_id, "producer", "edition"),
        execution.attestation.proof_id,
        "b" * 64,
        "c" * 64,
        4096,
        8192,
    )
    return request.policy(UUID("00000000-0000-0000-0000-000000000001"), "retained_owner", ("reader", "writer"))


def test_retention_policy_survives_current_input_recheck_and_binds_hash(address_build_case):
    policy = _retention_policy(address_build_case)
    original = _factory(address_build_case)
    factory = original.with_registry_source_retention(policy)
    expected = json.loads(factory.input_json)
    assert factory.input_hash != original.input_hash
    assert factory._current_input(address_build_case[2], expected) == factory.input_json
    assert factory._assert_inputs(address_build_case[2], _admission(factory)) == expected
    policy["runtime_roles"].append("z_reader")
    assert json.loads(factory.input_json)["registry_source_retention"]["runtime_roles"] == ["reader", "writer"]
    assert original.with_registry_source_retention(policy).input_hash != factory.input_hash
    with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
        factory._assert_inputs(address_build_case[2], _admission(original))


@pytest.mark.parametrize("change", ["schema", "source", "endpoint", "dataset", "hash", "date", "selection"])
def test_retention_policy_requires_exact_fence_and_selection(address_build_case, change):
    policy = _retention_policy(address_build_case)
    mutations_by_name = {
        "schema": (policy["source_pin"], "schema_name", "different"),
        "source": (policy["source_pin"], "source_id", "other"),
        "endpoint": (policy["source_pin"], "endpoint_id", "other"),
        "dataset": (policy["source_pin"], "dataset_id", "other"),
        "hash": (policy["source_pin"], "dataset_sha256", "f" * 64),
        "date": (policy["source_pin"], "as_of", "2026-01-03"),
        "selection": (policy, "selection_proof_id", "f" * 64),
    }
    target, key, value = mutations_by_name[change]
    target[key] = value
    if change == "schema":
        policy["binding_coordinates"]["dataset_schema"] = "different"
    elif change in {"source", "dataset"}:
        policy["binding_coordinates"][key + "_id"] = "other"
    with pytest.raises(ValueError, match="retention_(scope_changed|policy_invalid)"):
        _factory(address_build_case).with_registry_source_retention(policy)


@pytest.mark.parametrize("change", ["date", "hash", "selection"])
def test_retention_current_input_refuses_successor_scope(address_build_case, change):
    policy = _retention_policy(address_build_case)
    factory = _factory(address_build_case).with_registry_source_retention(policy)
    execution, fence = address_build_case[1:3]
    if change == "date":
        execution.attestation.desired_profile_as_of = "2026-01-03"
    elif change == "hash":
        fence.datasets[0].dataset_hash = "f" * 64
    else:
        execution.attestation.proof_id = "f" * 64
    with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
        factory._assert_inputs(fence, _admission(factory))


@pytest.mark.parametrize("changed_field", ["capture_id", "owner_role", "runtime_roles"])
def test_retention_custody_identity_changes_capacity_input_hash(address_build_case, changed_field):
    policy = _retention_policy(address_build_case)
    factory = _factory(address_build_case).with_registry_source_retention(policy)
    policy[changed_field] = {
        "capture_id": "00000000-0000-0000-0000-000000000002",
        "owner_role": "successor_owner",
        "runtime_roles": ["reader", "writer", "z_reader"],
    }[changed_field]
    changed_factory = _factory(address_build_case).with_registry_source_retention(policy)
    assert changed_factory.input_hash != factory.input_hash
    with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
        changed_factory._assert_inputs(address_build_case[2], _admission(factory))


def test_retention_policy_survives_both_prepared_doctors_extension_orders(address_build_case, prepared_doctors_case):
    policy = _retention_policy(address_build_case)
    doctors, bindings = prepared_doctors_case
    before = (
        _factory(address_build_case).with_registry_source_retention(policy).with_prepared_doctors(doctors, bindings)
    )
    after = _factory(address_build_case, doctors=doctors, dependency_bindings=bindings).with_registry_source_retention(
        policy
    )
    assert before.input_json == after.input_json
    assert before._assert_inputs(address_build_case[2], _admission(before))["registry_source_retention"] == policy


@pytest.mark.parametrize(
    "module_name",
    [
        "process.provider_directory_fhir",
        "process.provider_directory_address_overlay_components",
        "process.provider_directory_cms_overlay_projection",
        "process.provider_directory_cms_native_projection",
    ],
)
def test_mapper_digest_binds_desired_projection_code(monkeypatch, module_name):
    """A changed query mapper cannot retain an earlier native capacity identity."""
    original = address._mapping_digest()
    target = address.Path(address.importlib.import_module(module_name).__file__)
    read_bytes = address.Path.read_bytes

    def changed_bytes(path):
        source = read_bytes(path)
        return source + b"\n# changed query mapping\n" if path == target else source

    monkeypatch.setattr(address.Path, "read_bytes", changed_bytes)
    assert address._mapping_digest() != original


def test_input_hash_binds_complete_semantics_without_generated_stage_names(address_build_case):
    factory = _factory(address_build_case)
    assert _factory(address_build_case).input_hash == factory.input_hash
    fhir, execution, fence, dependencies = address_build_case[:4]
    reordered = SimpleNamespace(datasets=tuple(reversed(fence.datasets)))
    assert _factory((fhir, execution, reordered, dependencies, address_build_case[4])).input_hash == factory.input_hash
    payload = json.loads(factory.input_json)
    assert [pin["source_id"] for pin in payload["dataset_pins"]] == ["cms-npd", "retained"]
    assert "overlay_relation_oid" not in payload and "run_id" not in payload
    dependencies["doctors"]["local_generation"] += 1
    assert _factory(address_build_case).input_hash != factory.input_hash


@pytest.mark.parametrize("changed", ["date", "context", "data", "mapper", "option", "facility-option", "alias"])
def test_changed_semantic_inputs_cannot_use_original_address_reservation(address_build_case, monkeypatch, changed):
    factory = _factory(address_build_case)
    _, execution, fence, dependencies = address_build_case[:4]
    mutations_by_name = {
        "date": lambda: setattr(execution.attestation, "desired_profile_as_of", "2026-01-03"),
        "context": lambda: setattr(execution.attestation, "source_context_digest", "c" * 64),
        "data": lambda: setattr(fence.datasets[0], "dataset_hash", "c" * 64),
        "mapper": lambda: monkeypatch.setattr(address, "_mapping_digest", lambda **_options: "e" * 64),
        "option": lambda: monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY", "2"),
        "facility-option": lambda: monkeypatch.setenv("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES", "true"),
        "alias": lambda: dependencies.__setitem__("alias_generation", 3),
    }
    mutations_by_name[changed]()
    assert _factory(address_build_case).input_hash != factory.input_hash
    if changed != "alias":
        with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
            factory._assert_inputs(fence, _admission(factory))


@pytest.mark.parametrize(
    "option",
    ["LIMIT_PER_SOURCE", "REUSE_STAGE", "REUSE_RAW_STAGE", "KEEP_RAW_STAGE", "COMPACT_SOURCE_RECORD_IDS_BY_REWRITE"],
)
def test_limited_or_shared_native_builds_are_rejected_before_preparation(address_build_case, monkeypatch, option):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_" + option, "1")
    with pytest.raises(RuntimeError, match="full_owned_build_required"):
        _factory(address_build_case)


def test_logged_native_recipe_is_rejected_before_preparation(address_build_case, monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "false")
    with pytest.raises(RuntimeError, match="unlogged_preparation_required"):
        _factory(address_build_case)


@pytest.mark.parametrize("failure", [None, "body", "targets", "dependency"])
async def test_factory_prepares_full_desired_vector_and_retains_cleanup_ownership(
    address_build_case, monkeypatch, failure
):
    factory = _factory(address_build_case)
    fhir, _execution, fence, dependencies = address_build_case[:4]
    prepared = SimpleNamespace(
        stage_oids=tuple((name, name + "_staged", index + 1) for index, name in enumerate(factory.native_targets)),
        committed=False,
        context={},
    )
    if failure == "targets":
        prepared.stage_oids = prepared.stage_oids[:-1]
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value=dependencies))
    context_by_field = {"control_run_id": factory.run_id, "import_date": "synthetic"}
    monkeypatch.setattr(address, "_new_context", AsyncMock(return_value=context_by_field))
    cleanup = AsyncMock()
    monkeypatch.setattr(address, "cleanup_prepared_entity_address_generation", cleanup)

    async def prepare(build_context, task, **kwargs):
        assert build_context is context_by_field and task == address._RECIPE
        assert kwargs["native_input_hash"] == factory.input_hash
        preparation_input = kwargs["preparation_input"]
        assert len(preparation_input.dataset_pins) == 2 and preparation_input.overlay_relation_oid == 42
        assert preparation_input.semantic_as_of == "2026-01-02"
        assert preparation_input.relation_overrides == (("provider_directory_location", "location_staged"),)
        if failure == "dependency":
            address.capture_native_dependencies.return_value = {**dependencies, "alias_generation": 3}
        return prepared

    monkeypatch.setattr(address, "prepare_provider_directory_entity_address", prepare)
    relation_overrides_by_name = {
        "provider_directory_location": "location_staged",
        "provider_directory_source": "source_staged",
        "provider_directory_address_overlay": "overlay_staged",
    }
    overlay = SimpleNamespace(oid=42, relation="overlay_staged")
    if failure is not None:
        with pytest.raises(RuntimeError):
            async with factory(fence, relation_overrides_by_name, overlay, _admission(factory)):
                raise RuntimeError("synthetic body failure")
    else:
        async with factory(fence, relation_overrides_by_name, overlay, _admission(factory)) as prepared_address:
            assert prepared_address is prepared
            cleanup.assert_not_awaited()
            assert (
                address.admitted_native_input_fence(
                    SimpleNamespace(nonprofile_admission=_admission(factory)), prepared_address, dependencies
                )
                == address_build_case[4]
            )
    cleanup.assert_awaited_once_with(prepared)
    assert fhir.db._transaction_binding() is None


async def test_factory_refuses_changed_native_dependencies_before_startup(address_build_case, monkeypatch):
    factory = _factory(address_build_case)
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value={"alias_generation": 3}))
    startup = AsyncMock()
    monkeypatch.setattr(address, "_new_context", startup)
    with pytest.raises(RuntimeError, match="native_dependencies_changed"):
        async with factory(address_build_case[2], {}, None, _admission(factory)):
            pytest.fail("changed native authority must not yield")
    startup.assert_not_awaited()


async def test_factory_rejects_in_place_native_mutation_before_startup(address_build_case, monkeypatch):
    factory = _factory(address_build_case)
    address_build_case[4]["npi"]["input_revision"] += 1
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value=address_build_case[3]))
    startup = AsyncMock()
    monkeypatch.setattr(address, "_new_context", startup)
    with pytest.raises(RuntimeError, match="native_dependencies_changed"):
        async with factory(address_build_case[2], {}, None, _admission(factory)):
            pytest.fail("changed native content must not yield")
    startup.assert_not_awaited()


@pytest.mark.parametrize("input_json", [None, '{"native_input_fence":{}}'])
def test_cutover_rejects_missing_or_tampered_native_input_proof(address_build_case, input_json):
    factory = _factory(address_build_case)
    prepared = SimpleNamespace(nonprofile_admission=_admission(factory))
    staged_address = SimpleNamespace(context={"cms_native_address_input_json": input_json})
    with pytest.raises(RuntimeError, match="admitted_input_proof_required"):
        address.admitted_native_input_fence(prepared, staged_address, address_build_case[3])


def test_cutover_cannot_substitute_newer_native_output_dependencies(address_build_case):
    factory = _factory(address_build_case)
    prepared = SimpleNamespace(nonprofile_admission=_admission(factory))
    staged_address = SimpleNamespace(context={"cms_native_address_input_json": factory.input_json})
    recaptured_dependencies_by_field = {**address_build_case[3], "profile": {"generation_id": "successor"}}
    with pytest.raises(RuntimeError, match="admitted_native_dependencies_changed"):
        address.admitted_native_input_fence(prepared, staged_address, recaptured_dependencies_by_field)


@pytest.mark.parametrize("binding", ["different-database", "active-transaction"])
async def test_factory_rejects_wrong_database_or_outer_transaction_before_native_reads(
    address_build_case, monkeypatch, binding
):
    factory = _factory(address_build_case)
    if binding == "different-database":
        address_build_case[0].db = SimpleNamespace(_transaction_binding=lambda: None)
    else:
        address_build_case[0].db._transaction_binding = lambda: object()
    read = AsyncMock()
    monkeypatch.setattr(address, "capture_native_dependencies", read)
    with pytest.raises(RuntimeError, match="shared_unbound_database"):
        async with factory(address_build_case[2], {}, None, _admission(factory)):
            pytest.fail("invalid transaction ownership must not yield")
    read.assert_not_awaited()


@pytest.mark.parametrize("substitution", [None, "object", "admission", "missing"])
def test_cutover_archive_is_the_exact_prepared_address_input(address_build_case, substitution):
    factory = _factory(address_build_case)
    admission = _admission(factory)
    archive = SimpleNamespace(admission=admission, native_input_hash=factory.input_hash)
    prepared = SimpleNamespace(nonprofile_admission=admission, archive_delta=archive)
    staged_address = SimpleNamespace(
        context={"cms_native_address_input_json": factory.input_json},
        native_dependencies=SimpleNamespace(archive=archive),
    )
    if substitution == "object":
        prepared.archive_delta = SimpleNamespace(**vars(archive))
    elif substitution == "admission":
        archive.admission = _admission(factory)
    elif substitution == "missing":
        prepared.archive_delta = None
    if substitution is None:
        assert (
            address.admitted_native_input_fence(prepared, staged_address, address_build_case[3])
            == address_build_case[4]
        )
        return
    with pytest.raises(RuntimeError, match="prepared_archive_changed"):
        address.admitted_native_input_fence(prepared, staged_address, address_build_case[3])


async def test_factory_cleanup_runs_before_propagating_body_cancellation(address_build_case, monkeypatch):
    factory = _factory(address_build_case)
    prepared = SimpleNamespace(
        context={},
        stage_oids=tuple((name, name + "_staged", index + 1) for index, name in enumerate(factory.native_targets)),
    )
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value=address_build_case[3]))
    monkeypatch.setattr(address, "_new_context", AsyncMock(return_value={}))
    monkeypatch.setattr(address, "prepare_provider_directory_entity_address", AsyncMock(return_value=prepared))
    cleanup = AsyncMock()
    monkeypatch.setattr(address, "cleanup_prepared_entity_address_generation", cleanup)
    with pytest.raises(asyncio.CancelledError):
        async with factory(
            address_build_case[2], {}, SimpleNamespace(oid=42, relation="overlay_staged"), _admission(factory)
        ):
            raise asyncio.CancelledError("synthetic body cancellation")
    cleanup.assert_awaited_once_with(prepared)


async def test_new_context_refuses_an_occupied_stage_and_ignores_shared_import_suffix(monkeypatch):
    native = address._native()
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "shared")

    async def startup(build_context):
        build_context.update(context={}, import_date="shared")

    monkeypatch.setattr(native, "startup", startup)
    collision = AsyncMock(return_value=13)
    monkeypatch.setattr(native.db, "scalar", collision)
    with pytest.raises(RuntimeError, match="stage_name_occupied"):
        await address._new_context("run_" + "2" * 32, "fixture")
    assert "shared" not in collision.call_args.kwargs["relation"]


@pytest.fixture
def prepared_doctors_case(address_build_case, monkeypatch, tmp_path):
    """Supply a typed source bundle; physical seal checks remain in separate native tests."""
    database = address_build_case[0].db
    monkeypatch.setattr(doctors_preparation._native(), "db", database)
    stages = tuple(
        (model.__tablename__, doctors_preparation._native().make_class(model, "source").__tablename__, index + 70)
        for index, model in enumerate(doctors_preparation._models())
    )
    metrics = source_metrics(tmp_path, monkeypatch)
    provenance = mint_doctors_source_provenance(metrics)
    metrics.update(published=False, publication_state="prepared")
    doctors = doctors_preparation.PreparedCMSDoctorsGeneration(
        "fixture",
        "source",
        stages,
        None,
        None,
        metrics,
        {"publication_state": "prepared"},
        sealed_filenodes=tuple((oid, oid + 1000) for _target, _stage, oid in stages),
        source_provenance_json=provenance,
    )
    bindings_by_name = json.loads(json.dumps(address_build_case[4]["geo_bindings"]))
    bindings_by_name["fixture.doctor_clinician_address"].update(
        table_name=stages[0][1], relation_oid=stages[0][2], relfilenode=doctors.sealed_filenodes[0][1]
    )
    return doctors, bindings_by_name


def test_prepared_doctors_bind_all_three_sealed_heaps_separately_from_incumbent(
    address_build_case, prepared_doctors_case
):
    doctors, bindings = prepared_doctors_case
    factory = _factory(address_build_case, doctors=doctors, dependency_bindings=bindings)
    payload = json.loads(factory.input_json)
    assert payload["native_input_fence"] == address_build_case[4]
    assert payload["native_dependencies"] == address_build_case[3]
    assert payload["desired_doctors"]["stage_oids"] == [list(stage) for stage in doctors.stage_oids]
    assert payload["desired_geo_bindings"] == bindings
    assert factory.source_relation_overrides == doctors.relation_overrides
    factory.source_relation_overrides.clear()
    assert len(factory.source_relation_overrides) == 3
    bindings["fixture.doctor_clinician_address"]["relation_oid"] += 1
    assert factory._assert_inputs(address_build_case[2], _admission(factory)) == payload


@pytest.mark.parametrize("change", ["missing", "incumbent", "other-input", "filenode"])
def test_prepared_doctors_require_exact_desired_geo_bindings(address_build_case, prepared_doctors_case, change):
    doctors, bindings = prepared_doctors_case
    if change == "missing":
        bindings = None
    elif change == "incumbent":
        bindings = address_build_case[4]["geo_bindings"]
    elif change == "other-input":
        bindings["fixture.npi_address"]["relation_oid"] += 100
    else:
        bindings["fixture.doctor_clinician_address"]["relfilenode"] += 1
    with pytest.raises(RuntimeError, match="desired_geo_bindings"):
        _factory(address_build_case, doctors=doctors, dependency_bindings=bindings)


@pytest.mark.parametrize("change", ["provenance", "seal", "published"])
def test_unproved_doctors_source_cannot_enter_factory(address_build_case, prepared_doctors_case, change):
    doctors, bindings = prepared_doctors_case
    if change == "provenance":
        doctors.source_provenance_json = None
    elif change == "seal":
        doctors.sealed_filenodes = ()
    else:
        doctors.committed = True
    with pytest.raises((ValueError, RuntimeError), match="provenance_required|physical_seal_required|not prepared"):
        _factory(address_build_case, doctors=doctors, dependency_bindings=bindings)


def test_changed_doctors_provenance_cannot_use_original_reservation(address_build_case, prepared_doctors_case):
    doctors, bindings = prepared_doctors_case
    factory = _factory(address_build_case, doctors=doctors, dependency_bindings=bindings)
    doctors.metrics["education"]["downloaded_at"] = "2026-01-03T00:00:00"
    doctors.source_provenance_json = mint_doctors_source_provenance(doctors.metrics)
    assert _factory(address_build_case, doctors=doctors, dependency_bindings=bindings).input_hash != factory.input_hash
    with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
        factory._assert_inputs(address_build_case[2], _admission(factory))


@pytest.mark.parametrize("failure", [None, "seal", "override"])
async def test_factory_checks_prepared_doctors_before_native_work_and_passes_exact_inputs(
    address_build_case, prepared_doctors_case, monkeypatch, failure
):
    doctors, bindings = prepared_doctors_case
    factory = _factory(address_build_case, doctors=doctors, dependency_bindings=bindings)
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value=address_build_case[3]))
    startup = AsyncMock(return_value={})
    monkeypatch.setattr(address, "_new_context", startup)
    prepared = SimpleNamespace(
        context={},
        stage_oids=tuple((name, name + "_staged", index + 1) for index, name in enumerate(factory.native_targets)),
    )
    prepare = AsyncMock(return_value=prepared)
    cleanup = AsyncMock()
    monkeypatch.setattr(address, "prepare_provider_directory_entity_address", prepare)
    monkeypatch.setattr(address, "cleanup_prepared_entity_address_generation", cleanup)
    overrides_by_name = {"provider_directory_location": "location_staged", **factory.source_relation_overrides}
    if failure == "seal":
        address.source_dependencies.capture_dependencies.side_effect = RuntimeError("synthetic physical seal failure")
    if failure == "override":
        overrides_by_name["cms_doctor_education"] = "unproved_education"
    if failure:
        with pytest.raises(RuntimeError, match="physical seal|override_unproved"):
            async with factory(
                address_build_case[2],
                overrides_by_name,
                SimpleNamespace(oid=42, relation="overlay"),
                _admission(factory),
            ):
                pytest.fail("unproved input must not yield")
        startup.assert_not_awaited()
        cleanup.assert_not_awaited()
        return
    async with factory(
        address_build_case[2], overrides_by_name, SimpleNamespace(oid=42, relation="overlay"), _admission(factory)
    ):
        kwargs = prepare.call_args.kwargs
        assert kwargs["doctors"] is doctors and kwargs["dependency_bindings"] == bindings
        assert dict(kwargs["preparation_input"].relation_overrides) == {
            "provider_directory_location": "location_staged",
            "doctor_clinician_address": doctors.stage_oids[0][1],
        }
        assert not doctors.committed
    cleanup.assert_awaited_once_with(prepared)


async def test_unproved_doctors_override_is_rejected_with_incumbent_source(address_build_case, monkeypatch):
    factory = _factory(address_build_case)
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value=address_build_case[3]))
    with pytest.raises(RuntimeError, match="override_unproved"):
        async with factory(address_build_case[2], {"cms_doctor_group_site": "unproved"}, None, _admission(factory)):
            pytest.fail("unproved input must not yield")
