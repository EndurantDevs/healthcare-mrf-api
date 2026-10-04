# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preparation guards retain signed scope and creation-time storage ownership."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_candidate_preparation as candidate
from process import entity_address_preparation_admission as admitted
from process import provider_directory_cms_address as address
from process import provider_directory_cms_archive as archive
from process import provider_directory_cms_desired_fence as desired
from process import provider_directory_cms_native_inputs as native_inputs
from process import provider_directory_cms_native_layout as layout
from process import provider_directory_cms_preparation as preparation
from tests.test_entity_address_candidate_preparation_postgres import _inputs as _address_inputs
from tests.test_provider_directory_cms_address import (
    _admission as _address_admission,
)
from tests.test_provider_directory_cms_address import (
    _factory as _address_factory,
)
from tests.test_provider_directory_cms_address import (
    address_build_case,
)
from tests.test_provider_directory_cms_desired_fence import _fixture as _desired_fixture
from tests.test_provider_directory_cms_native_projection import _query_inputs
from tests.test_provider_directory_cms_preparation import (
    _admission,
    _fake_fhir,
    _inputs,
    _prepare,
    _signed_lease,
    emulate_only_in_memory_sql_backend,
)


@asynccontextmanager
async def _transaction():
    yield SimpleNamespace(execute=AsyncMock())


async def _started_admission():
    admission = _admission()
    execution, fence, projection = _inputs()
    await admission.before_scratch(
        execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
    )
    return admission


@pytest.mark.parametrize(
    "changes",
    [
        {"selection_proof_id": "different"},
        {"resource_types": ("Location",)},
        {"batch_size": True},
        {"batch_size": 0},
        {"worker_count": True},
        {"worker_count": 0},
    ],
)
async def test_invalid_signed_workload_does_not_consume_admission(changes):
    execution, fence, projection = _inputs()
    admission = _admission()
    admission.plan = replace(admission.plan, **changes)
    check = AsyncMock()
    admission.check_phase = check
    with pytest.raises(RuntimeError, match="nonprofile_scope_changed"):
        await admission.before_scratch(
            execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
        )
    assert not admission._started and not admission._relations
    check.assert_not_awaited()


async def test_consumed_admission_cannot_start_again():
    admission = await _started_admission()
    execution, fence, projection = _inputs()
    check = AsyncMock()
    admission.check_phase = check
    with pytest.raises(RuntimeError, match="nonprofile_scope_changed"):
        await admission.before_scratch(
            execution, fence, projection, {"address_overlay", "network_catalog"}, frozenset({"Practitioner"})
        )
    check.assert_not_awaited()


@pytest.mark.parametrize("failure", ["lease", "callback", "duplicate-storage"])
def test_admission_requires_verified_lease_and_unique_storage_classes(failure):
    admission = _admission()
    if failure == "lease":
        admission.lease = object()
    elif failure == "callback":
        admission.check_phase = None
    else:
        admission.plan = replace(admission.plan, reservation_bytes=admission.plan.reservation_bytes + (("data", 1),))
        admission.lease = _signed_lease(admission.plan)
    with pytest.raises(RuntimeError, match="nonprofile_admission_required|nonprofile_reservation_invalid"):
        admission._assert_lease()
    assert not admission._started


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"oid": None}, "relation_changed"),
        ({"oid": True}, "relation_changed"),
        ({"oid": 0}, "relation_changed"),
        ({"total_bytes": True}, "relation_storage_invalid"),
        ({"total_bytes": -1}, "relation_storage_invalid"),
        ({"persistence": "t"}, "relation_storage_invalid"),
    ],
)
async def test_invalid_storage_never_updates_captured_oid(changes, reason):
    admission = await _started_admission()
    fake, *_ = _fake_fhir([])
    fake.db.relations["test_schema.stage_a"] = {"oid": 41, "total_bytes": 10, "persistence": "u", **changes}
    with pytest.raises(RuntimeError, match=reason):
        await admission.measure(fake, "test_schema", ("stage_a",))
    assert admission._relations == {("test_schema", "stage_a"): 0}
    assert not admission._logged_relations


@pytest.mark.parametrize("failure", ["hash", "targets", "duplicate-name", "oid", "replaced"])
async def test_native_stage_registration_rejects_before_storage_or_phase_reads(failure):
    admission = await _started_admission()
    fake, *_ = _fake_fhir([])
    fake.db.first = AsyncMock()
    admission.check_phase = AsyncMock()
    stages = (("entity_address_unified", "native_stage", 41),)
    input_hash = admission.plan.native_address_input_hash
    if failure == "hash":
        input_hash = "f" * 64
    elif failure == "targets":
        stages = (("unexpected", "native_stage", 41),)
    elif failure == "duplicate-name":
        stages = stages * 2
        admission.plan = replace(admission.plan, native_address_targets=("entity_address_unified",) * 2)
    elif failure == "oid":
        stages = (("entity_address_unified", "native_stage", True),)
    else:
        admission._relations[("test_schema", "native_stage")] = 42
    previous_relations_by_name = dict(admission._relations)
    with pytest.raises(RuntimeError, match="address_scope_changed|address_identity_invalid"):
        await admission.register_address_stages(fake, "test_schema", stages, input_hash=input_hash)
    assert admission._relations == previous_relations_by_name and not admission._external_relations
    fake.db.first.assert_not_awaited()
    admission.check_phase.assert_not_awaited()


@pytest.mark.parametrize("operation", ["register", "assert", "retire"])
async def test_external_identity_rejection_keeps_registry_and_avoids_catalog_reads(operation):
    admission = await _started_admission()
    fake, *_ = _fake_fhir([])
    fake.db.scalar = AsyncMock()
    fake.db.first = AsyncMock()
    admission._relations[("test_schema", "native_stage")] = 41
    admission._external_relations.add(("test_schema", "native_stage"))
    method = getattr(admission, operation + "_external_relation")
    with pytest.raises(RuntimeError, match="external_identity_invalid"):
        await method(fake, "test_schema", "native_stage", 42)
    assert admission._relations == {("test_schema", "native_stage"): 41}
    fake.db.scalar.assert_not_awaited()
    fake.db.first.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, "destination-owned", "destination-replaced"])
async def test_logged_native_rename_requires_exact_destination_and_moves_logging_ownership(failure):
    admission = await _started_admission()
    fake, *_ = _fake_fhir([])
    admission._relations[("test_schema", "old_stage")] = 41
    admission._external_relations.add(("test_schema", "old_stage"))
    admission._logged_relations.add(("test_schema", "old_stage"))
    fake.db.relations["test_schema.new_stage"] = {"oid": 41, "total_bytes": 10, "persistence": "p"}
    if failure == "destination-owned":
        admission._relations[("test_schema", "new_stage")] = 41
    elif failure == "destination-replaced":
        fake.db.relations["test_schema.new_stage"]["oid"] = 42
    if failure:
        with pytest.raises(RuntimeError, match="external_name_conflict|relation_changed"):
            await admission.rename_external_relation(fake, "test_schema", "old_stage", "new_stage", 41)
        assert ("test_schema", "old_stage") in admission._external_relations
        assert admission._logged_relations == {("test_schema", "old_stage")}
    else:
        await admission.rename_external_relation(fake, "test_schema", "old_stage", "new_stage", 41)
        assert admission._relations == {("test_schema", "new_stage"): 41}
        assert admission._external_relations == admission._logged_relations == {("test_schema", "new_stage")}


@pytest.mark.parametrize("failure", [None, "body"])
async def test_cutover_authorization_is_scoped_and_rechecks_consumption(failure):
    admission = await _started_admission()
    fake, *_ = _fake_fhir([])
    events = []

    @asynccontextmanager
    async def operation():
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    admission.cutover_operation = operation
    admission.check_cutover = AsyncMock()
    try:
        async with admission.publication(fake, "test_schema"):
            assert admission._cutover_active
            await admission.assert_ready(fake, "test_schema", cutover=True)
            await admission.assert_cutover_complete()
            if failure:
                raise RuntimeError("body")
    except RuntimeError as error:
        assert failure == "body" and str(error) == "body"
    assert events == ["enter", "exit"] and not admission._cutover_active
    assert [call.args for call in admission.check_cutover.await_args_list] == [((),), (None,)]
    with pytest.raises(RuntimeError, match="cutover_authorization_required"):
        await admission.assert_cutover_complete()


async def test_stage_capture_requires_the_creation_transaction():
    fake, *_ = _fake_fhir([])
    admission = await _started_admission()
    token = preparation._ACTIVE.set(admission)
    try:
        with pytest.raises(RuntimeError, match="stage_capture_requires_transaction"):
            await preparation.capture_nonprofile_stage(fake, "test_schema", "stage_a")
        assert not admission._relations
        fake.db.relations["test_schema.stage_a"] = {"oid": 41, "total_bytes": 10, "persistence": "u"}
        fake.db.transaction_active = True
        await preparation.capture_nonprofile_stage(fake, "test_schema", "stage_a")
        assert admission._relations == {("test_schema", "stage_a"): 41}
    finally:
        preparation._ACTIVE.reset(token)


async def test_cleanup_rechecks_after_lock_and_records_replacement_once():
    fake, *_ = _fake_fhir([])
    admission = await _started_admission()
    admission._relations[("test_schema", "stage_a")] = 41
    fake.db.scalar = AsyncMock(side_effect=[41, 42, 42])
    fake.db.status = AsyncMock()
    await preparation._drop_owned_relation(fake, admission, "test_schema", "stage_a")
    await preparation._drop_owned_relation(fake, admission, "test_schema", "stage_a")
    assert admission.cleanup_preserved == [("test_schema", "stage_a")]
    assert [call.args[0] for call in fake.db.status.await_args_list] == [
        "LOCK TABLE test_schema.stage_a IN ACCESS EXCLUSIVE MODE NOWAIT;"
    ]


@pytest.mark.parametrize("failure", ["operation", "desired", "run", "profile-active", "nonprofile-active"])
async def test_invalid_serving_request_fails_before_projection_or_scratch(failure):
    fake, execution, fence, _ = _fake_fhir([])
    run_id = "run-a"
    profile_token = admission_token = None
    if failure == "operation":
        execution.attestation.operation = "unexpected"
    elif failure == "desired":
        execution.attestation.desired_cms_dataset = None
    elif failure == "run":
        run_id = ""
    elif failure == "profile-active":
        profile_token = fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object())
    else:
        admission_token = preparation._ACTIVE.set(object())
    try:
        with pytest.raises(RuntimeError, match="operation_invalid|desired_selection_required|admission_already_active"):
            async with _prepare(
                fake,
                execution,
                fence,
                run_id=run_id,
                control_run_id=None,
                metrics={},
                nonprofile_admission=_admission(),
            ):
                pytest.fail("invalid serving request yielded")
        fake._provider_directory_artifact_scope_exact_projection.assert_not_awaited()
        assert not fake.events and not fake.db.relations
    finally:
        if profile_token is not None:
            fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(profile_token)
        if admission_token is not None:
            preparation._ACTIVE.reset(admission_token)


async def test_incomplete_address_result_cleans_full_scope_without_starting_profile():
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()

    @asynccontextmanager
    async def incomplete(*args):
        yield None

    with pytest.raises(RuntimeError, match="address_preparation_incomplete"):
        async with _prepare(
            fake,
            execution,
            fence,
            run_id="run-a",
            control_run_id=None,
            metrics={},
            nonprofile_admission=admission,
            address_preparation=incomplete,
        ):
            pytest.fail("incomplete address result yielded")
    assert "profile-enter" not in fake.events and not fake.db.relations
    assert preparation._ACTIVE.get() is None
    assert fake._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None


@pytest.mark.parametrize("failure", ["missing", "reservation", "digest"])
async def test_profile_scope_requires_its_distinct_original_admission(failure):
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    if failure != "missing":
        admission.profile_admission = SimpleNamespace(
            lease=SimpleNamespace(
                reservation_id=admission.lease.reservation_id if failure == "reservation" else "distinct",
                lease_digest=admission.lease.lease_digest if failure == "digest" else "different",
            )
        )
    with pytest.raises(RuntimeError, match="profile_admission_missing|profile_reservation_reused"):
        async with preparation._profile_scope(fake, execution, fence, "run-a", None, {}, admission):
            pytest.fail("missing or reused profile admission yielded")
    assert "profile-enter" not in fake.events and not fake.db.relations


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"dataset_pins": []}, "immutable"),
        ({"relation_overrides": []}, "immutable"),
        ({"dataset_pins": (object(),)}, "dataset pin is invalid"),
        ({"overlay_relation_oid": 0}, "overlay OID is invalid"),
        ({"address_alias_generation": True}, "alias generation is invalid"),
        ({"address_alias_generation": -1}, "alias generation is invalid"),
        ({"semantic_as_of": "2026-1-02"}, "semantic date is invalid"),
        ({"relation_overrides": (("provider_directory_location", "stage_a"),) * 2}, "relation override is invalid"),
    ],
)
def test_address_input_rejections_leave_preparation_context_empty(changes, reason):
    with pytest.raises(ValueError, match=reason):
        candidate.validate_preparation_input(replace(_address_inputs(), **changes))
    assert candidate.current() is None and not candidate.has_source_query()


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("source_id", " source ", "dataset identity is invalid"),
        ("endpoint_id", "", "dataset identity is invalid"),
        ("dataset_id", 1, "dataset identity is invalid"),
        ("acquisition_root_run_id", "x" * 161, "dataset identity is invalid"),
        ("dataset_hash", "A" * 64, "dataset hash is invalid"),
    ],
)
def test_dataset_pin_rejects_coercible_or_noncanonical_identity(field, value, reason):
    inputs = _address_inputs()
    bad_pin = replace(inputs.dataset_pins[0], **{field: value})
    with pytest.raises(ValueError, match=reason):
        candidate.validate_preparation_input(replace(inputs, dataset_pins=(bad_pin,)))


def test_one_endpoint_cannot_select_two_datasets_but_may_have_source_aliases():
    inputs = _address_inputs()
    first, second = inputs.dataset_pins
    second = replace(second, endpoint_id=first.endpoint_id)
    with pytest.raises(ValueError, match="source vector is ambiguous"):
        candidate.validate_preparation_input(replace(inputs, dataset_pins=(first, second)))
    second = replace(second, dataset_id=first.dataset_id)
    candidate.validate_preparation_input(replace(inputs, dataset_pins=(first, second)))


@pytest.mark.parametrize("active", ["physical", "query"])
def test_query_only_scope_cannot_nest_inside_an_active_address_scope(active):
    variable = candidate._PREPARATION if active == "physical" else candidate._SOURCE_QUERY
    original = _address_inputs() if active == "physical" else _query_inputs()
    token = variable.set(original)
    try:
        with pytest.raises(RuntimeError, match="source query scope is invalid"):
            with candidate.source_query_scope(_query_inputs()):
                pytest.fail("nested query scope yielded")
        assert variable.get() is original
    finally:
        variable.reset(token)


@pytest.mark.parametrize("changes", [{"overlay": object()}, {"semantic_as_of": None}])
def test_source_query_requires_exact_overlay_and_date(changes):
    with pytest.raises(RuntimeError, match="exact desired overlay"):
        with candidate.source_query_scope(replace(_query_inputs(), **changes)):
            pytest.fail("unsupported desired query yielded")
    assert not candidate.has_source_query()


@pytest.mark.parametrize("failure", ["not-pinned", "pin-count", "aliases", "overlay"])
async def test_selected_address_inputs_are_rechecked_before_use(monkeypatch, failure):
    native = candidate._native()
    inputs = _address_inputs()
    monkeypatch.setattr(native.db, "scalar", AsyncMock(return_value=0 if failure == "pin-count" else 42))
    monkeypatch.setattr(native, "_address_alias_generation", AsyncMock(return_value=1 if failure == "aliases" else 0))
    token = candidate._PREPARATION.set(inputs)
    try:
        if failure in {"not-pinned", "pin-count"}:
            selected = inputs.dataset_pins[0]
            with pytest.raises(RuntimeError, match="not pinned|dataset pins changed"):
                await candidate.assert_selected_dataset(
                    "test_schema",
                    selected.source_id,
                    "other" if failure == "not-pinned" else selected.dataset_id,
                    selected.acquisition_root_run_id,
                )
            if failure == "not-pinned":
                native.db.scalar.assert_not_awaited()
        else:
            with pytest.raises(RuntimeError, match="address aliases changed|overlay changed"):
                await candidate._assert_staged_overlay("test_schema", inputs)
            if failure == "aliases":
                native.db.scalar.assert_not_awaited()
    finally:
        candidate._PREPARATION.reset(token)


@pytest.mark.parametrize("failure", ["scope", "semantic-date", "finalizer"])
async def test_candidate_preparation_rejects_without_leaking_task_context(monkeypatch, failure):
    native = candidate._native()
    monkeypatch.setattr(native.db, "_transaction_binding", lambda: None)
    build = AsyncMock()
    finalize = AsyncMock(return_value=object())
    dependencies = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(native, "process_entity_address_unified_data", build)
    monkeypatch.setattr(native, "publish_entity_address_unified_generation", finalize)
    monkeypatch.setattr(candidate.prepared_doctors, "capture_dependencies", dependencies)
    if failure == "scope":
        token = candidate._SOURCE_QUERY.set(_query_inputs())
        try:
            with pytest.raises(RuntimeError, match="source query cannot prepare"):
                await candidate.prepare_provider_directory_entity_address({}, {}, preparation_input=_address_inputs())
        finally:
            candidate._SOURCE_QUERY.reset(token)
    elif failure == "semantic-date":
        admission = _admission()
        with pytest.raises(RuntimeError, match="semantic date differs"):
            await candidate.prepare_provider_directory_entity_address(
                {},
                {},
                preparation_input=_address_inputs(),
                admission=admission,
                native_input_hash=admission.plan.native_address_input_hash,
            )
    else:
        with pytest.raises(RuntimeError, match="did not prepare a generation"):
            await candidate.prepare_provider_directory_entity_address({}, {}, preparation_input=_address_inputs())
        build.assert_awaited_once()
    if failure != "finalizer":
        build.assert_not_awaited()
        finalize.assert_not_awaited()
        dependencies.assert_not_awaited()
    assert candidate.current() is None and candidate._NATIVE_DEPENDENCIES.get() is None
    assert admitted._ADMISSION.get() is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("temp_file_limit_bytes_per_backend", True),
        ("temp_file_limit_bytes_per_backend", 0),
        ("temp_file_limit_bytes_per_backend", 1025),
        ("worker_count", True),
        ("worker_count", 0),
    ],
)
def test_native_admission_rejects_invalid_bounds_before_lease_use(field, value):
    admission = _admission()
    admission.plan = replace(admission.plan, **{field: value})
    with pytest.raises(ValueError, match="temp bound is invalid"):
        admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)


@pytest.mark.parametrize("value", [None, 1, "A" * 64, "a" * 63, "f" * 64])
def test_native_admission_requires_exact_canonical_input_hash(value):
    with pytest.raises(ValueError, match="admission is invalid"):
        admitted._admitted_preparation(_admission(), value)
    assert admitted._admitted_preparation(None, None) is None


@pytest.mark.parametrize("failure", ["schema", "unowned", "replacement"])
async def test_owned_native_stage_guard_prevents_drop_of_unowned_or_replaced_heap(monkeypatch, failure):
    native = candidate._native()
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema = "test_schema"
    scope.owned_oids["stage_a"] = 41
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=42))
    monkeypatch.setattr(native, "db", database)
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="not created by this build|owned stage changed"):
            await admitted._lock_owned_stage(
                "other_schema" if failure == "schema" else "test_schema",
                "stage_b" if failure == "unowned" else "stage_a",
            )
        assert scope.owned_oids == {"stage_a": 41}
        assert not any("DROP" in call.args[0] for call in database.status.await_args_list)
        if failure != "replacement":
            database.status.assert_not_awaited()
            database.scalar.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.parametrize("retire_fails", [False, True])
async def test_native_drop_forgets_oid_only_after_storage_retirement(monkeypatch, retire_fails):
    native = candidate._native()
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema = "test_schema"
    scope.owned_oids["stage_a"] = 41
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=41), transaction=_transaction)
    monkeypatch.setattr(native, "db", database)
    admission.retire_external_relation = AsyncMock(
        side_effect=RuntimeError("retirement failed") if retire_fails else None
    )
    token = admitted._ADMISSION.set(scope)
    try:
        if retire_fails:
            with pytest.raises(RuntimeError, match="retirement failed"):
                await admitted.drop_stage("test_schema", "stage_a")
            assert scope.cleanup_oids == (("stage_a", "stage_a", 41),)
        else:
            await admitted.drop_stage("test_schema", "stage_a")
            assert not scope.owned_oids
        assert database.status.await_args.args == ("DROP TABLE test_schema.stage_a RESTRICT",)
        admission.retire_external_relation.assert_awaited_once()
    finally:
        admitted._ADMISSION.reset(token)


async def test_nested_native_worker_requires_its_owned_backend_and_releases_slot(monkeypatch):
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    monkeypatch.setattr(candidate._native().db, "_transaction_binding", lambda: None)
    token = admitted._ADMISSION.set(scope)
    try:
        async with admitted._worker_slot():
            with pytest.raises(RuntimeError, match="nested work requires its owned backend"):
                async with admitted._worker_slot():
                    pytest.fail("unbound nested worker yielded")
        assert not scope.worker_tasks and scope.workers._value == admission.plan.worker_count
    finally:
        admitted._ADMISSION.reset(token)


async def test_admitted_gather_retains_requested_per_index_exception_results():
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    error = RuntimeError("index failure")
    token = admitted._ADMISSION.set(scope)
    try:
        assert await admitted.gather(
            AsyncMock(return_value=1)(), AsyncMock(side_effect=error)(), return_exceptions=True
        ) == [1, error]
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.parametrize("failure", ["schema", "name"])
async def test_unadmitted_logging_never_requests_storage_authority(failure):
    admission = _admission()
    admission.before_logging = AsyncMock()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema = "test_schema"
    scope.stage_oids = (("target", "stage_a", 41),)
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="logging stage is not admitted"):
            await admitted.before_stage_logging(
                "other_schema" if failure == "schema" else "test_schema",
                "stage_b" if failure == "name" else "stage_a",
            )
        admission.before_logging.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.parametrize("failure", ["origin", "setting"])
async def test_native_execution_requires_binding_and_valid_sql_settings(monkeypatch, failure):
    native = candidate._native()
    connection = SimpleNamespace(status=AsyncMock())

    @asynccontextmanager
    async def acquire():
        yield connection

    database = SimpleNamespace(_transaction_binding=lambda: None, status=AsyncMock())
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(native, "_entity_address_sql_settings", lambda: [("work_mem", "1MB")])
    token = None
    if failure == "origin":
        admission = _admission()
        token = admitted._ADMISSION.set(
            admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
        )
    else:
        database.acquire = acquire
        connection.status.side_effect = [None, RuntimeError("unrelated tuning failure"), None, None]
    try:
        with pytest.raises(RuntimeError, match="binding unavailable|unrelated tuning failure"):
            await admitted._execute_tuned_status("SELECT expensive_work()")
        database.status.assert_not_awaited()
        assert not any("expensive_work" in call.args[0] for call in connection.status.await_args_list)
        if failure == "setting":
            assert [call.args[0] for call in connection.status.await_args_list][-2:] == [
                "ROLLBACK TO SAVEPOINT entity_address_sql_setting_0;",
                "RELEASE SAVEPOINT entity_address_sql_setting_0;",
            ]
    finally:
        if token is not None:
            admitted._ADMISSION.reset(token)


@pytest.mark.parametrize(
    "field,value",
    [
        ("worker_count", True),
        ("worker_count", 0),
        ("temp_file_limit_bytes_per_backend", 0),
        ("temp_file_limit_bytes_per_backend", 1025),
        ("run_id", "run_invalid"),
    ],
)
def test_address_factory_rejects_invalid_execution_geometry(address_build_case, field, value):
    options_by_name = {
        "run_id": "run_" + "2" * 32,
        "worker_count": 2,
        "temp_file_limit_bytes_per_backend": 1024,
        field: value,
    }
    with pytest.raises(ValueError, match="execution_bounds_invalid|control_run_invalid"):
        address.cms_address_preparation(
            *address_build_case[:4], native_input_fence=address_build_case[4], **options_by_name
        )


@pytest.mark.parametrize("failure", ["operation", "cms-source"])
def test_address_factory_requires_a_publish_selection_with_cms(address_build_case, failure):
    if failure == "operation":
        address_build_case[1].attestation.operation = "purge"
    else:
        address_build_case[2].datasets = address_build_case[2].datasets[1:]
    with pytest.raises(RuntimeError, match="publish_selection_required"):
        _address_factory(address_build_case)


@pytest.mark.parametrize("overlay", [None, SimpleNamespace(oid=True), SimpleNamespace(oid=0)])
def test_address_factory_requires_captured_staged_overlay(address_build_case, overlay):
    with pytest.raises(RuntimeError, match="staged_overlay_required"):
        address._preparation_inputs(address_build_case[2], {}, overlay, address_build_case[3], "2026-01-02")


async def test_address_finalizer_returning_none_never_yields_or_claims_cleanup(address_build_case, monkeypatch):
    factory = _address_factory(address_build_case)
    monkeypatch.setattr(address, "capture_native_dependencies", AsyncMock(return_value=address_build_case[3]))
    monkeypatch.setattr(address, "_new_context", AsyncMock(return_value={}))
    monkeypatch.setattr(address, "prepare_provider_directory_entity_address", AsyncMock(return_value=None))
    cleanup = AsyncMock()
    monkeypatch.setattr(address, "cleanup_prepared_entity_address_generation", cleanup)
    with pytest.raises(RuntimeError, match="preparation_incomplete"):
        async with factory(
            address_build_case[2], {}, SimpleNamespace(oid=41, relation="overlay_stage"), _address_admission(factory)
        ):
            pytest.fail("missing finalized generation yielded")
    cleanup.assert_not_awaited()


async def test_empty_native_stage_inventory_gets_a_fresh_owned_context(monkeypatch):
    native = candidate._native()

    async def startup(context):
        context.update(context={}, import_date="shared")

    monkeypatch.setattr(native, "startup", startup)
    monkeypatch.setattr(native.db, "scalar", AsyncMock(return_value=None))
    context = await address._new_context("run_" + "2" * 32, "test_schema")
    assert context["import_date"].startswith("cms") and context["import_date"] != "shared"
    assert native.db.scalar.await_count == len(native.SUPPORT_TABLE_MODELS) + 4


@pytest.mark.parametrize("failure", ["version", "relations", "geo", "authority", "facility"])
def test_native_input_support_requires_complete_pins_and_publication_families(address_build_case, failure):
    fence = deepcopy(address_build_case[4])
    if failure == "version":
        fence["version"] = 2
    elif failure == "relations":
        fence["relations"].pop(next(iter(fence["relations"])))
    elif failure == "geo":
        fence["geo_bindings"] = None
    elif failure == "authority":
        fence["reference_authorities"].pop("mrf")
    else:
        fence["relations"]['"fixture"."facility_anchor"']["relation_oid"] = 41
    with pytest.raises(RuntimeError, match="input_fence_invalid|geo_inputs_unavailable|publication_unavailable"):
        native_inputs.require_supported_native_address_inputs(fence)
    if failure == "facility":
        fence["reference_authorities"]["facility-anchors"] = {}
        native_inputs.require_supported_native_address_inputs(fence)


@pytest.mark.parametrize("changes", [{"relkind": "v"}, {"relpersistence": "u"}, {"inherited": True}])
def test_native_inputs_cannot_adopt_views_temporary_or_inherited_storage(changes):
    relation_by_field = {"relation_oid": 41, "relkind": "r", "relpersistence": "p", "inherited": False, **changes}
    with pytest.raises(RuntimeError, match="persistent_heap"):
        native_inputs._require_heaps({"input": relation_by_field})


async def test_missing_geo_input_does_not_fabricate_projection_bindings():
    assert native_inputs._geo_bindings("test_schema", {'"test_schema"."npi_address"': {"relation_oid": None}}) is None


@pytest.mark.parametrize("failure", ["none", "duplicate", "different-desired", "fence-vector"])
def test_desired_cms_pair_and_selected_vector_are_unique(failure):
    cms, _, execution = _desired_fixture()
    pair = execution.attestation.pairs[0]
    if failure == "none":
        execution.attestation.pairs = ()
    elif failure == "duplicate":
        execution.attestation.pairs = (pair, pair)
    elif failure == "different-desired":
        execution.attestation.desired_cms_dataset = {**pair, "dataset_id": "different"}
    if failure == "fence-vector":
        with pytest.raises(RuntimeError, match="candidate_selection_changed"):
            desired._assert_cms_fence(execution, SimpleNamespace(datasets=(cms, cms)), pair)
    else:
        with pytest.raises(RuntimeError, match="selection_invalid"):
            desired._cms_pair(execution)


@pytest.mark.parametrize("failure", ["binding", "read-only", "isolation"])
async def test_retained_dependencies_require_read_only_repeatable_snapshot(failure):
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=["off"] if failure == "read-only" else ["on", "read committed"])
    )
    backend = SimpleNamespace(
        db=SimpleNamespace(
            _transaction_binding=lambda: None if failure == "binding" else SimpleNamespace(session=session)
        )
    )
    with pytest.raises(RuntimeError, match="dependency_read_snapshot_required"):
        await desired._prepare_in_snapshot(backend, object(), "run-a", {}, set())


@pytest.mark.parametrize("failure", ["query-tail", "complete", "count-bool", "count-negative"])
def test_retained_relation_proof_requires_exact_query_and_noncoercible_counts(failure):
    fake, *_ = _fake_fhir([])
    spec = (
        "dataset_affiliation_organization",
        "metadata",
        1,
        ("input_count",),
        lambda **kwargs: "SELECT 1",
        lambda: "SELECT 2",
        "edges",
        ("left_id", "right_id"),
    )
    if failure == "query-tail":
        with pytest.raises(RuntimeError, match="dependency_query_changed"):
            desired._live_edge_proof_sql(fake, spec)
        return
    dataset = SimpleNamespace(dataset_id="example", evidence_run_id="root")
    proof_by_field = {
        "complete": True,
        "version": 1,
        "dataset_id": "example",
        "acquisition_root_run_id": "root",
        "input_count": 1,
        "edge_count": 1,
        "replaced_edge_count": 0,
    }
    proof_by_field.update(
        {"complete": False} if failure == "complete" else {"input_count": True if failure == "count-bool" else -1}
    )
    with pytest.raises(RuntimeError, match="dependency_proof_invalid"):
        desired._assert_retained_proof_shape(proof_by_field, dataset, spec)


@pytest.mark.parametrize("failure", ["count", "column", "opclass", "expression"])
def test_native_index_keys_require_declared_columns_operator_class_and_expression(failure):
    index_by_field = {"indkey": "1", "opclass_names": ["text_ops"]}
    declaration_by_field = {"index_elements": ("checksum",)}
    if failure == "count":
        index_by_field["indkey"] = "1 2"
    elif failure == "column":
        declaration_by_field = {"index_elements": ("other_column",)}
    elif failure == "opclass":
        declaration_by_field = {"index_elements": ("checksum public.gin_trgm_ops",)}
    else:
        declaration_by_field = {"index_elements": ("lower(checksum)",)}
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        layout._assert_index_keys(index_by_field, declaration_by_field, {1: "checksum"})


@pytest.mark.parametrize("failure", ["identity", "triggers", "constraint", "index-details"])
async def test_native_layout_rejects_catalog_drift_before_returning_fingerprint(failure):
    name = "entity_address_unified_cms" + "a" * 20
    relation = preparation.OwnedRelation("test_schema", name, 41, 10, "u")
    relation_by_field = {"schema_name": "test_schema", "relation_name": name}
    attributes, indexes, constraints, triggers = [], [], [], []
    if failure == "identity":
        relation_by_field["relation_name"] = "replacement"
    elif failure == "triggers":
        triggers = [{"trigger": "unexpected"}]
    elif failure == "constraint":
        constraints = [{"constraint_type": "f", "condeferrable": False, "condeferred": False, "convalidated": True}]
    else:
        indexes = [{"index_oid": 51}]
    backend = SimpleNamespace(
        db=SimpleNamespace(all=AsyncMock(return_value=[])),
        _profile_capacity_relation_row=AsyncMock(return_value=(relation_by_field, None)),
        _profile_capacity_relation_catalog=AsyncMock(return_value=(attributes, indexes, constraints, triggers)),
        _pagination_checkpoint_row_mapping=lambda value: value,
    )
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_native_layout(backend, relation, ("entity_address_unified",))


@pytest.mark.parametrize("value", [None, True, -1])
async def test_archive_requires_registered_nonnegative_revision(value):
    with pytest.raises(RuntimeError, match="input_not_registered"):
        await archive._revision(SimpleNamespace(scalar=AsyncMock(return_value=value)), "test_schema", 41)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"attname": "address_key", "attgenerated": "s", "attidentity": ""}],
        [{"attname": "address_key", "attgenerated": "", "attidentity": "a"}],
    ],
)
async def test_archive_rejects_unsupported_columns_before_copy(rows):
    backend = SimpleNamespace(all=AsyncMock(return_value=[SimpleNamespace(_mapping=row) for row in rows]))
    with pytest.raises(RuntimeError, match="columns_unsupported"):
        await archive._columns(backend, 41)


@pytest.mark.parametrize("failure", ["unbound", "other-session", "inactive"])
def test_archive_publication_requires_the_callers_active_session(failure):
    session = SimpleNamespace(in_transaction=lambda: failure != "inactive")
    backend = SimpleNamespace(
        db=SimpleNamespace(
            _transaction_binding=lambda: (
                None
                if failure == "unbound"
                else SimpleNamespace(session=object() if failure == "other-session" else session)
            )
        )
    )
    with pytest.raises(RuntimeError, match="requires_owner_transaction"):
        archive._require_owner(backend, session)


@pytest.mark.parametrize("failure", ["seal-function", "seal-trigger"])
async def test_archive_read_seal_requires_exact_function_and_trigger(failure):
    backend = SimpleNamespace(scalar=AsyncMock(side_effect=[None] if failure == "seal-function" else [51, False]))
    with pytest.raises(RuntimeError, match="write_seal_changed"):
        await archive._assert_backend_seal(backend, "test_schema", 41)
    assert backend.scalar.await_count == (1 if failure == "seal-function" else 2)


@pytest.mark.parametrize("failure", ["name", "input-trigger", "identity", "constraint"])
async def test_archive_layout_rejects_unowned_or_unsupported_catalog(failure):
    name = "cms_archive_" + "a" * 32 + ("_input" if failure == "input-trigger" else "_delta")
    if failure == "name":
        name = "archive_live"
    relation = preparation.OwnedRelation("test_schema", name, 41, 10, "u")
    relation_by_field = {
        "schema_name": "test_schema",
        "relation_name": "replacement" if failure == "identity" else name,
    }
    backend = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(return_value=1 if failure == "input-trigger" else 0)),
        _profile_capacity_relation_row=AsyncMock(return_value=(relation_by_field, None)),
        _profile_capacity_relation_catalog=AsyncMock(
            return_value=([], [], [{"constraint_type": "f", "condeferrable": False}], [])
        ),
    )
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await archive.capture_archive_layout(backend, relation)
    if failure == "name":
        backend.db.scalar.assert_not_awaited()


async def test_archive_cleanup_detects_replacement_after_lock_and_never_drops_it(monkeypatch):
    fake, *_ = _fake_fhir([])
    admission = _admission()
    fake.db.status = AsyncMock()
    monkeypatch.setattr(archive, "_oid", AsyncMock(side_effect=[41, 42]))
    admission.retire_external_relation = AsyncMock()
    with pytest.raises(RuntimeError, match="cleanup_identity_changed"):
        await archive._cleanup(fake, admission, "test_schema", [("delta_stage", 41, "TABLE")])
    assert not any("DROP" in call.args[0] for call in fake.db.status.await_args_list)
    admission.retire_external_relation.assert_not_awaited()


@pytest.mark.parametrize("operation", ["publication", "completion"])
async def test_cutover_cannot_start_without_both_authority_callbacks(operation):
    admission = await _started_admission()
    fake, *_ = _fake_fhir([])
    if operation == "publication":
        with pytest.raises(RuntimeError, match="cutover_authorization_required"):
            async with admission.publication(fake, "test_schema"):
                pytest.fail("unauthorized publication yielded")
    else:
        admission._cutover_active = True
        with pytest.raises(RuntimeError, match="cutover_authorization_required"):
            await admission.assert_cutover_complete()
    assert not fake.db.relations


async def test_expired_paired_build_deadline_rejects_before_scratch():
    admission = _admission()
    fake, *_ = _fake_fhir([])
    admission.paired_profile_lease = replace(admission.lease, max_build_deadline=admission.lease.issued_at)
    with pytest.raises(RuntimeError, match="build_deadline_reached"):
        await preparation.remaining_build_seconds(fake, admission)
    assert not fake.db.relations


async def test_occupied_full_scope_is_not_adopted_or_deleted():
    fake, _, fence, projection = _fake_fhir([])
    fake._artifact_scope_relation_identities = AsyncMock(return_value={"occupied": (41, "r")})
    admission = await _started_admission()
    with pytest.raises(RuntimeError, match="scope_already_exists"):
        async with preparation._full_scope(fake, fence, frozenset({"Practitioner"}), projection, admission):
            pytest.fail("occupied scope yielded")
    assert not fake.materialized and not admission._relations and not fake.events


async def test_full_scope_reports_original_and_cleanup_failures_and_restores_contexts(monkeypatch):
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    fake._materialize_artifact_scope_payload = AsyncMock(side_effect=RuntimeError("build failure"))
    monkeypatch.setattr(preparation, "_drain_owned_cleanup", AsyncMock(side_effect=RuntimeError("cleanup failure")))
    with pytest.raises(BaseExceptionGroup, match="scope_and_cleanup_failed") as caught:
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ):
            pytest.fail("failed build yielded")
    assert [str(error) for error in caught.value.exceptions] == ["build failure", "cleanup failure"]
    assert fake.db.relations and admission._relations
    assert preparation._ACTIVE.get() is None
    assert fake._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None
    assert fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None


async def test_owned_cleanup_drains_repeated_cancellation_before_propagating(monkeypatch):
    fake, *_ = _fake_fhir([])
    admission = _admission()
    entered, release = asyncio.Event(), asyncio.Event()
    removed_names = []

    async def remove(_fhir, _admission, _schema, name):
        entered.set()
        await release.wait()
        removed_names.append(name)

    monkeypatch.setattr(preparation, "_drop_owned_relation", remove)
    owner = asyncio.create_task(preparation._drain_owned_cleanup(fake, admission, "test_schema", ["first", "last"]))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        owner.cancel()
        await asyncio.sleep(0)
        owner.cancel()
        await asyncio.sleep(0)
        assert not owner.done()
    finally:
        release.set()
        if not owner.done() and not owner.cancelling():
            owner.cancel()
        try:
            await asyncio.wait_for(owner, timeout=2)
        except asyncio.CancelledError:
            assert owner.done()
    assert owner.cancelled()
    assert removed_names == ["last", "first"]


async def test_missing_profile_metrics_never_consume_prepared_bundles():
    fake, execution, fence, _ = _fake_fhir([])
    async with _prepare(
        fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=_admission()
    ) as prepared:
        prepared.metrics["profile"] = None
        profile_result_by_field = {
            "selection_proof_id": "proof-a",
            "operation": "publish",
            "status": "published",
            "generation_id": "generation-a",
            "profile_as_of": "2026-09-29",
            "evidence_rows": 1,
            "profile_rows": 1,
        }
        with pytest.raises(RuntimeError, match="profile_metrics_missing"):
            await prepared.mark_committed(profile_result=profile_result_by_field)
        assert not prepared.nonprofile_bundle.promoted and not prepared.profile_bundle.promoted
    assert not fake.db.relations


@pytest.mark.parametrize("same_field", ["reservation_id", "lease_digest"])
async def test_profile_reservation_reuse_fails_before_full_build(same_field):
    fake, execution, fence, _ = _fake_fhir([])
    admission = _admission()
    lease = SimpleNamespace(reservation_id="distinct", lease_digest="different")
    setattr(lease, same_field, getattr(admission.lease, same_field))
    fake._admit_provider_directory_profile_capacity = AsyncMock(return_value=SimpleNamespace(lease=lease))
    with pytest.raises(RuntimeError, match="profile_reservation_reused"):
        async with _prepare(
            fake, execution, fence, run_id="run-a", control_run_id=None, metrics={}, nonprofile_admission=admission
        ):
            pytest.fail("reused paired reservation yielded")
    assert not fake.db.relations and not fake.materialized


@pytest.mark.parametrize("failure", ["dataset", "source", "run", "profile-active", "nonprofile-active"])
async def test_purge_rejects_nonempty_or_owned_scope_before_admission(failure):
    fake, execution, fence, _ = _fake_fhir([])
    execution.attestation.operation = "purge"
    fence.datasets, fence.source_ids = (), ()
    run_id = "run-a"
    variable = token = None
    if failure == "dataset":
        fence.datasets = (object(),)
    elif failure == "source":
        fence.source_ids = ("cms-npd",)
    elif failure == "run":
        run_id = ""
    else:
        variable = (
            fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION if failure == "profile-active" else preparation._ACTIVE
        )
        token = variable.set(object())
    try:
        with pytest.raises(RuntimeError, match="purge_scope_invalid"):
            async with _prepare(fake, execution, fence, run_id=run_id, control_run_id=None, metrics={}):
                pytest.fail("invalid purge yielded")
        assert not fake.events and not fake.db.relations
    finally:
        if token is not None:
            variable.reset(token)


async def test_missing_overlay_never_reads_or_invents_relation_identity():
    fake, *_ = _fake_fhir([])
    admission = _admission()
    admission.measure = AsyncMock()
    with pytest.raises(RuntimeError, match="overlay_missing"):
        await preparation._overlay_identity(fake, SimpleNamespace(relation_overrides={}), admission)
    admission.measure.assert_not_awaited()


def test_untyped_preparation_input_cannot_enter_scope():
    with pytest.raises(ValueError, match="preparation input is invalid"):
        candidate.validate_preparation_input(object())
    assert candidate.current() is None


def test_query_only_source_vector_cannot_be_mutable():
    with pytest.raises(ValueError, match="inputs must be immutable"):
        with candidate.source_query_scope(replace(_query_inputs(), dataset_pins=[])):
            pytest.fail("mutable source query yielded")
    assert not candidate.has_source_query()


@pytest.mark.parametrize("failure", ["schema", "missing", "foreign-oid"])
async def test_native_registration_requires_all_stages_created_by_its_scope(monkeypatch, failure):
    native = candidate._native()
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema = None if failure == "schema" else "test_schema"
    scope.owned_oids = {"stage_a": 42 if failure == "foreign-oid" else 41}
    monkeypatch.setattr(candidate, "_cutover_plan", lambda *args: ([object()], [], [], []))
    monkeypatch.setattr(
        candidate,
        "_capture_stage_oids",
        AsyncMock(return_value=() if failure == "missing" else (("target", "stage_a", 41),)),
    )
    admission.register_address_stages = AsyncMock()
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="no owned stages|stage is missing|not created by this build"):
            await candidate._register_admitted_stages({"import_date": "example"})
        admission.register_address_stages.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.parametrize("failure", ["missing", "admitted-drift"])
async def test_finalized_address_family_rejects_missing_or_changed_stage_before_logging(monkeypatch, failure):
    inputs = _address_inputs()
    pin_check, overlay_check, index_check = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(candidate, "assert_dataset_pins", pin_check)
    monkeypatch.setattr(candidate, "_assert_staged_overlay", overlay_check)
    monkeypatch.setattr(candidate, "_require_stage_indexes", index_check)
    monkeypatch.setattr(candidate, "_cutover_plan", lambda *args: ([object()], [], [], []))
    monkeypatch.setattr(
        candidate,
        "_capture_stage_oids",
        AsyncMock(return_value=() if failure == "missing" else (("target", "stage_a", 41),)),
    )
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema = "test_schema"
    scope.stage_oids = (("target", "stage_a", 42),)
    input_token = candidate._PREPARATION.set(inputs)
    admission_token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="stage is missing|stages changed after admission"):
            await candidate.prepare_finalized_generation("test_schema", object(), {}, context={})
        index_check.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(admission_token)
        candidate._PREPARATION.reset(input_token)


@pytest.mark.parametrize("failure", ["committed", "receipt"])
async def test_prepared_native_publication_requires_unconsumed_stage_and_result_authority(monkeypatch, failure):
    native = candidate._native()
    monkeypatch.setattr(native, "require_caller_owned_cutover_transaction", lambda db: None)
    cutover = AsyncMock()
    monkeypatch.setattr(native, "_run_entity_address_cutover", cutover)
    prepared = SimpleNamespace(
        committed=failure == "committed",
        db_schema="test_schema",
        swaps=[],
        patch_statements=[],
        relation_names=[],
        required_names=[],
        context={},
        native_receipt=None,
    )
    with pytest.raises(RuntimeError, match="already committed|requires native result authority"):
        await candidate.publish_prepared_entity_address_generation(prepared)
    if failure == "committed":
        cutover.assert_not_awaited()
    else:
        cutover.assert_awaited_once()
    assert prepared.context == {}


async def test_candidate_cleanup_waits_for_outer_transaction_before_any_ddl(monkeypatch):
    monkeypatch.setattr(candidate._native().db, "_transaction_binding", lambda: object())
    cleanup = AsyncMock()
    monkeypatch.setattr(candidate, "_drain_owned_stages", cleanup)
    with pytest.raises(RuntimeError, match="outer transaction to finish"):
        await candidate.cleanup_prepared_entity_address_generation(SimpleNamespace())
    cleanup.assert_not_awaited()


@pytest.mark.parametrize("failure", ["identifier", "schema", "duplicate"])
async def test_owned_create_rejects_unsafe_or_conflicting_names_before_ddl(failure):
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema = "test_schema"
    scope.owned_oids = {"stage_a": 41}
    create = AsyncMock()
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises((ValueError, RuntimeError), match="relation is invalid|ownership changed"):
            await admitted._create_owned_relation(
                "other_schema" if failure == "schema" else "test_schema",
                "unsafe;table" if failure == "identifier" else "stage_a",
                create,
            )
        create.assert_not_awaited()
        assert scope.owned_oids == {"stage_a": 41}
    finally:
        admitted._ADMISSION.reset(token)


async def test_owned_backend_lock_rechecks_each_creation_time_oid(monkeypatch):
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema, scope.owned_oids = "test_schema", {"stage_a": 41}
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=42))
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="owned stage changed"):
            await admitted.lock_owned_relations(database)
        assert scope.owned_oids == {"stage_a": 41}
        assert database.status.await_args.args == ("LOCK TABLE test_schema.stage_a IN ACCESS SHARE MODE NOWAIT",)
    finally:
        admitted._ADMISSION.reset(token)


async def test_bound_validation_uses_one_parent_worker_and_releases_it(monkeypatch):
    native = candidate._native()
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    database = SimpleNamespace(
        transaction=_transaction, scalar=AsyncMock(return_value=True), _transaction_binding=lambda: object()
    )
    monkeypatch.setattr(native, "db", database)

    @asynccontextmanager
    async def tuned(*args):
        yield

    monkeypatch.setattr(native, "entity_address_tuned_transaction", tuned)
    tasks = []

    async def read(value):
        tasks.append(asyncio.current_task())
        return value

    token = admitted._ADMISSION.set(scope)
    try:
        assert await admitted.validation_operations(database, lambda: read(1), lambda: read(2)) == (1, 2)
        assert tasks == [asyncio.current_task()] * 2 and not scope.worker_tasks
        assert scope.workers._value == admission.plan.worker_count
        assert database.scalar.await_count == 2
    finally:
        admitted._ADMISSION.reset(token)


async def test_logging_rechecks_stage_oid_after_exclusive_lock(monkeypatch):
    native = candidate._native()
    admission = _admission()
    admission.before_logging = AsyncMock()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    scope.db_schema, scope.stage_oids = "test_schema", (("target", "stage_a", 41),)
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(side_effect=[True, 42]))
    monkeypatch.setattr(native, "db", database)

    @asynccontextmanager
    async def tuned(*args):
        yield

    monkeypatch.setattr(admitted, "native_transaction", tuned)
    monkeypatch.setattr(native, "entity_address_tuned_transaction", tuned)
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="logging stage changed"):
            async with admitted.stage_logging_scope("test_schema", "stage_a"):
                pytest.fail("replacement stage was authorized for logging")
        assert database.status.await_args.args == ("LOCK TABLE test_schema.stage_a IN ACCESS EXCLUSIVE MODE NOWAIT",)
        assert scope.stage_oids == (("target", "stage_a", 41),)
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.parametrize("operation", ["capture", "register"])
async def test_native_fence_rejects_identity_changed_after_locks_before_authority_reads(monkeypatch, operation):
    original_relations_by_name = {
        '"test_schema"."input"': {"relation_oid": 41, "relkind": "r", "relpersistence": "p", "inherited": False}
    }
    changed = deepcopy(original_relations_by_name)
    changed['"test_schema"."input"']["relation_oid"] = 42
    monkeypatch.setattr(
        native_inputs, "_physical_relations", AsyncMock(side_effect=[original_relations_by_name, changed])
    )
    lock = AsyncMock()
    monkeypatch.setattr(native_inputs, "_lock_inputs", lock)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(RuntimeError, match="native_inputs_changed"):
        if operation == "capture":
            await native_inputs.capture_native_address_input_fence(session, "test_schema", cutover=True)
        else:
            await native_inputs.register_native_address_inputs(session, "test_schema")
    lock.assert_awaited_once()
    session.execute.assert_not_awaited()


async def test_native_snapshot_rejects_read_committed_before_catalog_reads(monkeypatch):
    physical = AsyncMock()
    monkeypatch.setattr(native_inputs, "_physical_relations", physical)
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "read committed")))
    with pytest.raises(RuntimeError, match="native_snapshot_required"):
        await native_inputs.capture_native_address_input_fence(session, "test_schema")
    physical.assert_not_awaited()


async def test_native_revision_guard_cannot_substitute_for_registration(monkeypatch):
    guard_by_field = {
        "relation_oid": 41,
        "tgtype": 60,
        "tgenabled": "A",
        "tgnargs": 0,
        "unconditional": True,
        "prosecdef": True,
        "returns_trigger": True,
        "proconfig": ["search_path=pg_catalog"],
        "lanname": "plpgsql",
        "prosrc": native_inputs._advance_body("test_schema"),
    }
    monkeypatch.setattr(native_inputs, "_require_ledger_guards", AsyncMock())
    results = [
        SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [guard_by_field])),
        SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [])),
    ]
    session = SimpleNamespace(execute=AsyncMock(side_effect=results))
    with pytest.raises(RuntimeError, match="native_input_not_registered"):
        await native_inputs._revision_guards(session, "test_schema", {"input": {"relation_oid": 41}})


def test_native_evidence_heap_declarations_include_exact_shard_and_location_keys():
    name = "entity_address_unified_cms" + "a" * 20 + "_evidence"
    declarations = layout._declarations(name, layout.ENTITY_ADDRESS_RESULT_MODELS[0])
    assert list(declarations.values()) == [{"index_elements": ("evidence_shard", "location_key")}]


@pytest.mark.parametrize("failure", ["unowned", "persistence"])
async def test_unowned_native_layout_never_reads_catalog(failure):
    relation = preparation.OwnedRelation(
        "test_schema",
        "unowned" if failure == "unowned" else "entity_address_unified_cms" + "a" * 20,
        41,
        10,
        "t" if failure == "persistence" else "u",
    )
    row = AsyncMock()
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_native_layout(
            SimpleNamespace(_profile_capacity_relation_row=row), relation, ("entity_address_unified",)
        )
    row.assert_not_awaited()


@pytest.mark.parametrize("failure", ["toast", "primary", "undeclared"])
def test_native_indexes_cannot_adopt_wrong_toast_primary_or_undeclared_shape(failure):
    name = "entity_address_unified_cms" + "a" * 20
    model = layout.ENTITY_ADDRESS_RESULT_MODELS[0]
    relation = preparation.OwnedRelation("test_schema", name, 41, 10, "u")
    index_by_field = {
        "indisvalid": True,
        "indisready": True,
        "indislive": True,
        "indimmediate": True,
        "indisexclusion": False,
        "indisreplident": False,
        "relfilenode": 51,
        "index_persistence": "u",
        "index_schema": "test_schema",
        "relation_oid": 42 if failure == "toast" else 41,
        "index_am": "btree",
        "indisunique": failure != "undeclared",
        "indisprimary": failure == "primary",
        "indkey": "1",
        "indnatts": 1,
        "indnkeyatts": 1,
        "index_predicate": None,
        "index_expressions": None,
        "index_name": "undeclared_index",
    }
    attributes = [{"relation_oid": 41, "attnum": 1, "attname": "unrelated_column"}]
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        layout._assert_indexes([index_by_field], attributes, relation, model, 42 if failure == "toast" else None)


async def test_desired_dependency_proof_rejects_live_canonical_drift(monkeypatch):
    fake, *_ = _fake_fhir([])
    dataset = SimpleNamespace(dataset_id="example", evidence_run_id="root")
    spec = (
        "dataset_affiliation_organization",
        "metadata",
        1,
        ("input_count",),
        lambda **kwargs: "SELECT 1",
        lambda: "SELECT 1",
        "edges",
        ("left_id", "right_id"),
    )
    proof_by_field = {
        "complete": True,
        "version": 1,
        "dataset_id": "example",
        "acquisition_root_run_id": "root",
        "input_count": 1,
        "edge_count": 1,
        "replaced_edge_count": 0,
    }
    fake.db.first = AsyncMock(return_value={"edges_match": True})
    fake._validated_dataset_affiliation_organization_proof = lambda *args, **kwargs: {**proof_by_field, "edge_count": 2}
    with pytest.raises(RuntimeError, match="dependency_proof_changed"):
        await desired._validated_live_relation(fake, dataset, proof_by_field, spec)


def _archive_delta(admission):
    return archive.PreparedArchiveDelta(
        "test_schema",
        "delta_stage",
        41,
        "effective_view",
        42,
        43,
        0,
        admission.plan.native_address_input_hash,
        ("address_key", "city"),
        1,
        "a" * 64,
        "SELECT archive",
        51,
        admission,
    )


@pytest.mark.parametrize("failure", ["transaction", "result", None])
async def test_archive_ownership_is_consumed_only_by_exact_committed_result(failure):
    admission = _admission()
    prepared = _archive_delta(admission)
    fake, *_ = _fake_fhir([])
    fake.db.transaction_active = failure == "transaction"
    result_by_field = {
        "target_oid": 43,
        "from_revision": 0,
        "to_revision": 2,
        "native_input_hash": admission.plan.native_address_input_hash,
        "delta_rows": 1,
        "delta_sha256": "a" * 64,
    }
    if failure == "result":
        result_by_field["delta_rows"] = 2
    if failure:
        with pytest.raises(RuntimeError, match="commit_result_changed"):
            await prepared.mark_committed(fake, result_by_field)
        assert not prepared.committed
    else:
        await prepared.mark_committed(fake, result_by_field)
        assert prepared.committed


@pytest.mark.parametrize("failure", ["write-lock", "revision"])
async def test_archive_apply_requires_write_intent_and_exact_revision_increment(monkeypatch, failure):
    prepared = _archive_delta(_admission())
    session = SimpleNamespace(
        in_transaction=lambda: True, scalar=AsyncMock(return_value=failure != "write-lock"), execute=AsyncMock()
    )
    backend = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session)),
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _q=lambda name: '"' + name + '"',
    )
    prepared.assert_ready = AsyncMock()
    monkeypatch.setattr(archive, "_revision", AsyncMock(return_value=1))
    with pytest.raises(RuntimeError, match="write_lock_required|publication_revision_changed"):
        await prepared.apply(backend, session)
    if failure == "write-lock":
        session.execute.assert_not_awaited()
        prepared.assert_ready.assert_not_awaited()
    else:
        session.execute.assert_awaited_once()
    assert not prepared.committed


async def test_archive_unknown_read_transport_cannot_claim_transaction_ownership():
    with pytest.raises(RuntimeError, match="read_requires_owner_transaction"):
        archive._require_backend(SimpleNamespace(_transaction_binding=lambda: object()))


async def test_archive_missing_owned_heap_retires_registration_without_drop(monkeypatch):
    fake, *_ = _fake_fhir([])
    admission = await _started_admission()
    admission._relations[("test_schema", "delta_stage")] = 41
    admission._external_relations.add(("test_schema", "delta_stage"))
    fake.db.status = AsyncMock()
    monkeypatch.setattr(archive, "_oid", AsyncMock(return_value=None))
    await archive._cleanup(fake, admission, "test_schema", [("delta_stage", 41, "TABLE")])
    assert not admission._relations and not admission._external_relations
    fake.db.status.assert_not_awaited()


@pytest.mark.parametrize("limit", [True, 0, -1024, 1025])
async def test_nonprofile_sql_rejects_invalid_spill_bound_before_clock_or_backend_use(limit):
    admission = _admission()
    admission.plan = replace(admission.plan, temp_file_limit_bytes_per_backend=limit)
    fake = SimpleNamespace(_profile_capacity_preflight_clock=AsyncMock(), db=SimpleNamespace(status=AsyncMock()))
    with pytest.raises(RuntimeError, match="nonprofile_temp_limit_invalid"):
        async with preparation.nonprofile_sql_transaction(fake, admission):
            pytest.fail("invalid SQL bound yielded")
    fake._profile_capacity_preflight_clock.assert_not_awaited()
    fake.db.status.assert_not_awaited()


@pytest.mark.parametrize("resume_fails", [False, True])
async def test_profile_handoff_resumes_the_original_distinct_admission_and_restores_scope(resume_fails):
    fake, execution, fence, _projection = _fake_fhir([])
    admission = _admission()
    original = SimpleNamespace(lease=SimpleNamespace(reservation_id="profile-distinct", lease_digest="cd" * 32))
    admission.profile_admission = original
    admission.resume_profile = AsyncMock(
        side_effect=RuntimeError("profile resumption failed") if resume_fails else None, return_value=original
    )
    token = preparation._ACTIVE.set(admission)
    try:
        if resume_fails:
            with pytest.raises(RuntimeError, match="profile resumption failed"):
                async with preparation._profile_scope(fake, execution, fence, "run-a", None, {}, admission):
                    pytest.fail("failed Profile resumption yielded")
            assert fake.events == []
        else:
            async with preparation._profile_scope(fake, execution, fence, "run-a", None, {}, admission) as (
                bundle,
                metrics,
            ):
                assert fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is original
                assert preparation._ACTIVE.get() is None
                assert bundle.profile_delta.refresh_source_ids == ("cms-npd",)
                assert metrics == {"profile": {"prepared": True}}
            assert fake.events == ["profile-enter", "profile-exit"]
        assert preparation._ACTIVE.get() is admission
        assert fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
        assert admission.resume_profile.await_args.args[0] is original
        assert admission.resume_profile.await_args.args[1].source_ids == ("cms-npd",)
        assert admission.resume_profile.await_args.args[2] == frozenset({"Practitioner"})
    finally:
        preparation._ACTIVE.reset(token)


async def test_missing_creation_oid_is_not_registered_as_an_owned_native_heap(monkeypatch):
    native = candidate._native()
    admission = _admission()
    admission.register_external_relation = AsyncMock()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    database = SimpleNamespace(transaction=_transaction, scalar=AsyncMock(return_value=None))
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(native, "_apply_entity_address_transaction_settings", AsyncMock())
    create = AsyncMock()
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="created stage is missing"):
            await admitted._create_owned_relation("test_schema", "stage_a", create)
        create.assert_awaited_once()
        admission.register_external_relation.assert_not_awaited()
        assert scope.owned_oids == {} and not scope.worker_tasks
        assert scope.workers._value == admission.plan.worker_count
    finally:
        admitted._ADMISSION.reset(token)


async def test_failed_admitted_sibling_build_is_drained_before_failure_returns():
    admission = _admission()
    scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
    started, release = asyncio.Event(), asyncio.Event()
    events = []
    original = RuntimeError("native producer failed")

    async def failing():
        await started.wait()
        raise original

    async def sibling():
        started.set()
        events.append("started")
        try:
            await release.wait()
        finally:
            events.append("drained")

    token = admitted._ADMISSION.set(scope)
    owner = asyncio.create_task(admitted.gather(failing(), sibling()))
    try:
        with pytest.raises(ExceptionGroup) as caught:
            await asyncio.wait_for(owner, timeout=2)
        assert caught.value.exceptions == (original,)
        assert events == ["started", "drained"]
    finally:
        try:
            release.set()
            if not owner.done():
                owner.cancel()
                try:
                    await asyncio.wait_for(owner, timeout=2)
                except asyncio.CancelledError:
                    assert owner.done()
        finally:
            admitted._ADMISSION.reset(token)


async def test_stage_inventory_omits_missing_heaps_so_registration_cannot_invent_their_oid(monkeypatch):
    native = candidate._native()
    database = SimpleNamespace(scalar=AsyncMock(side_effect=[None, 42]))
    monkeypatch.setattr(native, "db", database)
    swaps = tuple(
        SimpleNamespace(live_cls=SimpleNamespace(__main_table__=target), stage_cls=SimpleNamespace(__tablename__=stage))
        for target, stage in (
            ("entity_address_unified", "missing_stage"),
            ("entity_address_evidence", "evidence_stage"),
        )
    )
    assert await candidate._capture_stage_oids("test_schema", swaps) == (
        ("entity_address_evidence", "evidence_stage", 42),
    )
    assert [call.kwargs["relation"] for call in database.scalar.await_args_list] == [
        "test_schema.missing_stage",
        "test_schema.evidence_stage",
    ]


async def test_native_preparation_reports_original_and_owned_cleanup_errors_without_leaking_context(monkeypatch):
    native = candidate._native()
    original, cleanup_error = RuntimeError("native build failed"), OSError("owned cleanup failed")
    admission = _admission()
    inputs = replace(_address_inputs(), semantic_as_of=admission.plan.desired_profile_as_of)
    database = SimpleNamespace(
        _transaction_binding=lambda: None, transaction=_transaction, scalar=AsyncMock(return_value=41)
    )

    async def status(statement):
        if statement.startswith("DROP TABLE"):
            raise cleanup_error

    database.status = AsyncMock(side_effect=status)
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(native, "_acquire_cutover_locks", AsyncMock())
    monkeypatch.setattr(candidate.prepared_doctors, "capture_dependencies", AsyncMock(return_value=SimpleNamespace()))

    async def build(_ctx, _task):
        scope = admitted._ADMISSION.get()
        scope.db_schema = "test_schema"
        scope.owned_oids["stage_a"] = 41
        raise original

    monkeypatch.setattr(native, "process_entity_address_unified_data", build)
    finalizer = AsyncMock()
    monkeypatch.setattr(native, "publish_entity_address_unified_generation", finalizer)
    with pytest.raises(ExceptionGroup) as caught:
        await candidate.prepare_provider_directory_entity_address(
            {},
            {},
            preparation_input=inputs,
            admission=admission,
            native_input_hash=admission.plan.native_address_input_hash,
        )
    assert caught.value.exceptions == (original, cleanup_error)
    finalizer.assert_not_awaited()
    assert candidate.current() is None and candidate._NATIVE_DEPENDENCIES.get() is None
    assert admitted._ADMISSION.get() is None
    assert database.status.await_args.args == ("DROP TABLE test_schema.stage_a RESTRICT",)


async def test_native_registration_preserves_absent_inputs_and_requires_revision_ledger():
    relations = [
        {"name": f'"{namespace}"."{name}"', "relation_oid": None}
        for namespace, name in native_inputs._relations("test_schema")
    ]

    async def execute(statement, parameters=None):
        sql = str(statement)
        if "unnest(CAST(:names AS text[]))" in sql:
            return SimpleNamespace(mappings=lambda: relations)
        if "SELECT to_regclass(:name)" in sql:
            return SimpleNamespace(scalar_one=lambda: None)
        if "FROM pg_trigger trigger JOIN pg_proc" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
        raise AssertionError("unexpected registration statement: " + sql)

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    with pytest.raises(RuntimeError, match="native_revision_ledger_unavailable"):
        await native_inputs.register_native_address_inputs(session, "test_schema")
    assert all(
        not str(call.args[0]).lstrip().startswith(("LOCK TABLE", "INSERT", "CREATE", "ALTER"))
        for call in session.execute.await_args_list
    )


async def test_index_free_native_heap_preserves_catalog_fingerprint():
    name = "entity_address_unified_cms" + "a" * 20 + "_raw"
    relation = preparation.OwnedRelation("test_schema", name, 41, 10, "u")
    relation_by_field = {"schema_name": "test_schema", "relation_name": name, "effective_tablespace_oid": 42}
    fhir = admitted._fhir()
    backend = SimpleNamespace(
        db=SimpleNamespace(all=AsyncMock(), scalar=AsyncMock(return_value=9)),
        _profile_capacity_relation_row=AsyncMock(return_value=(relation_by_field, None)),
        _profile_capacity_relation_catalog=AsyncMock(return_value=([], [], [], [])),
        _profile_capacity_tablespaces=fhir._profile_capacity_tablespaces,
        _identity_hash=fhir._identity_hash,
    )
    actual = await layout.capture_native_layout(backend, relation, ("entity_address_unified",))
    assert actual.relation_oid == 41 and actual.effective_tablespace_oids == (42,)
    expected_by_field = {
        "contract": "cms-native-observed-layout.v1",
        "database_oid": 9,
        "relation": relation_by_field,
        "attributes": [],
        "constraints": [],
        "triggers": [],
        "indexes": [],
    }
    assert actual.exact_fingerprint == fhir._identity_hash(expected_by_field)
    backend.db.all.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, "table-replaced", "view-replaced", "view-absent"])
async def test_archive_cleanup_drops_owned_relations_and_records_replacements_once(failure):
    fake, *_ = _fake_fhir([])
    admission = await _started_admission()
    admission._relations[("test_schema", "delta_stage")] = 41
    admission._external_relations.add(("test_schema", "delta_stage"))
    admission._logged_relations.add(("test_schema", "delta_stage"))
    relation_oids_by_name = {
        "test_schema.delta_stage": 51 if failure == "table-replaced" else 41,
        "test_schema.effective_view": 52 if failure == "view-replaced" else 42,
    }
    if failure == "view-absent":
        del relation_oids_by_name["test_schema.effective_view"]

    async def scalar(_statement, **parameters):
        return relation_oids_by_name.get((parameters.get("name") or parameters["relation_ref"]).replace('"', ""))

    async def status(statement):
        if statement.startswith("DROP "):
            del relation_oids_by_name[statement.split(" ", 2)[2]]

    fake.db.scalar, fake.db.status = AsyncMock(side_effect=scalar), AsyncMock(side_effect=status)
    identities = [("delta_stage", 41, "TABLE"), ("effective_view", 42, "VIEW")]
    await archive._cleanup(fake, admission, "test_schema", identities)
    await archive._cleanup(fake, admission, "test_schema", identities)
    if failure == "table-replaced":
        assert relation_oids_by_name == {"test_schema.delta_stage": 51}
        assert admission.cleanup_preserved == [("test_schema", "delta_stage")]
        assert admission._relations == {("test_schema", "delta_stage"): 41}
    else:
        assert not admission._relations and not admission._external_relations and not admission._logged_relations
        assert relation_oids_by_name == ({"test_schema.effective_view": 52} if failure == "view-replaced" else {})
        assert admission.cleanup_preserved == (
            [("test_schema", "effective_view")] if failure == "view-replaced" else []
        )
    assert not any("CASCADE" in call.args[0] for call in fake.db.status.await_args_list)


async def test_cancelled_archive_cleanup_finishes_owned_drop_and_retirement_before_propagating():
    fake, *_ = _fake_fhir([])
    admission = await _started_admission()
    admission._relations[("test_schema", "delta_stage")] = 41
    admission._external_relations.add(("test_schema", "delta_stage"))
    entered, release = asyncio.Event(), asyncio.Event()
    heap_presence_flags = [True]
    fake.db.scalar = AsyncMock(side_effect=lambda _statement, **_parameters: 41 if heap_presence_flags[0] else None)

    async def status(statement):
        if statement.startswith("DROP TABLE"):
            entered.set()
            await release.wait()
            heap_presence_flags[0] = False

    fake.db.status = AsyncMock(side_effect=status)
    owner = asyncio.create_task(archive._cleanup(fake, admission, "test_schema", [("delta_stage", 41, "TABLE")]))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        owner.cancel()
        await asyncio.sleep(0)
        assert not owner.done() and heap_presence_flags[0]
    finally:
        release.set()
        if not owner.done() and not owner.cancelling():
            owner.cancel()
        try:
            await asyncio.wait_for(owner, timeout=2)
        except asyncio.CancelledError:
            assert owner.done()
    assert owner.cancelled()
    assert not heap_presence_flags[0] and not admission._relations and not admission._external_relations


class _ArchiveRows:
    def __init__(self, rows):
        self.rows, self.closed = iter(rows), False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return (next(self.rows),)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self):
        self.closed = True


async def _archive_catalog_response(statement, parameters):
    """Return captured relation identities and seals for the synthetic owner connection."""
    sql = str(statement)
    scalars_by_query = {
        "pg_relation_filenode": 51,
        "pg_get_viewdef": "SELECT archive",
        "SELECT revision": 0,
        "FROM pg_proc p JOIN pg_language": 61,
        "SELECT count(*)=1": True,
    }
    scalar_response = next((scalar for query, scalar in scalars_by_query.items() if query in sql), None)
    if "to_regclass(:name)" in sql:
        oids_by_name = {"delta_stage": 41, "effective_view": 42, "address_archive_v2": 43}
        scalar_response = oids_by_name[parameters["name"].split(".")[-1].strip('"')]
    columns = [
        SimpleNamespace(_mapping={"attname": name, "attgenerated": "", "attidentity": ""})
        for name in ("address_key", "city")
    ]
    return SimpleNamespace(scalar=lambda: scalar_response, all=lambda: columns, rowcount=0)


@pytest.mark.parametrize("phase", ["build", "changed-build", "cutover", "profile"])
async def test_archive_build_verifies_rows_and_handoff_verifies_catalog(phase):
    """Reject changed build rows, retain catalog checks at handoff, and close every row stream."""
    fake, *_ = _fake_fhir([])
    admission = _admission()
    prepared = _archive_delta(admission)
    serialized_rows, streams = ['{"address_key":"synthetic-address","city":"Example"}'], []

    async def stream(_statement):
        stream_result = _ArchiveRows(serialized_rows)
        streams.append(stream_result)
        return stream_result

    connection = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=_archive_catalog_response))
    database = archive.ConnectionProxy(None, connection, None)
    database._transaction_binding = lambda: SimpleNamespace(session=database)
    database.in_transaction = lambda: True
    database.stream = AsyncMock(side_effect=stream)
    fake.db = database
    prepared.delta_rows, prepared.delta_sha256 = await archive._digest(fake, prepared.schema, prepared.delta_table)
    if phase == "changed-build":
        serialized_rows.append('{"address_key":"other-address","city":"Example"}')
    active_token = preparation._ACTIVE.set(admission)
    capacity_token = fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object() if phase == "profile" else None)
    try:
        if phase == "changed-build":
            with pytest.raises(RuntimeError, match="archive_preparation_changed"):
                await prepared.assert_ready(fake)
        else:
            await prepared.assert_ready(fake, cutover=phase == "cutover")
        assert len(streams) == (2 if phase in {"build", "changed-build"} else 1)
        assert all(stream_result.closed for stream_result in streams)
        assert not prepared.committed
        assert not any(
            str(call.args[0]).lstrip().startswith(("INSERT", "UPDATE", "DELETE", "DROP"))
            for call in connection.execute.await_args_list
        )
    finally:
        fake._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)
        preparation._ACTIVE.reset(active_token)
