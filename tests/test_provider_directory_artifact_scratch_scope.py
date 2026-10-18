# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host lifecycle/forwarding checks with substituted issuance APIs, not native authority."""

import asyncio
import importlib
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_scratch_scope as scratch
from process import provider_directory_cms_preparation as preparation


def _new_case():
    """Build typed host carriers without claiming a signed/native admission."""
    name_by_root = {
        root: f"scratch_{index}" for index, root in enumerate((scratch.inputs.SOURCE, *scratch.inputs.TYPED_ROOTS))
    }
    oid_by_root = {root: index + 101 for index, root in enumerate(name_by_root)}
    fence = SimpleNamespace(
        datasets=(SimpleNamespace(source_id="source", dataset_id="dataset", artifact_resources=()),)
    )
    projection = SimpleNamespace(projection_hash="a" * 64)
    plan = SimpleNamespace(
        desired_fence_hash=preparation.desired_fence_hash(fence),
        resource_types=("Practitioner",),
        artifact_scope_projection_hash=projection.projection_hash,
    )
    admission = preparation.NonprofileAdmission(None, plan, AsyncMock())
    admission._started = True
    directory = SimpleNamespace(
        _PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES=ContextVar("host_names", default={}),
        _PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS=ContextVar("host_oids", default={}),
    )
    return SimpleNamespace(
        directory=directory,
        admission=admission,
        scope=SimpleNamespace(admission=admission),
        fence=fence,
        projection=projection,
        types=frozenset({"Practitioner"}),
        names=name_by_root,
        oids=oid_by_root,
        events=[],
        metadata=None,
        metadata_error=None,
        cleanup_errors=[],
        metadata_seen=False,
        owner=None,
        cleanup_entered=None,
        cleanup_release=None,
        active=ContextVar("host_active_scratch", default=None),
    )


async def _prepare(case, *arguments):
    assert arguments == (case.directory, case.admission, case.fence, case.types, case.projection)
    case.owner = asyncio.current_task()
    case.events.append("prepare")
    return case.scope


async def _metadata(case, directory, scope):
    case.events.append("metadata")
    assert directory is case.directory
    if scope is not case.scope or not case.admission._started:
        raise RuntimeError("host metadata lifetime refused")
    if case.metadata_error is not None:
        raise case.metadata_error
    case.metadata_seen = True
    return deepcopy(case.metadata or {"relation_by_table": case.names, "relation_oids": case.oids})


@contextmanager
def _input_scope(case, scope):
    assert scope is case.scope and case.metadata_seen
    case.events.append("scope enter")
    token = case.active.set(scope)
    try:
        yield
    finally:
        case.active.reset(token)
        case.events.append("scope exit")


@contextmanager
def _artifact_tokens(case, full_fence, resource_fence, names, oids):
    assert full_fence is resource_fence is case.fence and case.metadata_seen
    case.events.append("tokens enter")
    names_token = case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(names)
    oids_token = case.directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.set(oids)
    try:
        yield
    finally:
        case.directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.reset(oids_token)
        case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(names_token)
        case.events.append("tokens exit")


async def _discard(case, directory, scope):
    assert directory is case.directory and scope is case.scope
    assert asyncio.current_task() is case.owner
    assert case.active.get() is None and scratch._PROJECTION.get() is None
    assert case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == {}
    case.events.append("discard")
    if case.cleanup_entered is not None:
        case.cleanup_entered.put_nowait(asyncio.current_task())
        await case.cleanup_release.wait()
    if case.cleanup_errors:
        raise case.cleanup_errors.pop(0)


@pytest.fixture
def case(monkeypatch):
    """Substitute explicit issuance/metadata APIs while exercising the real scope lifecycle."""
    state = _new_case()
    state.directory._artifact_scope_tokens = partial(_artifact_tokens, state)
    monkeypatch.setattr(scratch.inputs, "prepare_scratch_inputs", partial(_prepare, state))
    monkeypatch.setattr(scratch.inputs, "scratch_input_metadata", partial(_metadata, state), raising=False)
    monkeypatch.setattr(scratch.inputs, "scratch_input_scope", partial(_input_scope, state))
    monkeypatch.setattr(scratch.inputs, "discard_scratch_inputs", partial(_discard, state))
    monkeypatch.setattr(scratch.inputs, "active_scratch_inputs", lambda directory: state.active.get())
    return state


def prepared(case):
    """Enter the actual lifecycle function while substituting only its external APIs."""
    return scratch.prepared_scratch_scope(case.directory, case.admission, case.fence, case.types, case.projection)


async def test_metadata_precedes_exposure_and_same_task_cleanup(case):
    async with prepared(case) as names:
        assert names == case.names and names is not case.names
        assert case.events == ["prepare", "metadata", "scope enter", "tokens enter"]
        names["host-only"] = "cannot mutate captured metadata"
        assert "host-only" not in case.names
    assert case.events[-3:] == ["tokens exit", "scope exit", "discard"]


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_body_error_preserves_original_object(case, error_type):
    error = error_type("body")
    with pytest.raises(error_type) as caught:
        async with prepared(case):
            raise error
    assert caught.value is error
    assert case.events[-3:] == ["tokens exit", "scope exit", "discard"]


@pytest.mark.parametrize("body_cancelled", [False, True])
async def test_repeated_cleanup_cancellation_drains_on_original_task(case, body_cancelled):
    first, second = asyncio.CancelledError("first"), asyncio.CancelledError("second")
    body_error = asyncio.CancelledError("body") if body_cancelled else None
    case.cleanup_errors = [first, second]
    with pytest.raises(asyncio.CancelledError) as caught:
        async with prepared(case):
            if body_error is not None:
                raise body_error
    assert caught.value is (body_error or second)
    assert case.events[-3:] == ["discard", "discard", "discard"]
    assert not case.cleanup_errors


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_body_and_cleanup_failures_are_both_retained(case, error_type):
    body_error, cleanup_error = error_type("body"), ValueError("cleanup")
    case.cleanup_errors = [cleanup_error]
    with pytest.raises(BaseExceptionGroup) as caught:
        async with prepared(case):
            raise body_error
    assert caught.value.exceptions == (body_error, cleanup_error)
    assert caught.value.__cause__ is body_error


async def test_cleanup_failure_after_success_propagates(case):
    error = ValueError("cleanup")
    case.cleanup_errors = [error]
    with pytest.raises(ValueError) as caught:
        async with prepared(case):
            assert case.events[-1] == "tokens enter"
    assert caught.value is error


@pytest.mark.parametrize("fault", ["missing", "foreign", "duplicate_name", "duplicate_oid", "boolean_oid"])
async def test_invalid_metadata_never_exposes_names(case, fault):
    names, oids = dict(case.names), dict(case.oids)
    first, second = list(names)[:2]
    if fault == "missing":
        oids.pop(first)
    elif fault == "foreign":
        names["foreign"] = names.pop(first)
        oids["foreign"] = oids.pop(first)
    elif fault == "duplicate_name":
        names[first] = names[second]
    elif fault == "duplicate_oid":
        oids[first] = oids[second]
    else:
        oids[first] = True
    case.metadata = {"relation_by_table": names, "relation_oids": oids}
    with pytest.raises(RuntimeError, match="scratch_metadata_changed"):
        async with prepared(case):
            pytest.fail("invalid metadata was exposed")
    assert case.events == ["prepare", "metadata", "discard"]


async def test_metadata_api_failure_still_discards(case):
    error = RuntimeError("issued metadata unavailable")
    case.metadata_error = error
    with pytest.raises(RuntimeError) as caught:
        async with prepared(case):
            pytest.fail("failed metadata was exposed")
    assert caught.value is error
    assert case.events == ["prepare", "metadata", "discard"]


async def test_reuse_returns_exact_projection_for_active_nonprofile(case):
    token = preparation._ACTIVE.set(case.admission)
    try:
        async with prepared(case):
            assert (
                await scratch.reuse_scratch_scope(case.directory, case.fence, case.fence, case.types) is case.projection
            )
    finally:
        preparation._ACTIVE.reset(token)


def _reuse_fault(case, fault, monkeypatch):
    """Change one authority coordinate while retaining the original scope lifecycle."""
    fence, resource_types = case.fence, case.types
    match fault:
        case "missing":
            preparation._ACTIVE.set(None)
        case "profile":
            preparation._ACTIVE.set(SimpleNamespace(plan=case.admission.plan, _started=True))
        case "foreign":
            preparation._ACTIVE.set(preparation.NonprofileAdmission(None, case.admission.plan, AsyncMock()))
        case "unstarted":
            case.admission._started = False
        case "fence":
            fence = SimpleNamespace(datasets=())
        case "types":
            resource_types = frozenset()
        case "projection":
            case.projection.projection_hash = "b" * 64
        case "names":
            case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({})
        case "oids":
            case.directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.set({})
        case "scope":
            monkeypatch.setattr(
                scratch.inputs, "active_scratch_inputs", lambda directory: SimpleNamespace(admission=case.admission)
            )
    return fence, resource_types


@pytest.mark.parametrize(
    "fault", ["missing", "profile", "foreign", "unstarted", "fence", "types", "projection", "names", "oids", "scope"]
)
async def test_reuse_refuses_changed_authority_and_plan(case, fault, monkeypatch):
    token = preparation._ACTIVE.set(case.admission)
    try:
        async with prepared(case):
            fence, resource_types = _reuse_fault(case, fault, monkeypatch)
            with pytest.raises(RuntimeError):
                await scratch.reuse_scratch_scope(case.directory, case.fence, fence, resource_types)
    finally:
        preparation._ACTIVE.reset(token)


async def test_reuse_outside_issued_scope_does_not_prepare_or_fallback(case):
    with pytest.raises(RuntimeError, match="scratch_admission_required"):
        await scratch.reuse_scratch_scope(case.directory, case.fence, case.fence, case.types)
    assert case.events == []


async def test_cms_full_scope_uses_typed_bridge_before_legacy_io(case, monkeypatch):
    from process import provider_directory_generic_source_policy as source

    monkeypatch.setattr(source, "is_source_storage_required", lambda: True)
    case.directory._schema = lambda: "host_schema"
    case.admission.assert_ready = AsyncMock()
    async with preparation._full_scope(
        case.directory, case.fence, case.types, case.projection, case.admission
    ) as names:
        assert names == case.names
        case.admission.assert_ready.assert_awaited_once_with(case.directory, "host_schema")
    assert case.events[-1] == "discard"


async def test_actual_repeated_task_cancellation_drains_same_task_cleanup(case):
    case.cleanup_entered = asyncio.Queue()
    case.cleanup_release = asyncio.Event()

    async def lifecycle():
        async with prepared(case):
            assert case.events[-1] == "tokens enter"

    task = asyncio.create_task(lifecycle())
    try:
        async with asyncio.timeout(2):
            assert await case.cleanup_entered.get() is task
            task.cancel("first cancellation")
            assert await case.cleanup_entered.get() is task
            task.cancel("second cancellation")
            assert await case.cleanup_entered.get() is task
            case.cleanup_release.set()
            with pytest.raises(asyncio.CancelledError, match="second cancellation"):
                await task
        assert case.events[-3:] == ["discard", "discard", "discard"]
    finally:
        case.cleanup_release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_generic_source_missing_admission_refuses_before_legacy_io(case, monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")
    from process import provider_directory_generic_source_policy as source

    def legacy_io():
        pytest.fail("legacy schema/IO branch was selected")

    monkeypatch.setattr(source, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(directory, "_schema", legacy_io)
    with pytest.raises(RuntimeError, match="scratch_admission_required"):
        async with directory._provider_directory_artifact_dataset_scope(
            run_id=None, source_ids=None, fence=case.fence, resource_types=case.types
        ):
            pytest.fail("unadmitted generic source scope was exposed")
    assert case.events == []


def _native_manifest_fixture(monkeypatch):
    from process import provider_directory_artifact_scratch_indexes as indexes

    support = importlib.import_module("tests.test_provider_directory_cms_preparation")
    fixture = support._manifest_fixture(monkeypatch, "initial")
    inventory, selected = {}, []
    for number, (root, relation) in enumerate(fixture.prepared.relation_overrides.items()):
        oid = fixture.heaps[relation]["oid"]
        selected.append(oid)
        native_index = dict(
            relation_oid=oid,
            index_oid=9000 + number,
            index_name=f"prepared_native_{number}",
            indisvalid=True,
            indisready=True,
            indislive=True,
            indisprimary=True,
        )
        fixture.indexes[:] = [
            catalog_entry for catalog_entry in fixture.indexes if catalog_entry["relation_oid"] != oid
        ]
        fixture.indexes.append(native_index)
        inventory[root] = dict(
            schema="test_schema",
            relation=relation,
            oid=oid,
            indexes=((native_index["index_name"], native_index["index_oid"]),),
        )

    async def metadata(directory, _scope):
        assert directory is fixture.fhir and directory.db._transaction_binding() is None
        return inventory

    monkeypatch.setattr(scratch.inputs, "active_scratch_inputs", lambda _directory: object())
    monkeypatch.setattr(indexes, "scratch_index_metadata", metadata)
    fixture.native_rows = [
        catalog_entry for catalog_entry in fixture.indexes if catalog_entry["relation_oid"] in selected
    ]
    return support, fixture


@pytest.mark.parametrize("fault", [None, "missing", "extra", "replaced", "invalid", "primary"])
async def test_manifest_compares_exact_prepared_index_names_and_oids(monkeypatch, capsys, fault):
    support, fixture = _native_manifest_fixture(monkeypatch)
    row = fixture.native_rows[0]
    match fault:
        case "missing":
            fixture.indexes.remove(row)
        case "extra":
            fixture.indexes.append({**row, "index_name": "extra_native", "index_oid": 99000})
        case "replaced":
            row["index_oid"] += 50000
        case "invalid":
            row["indisvalid"] = False
        case "primary":
            row["indisprimary"] = False
    if fault is not None:
        reason = "indexes_incomplete" if fault in {"invalid", "primary"} else "index_identity_changed"
        await _assert_manifest_index_refusal(support, fixture, reason)
        return
    async with support._manifest_boundary(fixture):
        assert "prepared_native_0" in capsys.readouterr().out


async def _assert_manifest_index_refusal(support, fixture, reason):
    with pytest.raises(RuntimeError, match=reason):
        async with support._manifest_boundary(fixture):
            pytest.fail("changed prepared native index inventory was published")
