# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""REST artifact lifecycle boundaries; native authority is tested separately."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_artifacts as artifacts


@pytest.fixture
def case(monkeypatch):
    fence = SimpleNamespace(
        datasets=(SimpleNamespace(source_id="source_a", status="published", is_current=True),),
        promotion_datasets=(),
        should_select_validated_candidates=False,
        source_ids=["source_a"],
    )
    factory = SimpleNamespace(artifact_fence=object())
    directory = SimpleNamespace(db=object())
    directory.UHC_RETAINED_SOURCE_ID = "uhc"
    directory.is_uhc_flex_profile_source = lambda _source_id: False
    directory._uses_alohr_graphql_connector = lambda _record: False
    directory._uses_uhc_official_file_connector = lambda _record: False
    directory.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT = "500ms"
    directory.PROVIDER_DIRECTORY_ARTIFACT_FENCE_STATEMENT_TIMEOUT = "8s"
    monkeypatch.setattr(artifacts.source, "source_producer_identity", lambda: ("a" * 40, "sha256:" + "b" * 64))
    monkeypatch.setattr(artifacts, "desired_fence_hash", lambda selected: repr(vars(selected)))
    token = artifacts.source._FACTORY.set(factory)
    admission = artifacts._Admission(
        directory,
        factory,
        None,
        fence,
        artifacts.desired_fence_hash(fence),
        frozenset({"Location"}),
        artifacts.source.source_producer_identity(),
    )
    try:
        yield SimpleNamespace(directory=directory, factory=factory, fence=fence, admission=admission)
    finally:
        artifacts.source._FACTORY.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["task", "factory", "producer", "fence"])
async def test_admission_cannot_be_borrowed_or_rebound(case, damage):
    admission = replace(case.admission, task=asyncio.current_task())
    match damage:
        case "task":
            admission = replace(admission, task=object())
        case "factory":
            admission = replace(admission, factory=object())
        case "producer":
            admission = replace(admission, producer=("c" * 40, "sha256:" + "d" * 64))
        case "fence":
            admission = replace(admission, fence_hash="changed")
    token = artifacts._ACTIVE.set(admission)
    try:
        with pytest.raises(RuntimeError, match="rest_artifact_admission_changed"):
            artifacts.current(case.directory)
    finally:
        artifacts._ACTIVE.reset(token)


@pytest.mark.asyncio
async def test_only_exact_prepared_fence_and_resource_set_are_reused(case):
    case.admission = replace(case.admission, task=asyncio.current_task())
    token = artifacts._ACTIVE.set(case.admission)
    try:
        assert artifacts.reuse_scope(case.directory, case.fence, None, {"Location"}) is case.fence
        with pytest.raises(RuntimeError, match="rest_artifact_scope_changed"):
            artifacts.reuse_scope(case.directory, case.fence, None, {"Practitioner"})
        assert artifacts.is_fence_held(case.directory, case.fence, None) is False
    finally:
        artifacts._ACTIVE.reset(token)


def _configure_terminal_case(case, monkeypatch, fault, events):
    case.admission = replace(case.admission, task=asyncio.current_task())
    executor = SimpleNamespace(status=AsyncMock())
    session = SimpleNamespace(in_transaction=lambda: True)
    binding = SimpleNamespace(session=session)

    @asynccontextmanager
    async def transaction():
        events.append("ordinary opened")
        try:
            yield
        except BaseException:
            events.append("ordinary rolled back")
            raise
        else:
            events.append("ordinary committed")

    @asynccontextmanager
    async def control_transaction(_directory, owner, **_options):
        async with owner:
            yield

    case.directory.db = SimpleNamespace(
        _transaction_binding=lambda: binding, transaction=transaction, status=AsyncMock()
    )
    case.directory.profile_control_custody = SimpleNamespace(transaction=control_transaction)
    case.directory._provider_directory_profile_capacity_admission = lambda: None
    case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE = SimpleNamespace(get=lambda: case.fence)

    @asynccontextmanager
    async def fence(database):
        assert database is case.directory.db
        events.append("Source opened")
        try:
            yield executor
            events.append("Source verified")
            if fault == "Source verification":
                raise ValueError("Source verification failure")
        finally:
            events.append("Source rolled back")

    async def lock(_directory, selected, actual_executor, *, for_update):
        assert selected is case.fence and actual_executor is executor
        assert for_update is True
        assert artifacts.is_fence_held(case.directory, selected, actual_executor) is False
        events.append("Source locked")

    async def ordinary_locks(_directory, selected, database):
        assert selected is case.fence and database is case.directory.db
        events.append("ordinary locked")

    case.factory.artifact_fence = fence
    monkeypatch.setattr(artifacts, "_verify_header", lock)
    monkeypatch.setattr(artifacts, "_lock_ordinary_controls", ordinary_locks)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "body", "Source verification"])
async def test_source_verification_settles_before_ordinary_commit(case, monkeypatch, fault):
    from process import provider_directory_profile_artifact_custody as custody

    events = []
    _configure_terminal_case(case, monkeypatch, fault, events)
    token = artifacts._ACTIVE.set(case.admission)
    try:
        try:
            async with custody.cutover_transaction(case.directory, "bundle", object()):
                assert artifacts.is_fence_held(case.directory, case.fence, case.directory.db)
                events.append("ordinary swapped")
                if fault == "body":
                    raise ValueError("ordinary publication failure")
        except ValueError as error:
            assert fault is not None and "failure" in str(error)
        assert artifacts._TERMINAL.get() is None
    finally:
        artifacts._ACTIVE.reset(token)
    assert events == [
        "ordinary opened",
        "ordinary locked",
        "Source opened",
        "Source locked",
        "ordinary swapped",
        *([] if fault == "body" else ["Source verified"]),
        "Source rolled back",
        "ordinary rolled back" if fault else "ordinary committed",
    ]


@pytest.mark.asyncio
async def test_terminal_capability_cannot_replace_another_fence(case):
    case.admission = replace(case.admission, task=asyncio.current_task())
    active = artifacts._ACTIVE.set(case.admission)
    session = SimpleNamespace(in_transaction=lambda: True)
    case.directory.db = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    held = artifacts._TERMINAL.set((case.admission, session))
    try:
        with pytest.raises(RuntimeError, match="rest_artifact_terminal_changed"):
            artifacts.is_fence_held(case.directory, SimpleNamespace(datasets=()), None)
        case.directory.db._transaction_binding = lambda: SimpleNamespace(
            session=SimpleNamespace(in_transaction=lambda: True)
        )
        with pytest.raises(RuntimeError, match="rest_artifact_terminal_changed"):
            artifacts.is_fence_held(case.directory, case.fence, None)
    finally:
        artifacts._TERMINAL.reset(held)
        artifacts._ACTIVE.reset(active)


@pytest.mark.asyncio
async def test_cleanup_refuses_a_replaced_native_relation(case, monkeypatch):
    @asynccontextmanager
    async def transaction():
        yield

    case.directory.db = SimpleNamespace(transaction=transaction, status=AsyncMock())
    monkeypatch.setattr(artifacts, "_catalog", AsyncMock(return_value=(101, 202, "r", "u")))
    with pytest.raises(RuntimeError, match="rest_artifact_cleanup_custody_changed"):
        await artifacts._cleanup(case.directory, {"pdra_owned": (100, 202, "r", "u")})
    case.directory.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_typed_copy_retains_native_scalar_encoders_and_fixed_reader(case, monkeypatch):
    copied_records = []
    model = SimpleNamespace(__table__=SimpleNamespace(columns=[SimpleNamespace(name=name) for name in ("time", "n")]))
    values_by_column = dict(time=datetime(2026, 1, 1), n=Decimal("1.25"))

    async def pages(database, statement, params, actual_model):
        assert database is case.directory.db and actual_model is model
        yield [values_by_column]

    async def copy(_name, **options):
        copied_records.extend(options["records"])
        return "COPY 1"

    case.directory.ProviderDirectorySource = object()
    case.directory._schema = lambda: "sample"
    case.directory._json_columns = lambda _table: set()
    case.directory._copy_record = lambda row, columns, _json: tuple(row[column] for column in columns)
    monkeypatch.setattr(artifacts, "projected_pages", pages)
    assert (
        await artifacts._copy(
            case.directory, SimpleNamespace(copy_records_to_table=copy), model, "private_stage", "SELECT", {}
        )
        == 1
    )
    assert copied_records == [(values_by_column["time"], values_by_column["n"])]


@pytest.mark.asyncio
async def test_prepared_npi_metrics_require_original_native_stages_and_selector(case, monkeypatch):
    counts_by_resource = dict(Organization=2, Practitioner=3)
    case.admission = replace(
        case.admission,
        task=asyncio.current_task(),
        prepared=dict(
            relations={"typed": ("private_stage", 1, 2, "r", "u")},
            metrics=dict(resource_id_npis_backfilled=counts_by_resource),
        ),
    )
    case.directory._schema = lambda: "sample"
    case.directory._clean_source_id_list = list
    case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES = SimpleNamespace(
        get=lambda: {"typed": "private_stage"}
    )
    catalog = AsyncMock(return_value=(1, 2, "r", "u"))
    monkeypatch.setattr(artifacts, "_catalog", catalog)
    token = artifacts._ACTIVE.set(case.admission)
    try:
        prepared_counts = await artifacts.prepared_npi_backfill(
            case.directory, None, run_id=None, source_ids=["source_a"], seen_table=None
        )
        assert prepared_counts == counts_by_resource and prepared_counts is not counts_by_resource
        with pytest.raises(RuntimeError, match="npi_selector_changed"):
            await artifacts.prepared_npi_backfill(
                case.directory, None, run_id="other", source_ids=["source_a"], seen_table=None
            )
        catalog.return_value = (9, 2, "r", "u")
        with pytest.raises(RuntimeError, match="stage_changed"):
            await artifacts.prepared_npi_backfill(
                case.directory, None, run_id=None, source_ids=["source_a"], seen_table=None
            )
    finally:
        artifacts._ACTIVE.reset(token)


@pytest.mark.asyncio
async def test_partial_address_pins_source_on_its_own_transaction(case, monkeypatch):
    session = SimpleNamespace(in_transaction=lambda: True)
    case.directory.db = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    case.directory._schema = lambda: "sample"
    selected = replace(case.admission, task=asyncio.current_task()).fence
    selected.datasets[0].dataset_id = "dataset"
    selected.datasets[0].evidence_run_id = "root"
    case.directory._resolve_provider_directory_artifact_datasets = AsyncMock(return_value=selected)
    lock = AsyncMock()
    verify = AsyncMock()
    monkeypatch.setattr(artifacts, "_lock_ordinary_controls", lock)
    monkeypatch.setattr(artifacts, "_verify_header", verify)
    context_by_field = dict(
        partial_provider_directory_dataset_id="dataset",
        partial_provider_directory_run_id="root",
        partial_provider_directory_source_ids=["source_a"],
    )
    await artifacts.lock_address_source_fence(case.directory, case.directory.db, "sample", context_by_field)
    lock.assert_awaited_once_with(case.directory, selected, case.directory.db)
    verify.assert_awaited_once_with(case.directory, selected, case.directory.db, for_update=False)
    with pytest.raises(RuntimeError, match="address_source_fence_changed"):
        await artifacts.lock_address_source_fence(
            case.directory,
            case.directory.db,
            "sample",
            {**context_by_field, "partial_provider_directory_run_id": "other"},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"publish_artifacts": True},
        {"publish_after_acquisition": True},
        {"publish_artifacts_only": True},
        {"defer_typed_materialization": False},
        {"import_resources": False},
        {"publish_corroboration": True},
        {"seed_only": True},
        {"publish_source_dataset_only": 1},
    ],
)
async def test_source_only_opt_in_rejects_incompatible_work_before_io(case, change):
    task_by_field = dict(
        publish_source_dataset_only=True,
        defer_typed_materialization=True,
        import_resources=True,
        publish_artifacts=False,
    )
    artifacts.validate_source_only_task(case.directory, task_by_field, ["source_a"], None)
    with pytest.raises((ValueError, RuntimeError), match="invalid"):
        artifacts.validate_source_only_task(case.directory, {**task_by_field, **change}, ["source_a"], None)
    with pytest.raises(RuntimeError, match="invalid"):
        artifacts.validate_source_only_task(case.directory, task_by_field, ["source_a"], object())
    artifacts.validate_source_only_task(case.directory, {}, [], None)


def test_special_profile_source_is_not_generic_rest(case):
    case.directory.is_uhc_flex_profile_source = lambda _source_id: True
    task_by_field = dict(
        publish_source_dataset_only=True,
        import_resources=True,
        defer_typed_materialization=True,
        publish_artifacts=False,
    )
    with pytest.raises(RuntimeError, match="rest_source_publication_task_invalid"):
        artifacts.validate_source_only_task(case.directory, task_by_field, ["source_a"], None)
    with pytest.raises(RuntimeError, match="rest_source_publication_source_invalid"):
        with artifacts.source_publication_request(case.directory, [{"source_id": "source_a"}], True):
            pytest.fail("special profile entered the REST scope")


def test_source_publication_requires_genuine_followup():
    metrics_by_key = {}
    artifacts.record_dataset_followup({}, metrics_by_key, None)
    assert metrics_by_key == {}
    with pytest.raises(RuntimeError, match="rest_source_publication_incomplete"):
        artifacts.record_dataset_followup({"publish_source_dataset_only": True}, metrics_by_key, None)
    descriptor_by_field = {
        "source_id": "source_a",
        "endpoint_id": "endpoint_a",
        "dataset_id": "dataset",
        "dataset_hash": "a" * 64,
        "parent_run_id": "root",
    }
    artifacts.record_dataset_followup({"publish_source_dataset_only": True}, metrics_by_key, descriptor_by_field)
    assert metrics_by_key["dataset_followup"] == {
        **descriptor_by_field,
        artifacts.recovery.DESCRIPTOR_FIELD: artifacts.recovery.publication_generation(descriptor_by_field),
    }
    assert artifacts.recovery.DESCRIPTOR_FIELD not in descriptor_by_field


@pytest.mark.asyncio
async def test_source_only_publication_uses_validated_candidate_and_closes_inherited_scope(case, monkeypatch):
    from process import provider_directory_source_local_publication as publication

    candidate = SimpleNamespace(dataset_id="dataset", source_ids=("source_a",))
    case.directory._is_endpoint_dataset_publishable = lambda *_: True
    case.directory._validate_endpoint_dataset_candidate = AsyncMock(return_value={"validated": True})
    case.directory.ENDPOINT_DATASET_PUBLISHED = "published"
    publish = AsyncMock()
    monkeypatch.setattr(publication, "publish_validated_source_local_dataset", publish)
    assert await artifacts.finalize_candidate(case.directory, candidate, {}) == {"validated": True}
    publish.assert_not_awaited()
    with artifacts.source_publication_request(case.directory, [{"source_id": "source_a"}], True):
        result = await asyncio.create_task(artifacts.finalize_candidate(case.directory, candidate, {}))
        assert result == {"validated": True, "published": True, "is_current": True, "status": "published"}
        publish.assert_awaited_once_with(case.directory, candidate, "source_a")
        stale = artifacts._SOURCE_ONLY.get()
    token = artifacts._SOURCE_ONLY.set(stale)
    try:
        with pytest.raises(RuntimeError, match="scope_changed"):
            await artifacts.finalize_candidate(case.directory, candidate, {})
    finally:
        artifacts._SOURCE_ONLY.reset(token)
