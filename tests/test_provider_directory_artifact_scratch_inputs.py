# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Projection, transport and task-boundary checks; no mocked native authority claim."""

import asyncio
import hashlib
import importlib
from copy import deepcopy
from dataclasses import fields, replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_artifact_scratch_inputs as scratch
from process import provider_directory_cms_preparation as preparation
from process.provider_directory_output_address_key_backfill import _digest_rows
from process.provider_directory_output_snapshot_catalog import output_models
from tests.test_entity_address_source_preparation import source_case as source_case
from tests.test_provider_directory_cms_preparation import _admission, _inputs
from tests.test_provider_directory_output_snapshot_custody import operation, relation
from tests.test_provider_directory_output_snapshot_journal import _plan

directory = importlib.import_module("process.provider_directory_fhir")


def _source():
    dataset = SimpleNamespace(
        source_id="synthetic-source",
        endpoint_id="synthetic-endpoint",
        dataset_id="synthetic-dataset",
        evidence_run_id="synthetic-run",
        dataset_hash="a" * 64,
    )
    binding_by_field = dict(
        identity=dict(
            dataset_id=dataset.dataset_id,
            endpoint_id=dataset.endpoint_id,
            acquisition_root_run_id=dataset.evidence_run_id,
            metadata=dict(
                source_ids=[dataset.source_id], resource_hash_contract="contract-v1", semantic_projection_as_of=None
            ),
        ),
        tables={scratch.source_storage.RESOURCE: {"name": "pdgs_" + "b" * 32 + "_r"}},
    )
    return dataset, binding_by_field


@pytest.mark.parametrize("root", scratch.TYPED_ROOTS)
def test_all_eight_typed_encoders_use_only_selected_physical_child(root):
    dataset, binding = _source()
    model = output_models()[root]
    sql, params = scratch.typed_input_projection(directory, model, dataset, binding)
    assert "INSERT " not in sql and "UPDATE " not in sql and "DELETE " not in sql
    assert '"' + binding["tables"][scratch.source_storage.RESOURCE]["name"] + '"' in sql
    assert '"provider_directory_dataset_resource"' not in sql
    assert "CAST(resource.payload_json::jsonb ->>" in sql
    assert (
        "selected.evidence_run_id" in sql
        and "COALESCE(dataset.published_at, dataset.validated_at, dataset.created_at)" in sql
    )
    assert params == dict(
        source_ids=[dataset.source_id],
        dataset_ids=[dataset.dataset_id],
        evidence_run_ids=[dataset.evidence_run_id],
        resource_type=directory.RESOURCE_TYPES_BY_MODEL[model],
    )
    assert sql.startswith(
        "WITH incoming(" + ",".join('"' + str(column.name) + '"' for column in model.__table__.columns)
    )


def test_source_control_and_other_outputs_cannot_enter_typed_authority():
    dataset, binding = _source()
    for model in (directory.ProviderDirectorySource, directory.ProviderDirectoryCanonicalResource):
        with pytest.raises(RuntimeError, match="scratch_model_changed"):
            scratch.typed_input_projection(directory, model, dataset, binding)


@pytest.mark.parametrize("coordinate", ["dataset_id", "endpoint_id", "acquisition_root_run_id"])
def test_changed_source_coordinate_refuses_before_projection(coordinate):
    dataset, binding = _source()
    binding["identity"][coordinate] = "different"
    with pytest.raises(RuntimeError, match="scratch_source_changed"):
        scratch.typed_input_projection(directory, directory.ProviderDirectoryLocation, dataset, binding)


@pytest.mark.asyncio
async def test_header_prefix_bounds_preserve_two_independently_large_facts():
    headers = [dict(dataset_id="a", bytes=9 * 1024 * 1024), dict(dataset_id="b", bytes=9 * 1024 * 1024)]
    fetch = AsyncMock(
        side_effect=[
            headers,
            [dict(dataset_id="a", facts='{"dataset_id":"a"}')],
            headers[1:],
            [dict(dataset_id="b", facts='{"dataset_id":"b"}')],
        ]
    )
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="synthetic"), connection=SimpleNamespace(fetch=fetch))
    pages = [rows async for rows in scratch._source_headers(publisher, ["a", "b"])]
    assert pages == [[dict(dataset_id="a")], [dict(dataset_id="b")]]
    assert fetch.await_args_list[1].args[1] == ["a"] and fetch.await_args_list[3].args[1] == ["b"]


async def test_source_pages_type_the_first_and_later_keyset_cursors():
    headers = [SimpleNamespace(_mapping=dict(source_id=name, record_bytes=9 * 1024**2)) for name in ("a", "b")]
    source_rows = [SimpleNamespace(_mapping=dict(source_id=name)) for name in ("a", "b")]
    fetch = AsyncMock(side_effect=[headers, source_rows[:1], headers[1:], source_rows[1:], []])
    directory = SimpleNamespace(db=SimpleNamespace(all=fetch))
    pages = [page async for page in scratch._source_pages(directory, "SELECT source_id FROM synthetic", ["a", "b"])]
    assert pages == [[dict(source_id="a")], [dict(source_id="b")]]
    assert [call.kwargs["after_source_id"] for call in fetch.await_args_list] == [None, None, "a", "a", "b"]
    assert all(call.args[0].count("CAST(:after_source_id AS varchar)") == 2 for call in fetch.await_args_list)
    assert [fetch.await_args_list[index].kwargs["upper_source_id"] for index in (1, 3)] == ["a", "b"]
    assert all("OFFSET" not in call.args[0] for call in fetch.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size,reason", [(17 * 1024 * 1024, "scratch_header_too_large"), (True, "scratch_header_size_invalid")]
)
async def test_oversized_header_refuses_before_payload_transport(size, reason):
    fetch = AsyncMock(return_value=[dict(dataset_id="a", bytes=size)])
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="synthetic"), connection=SimpleNamespace(fetch=fetch))
    with pytest.raises(RuntimeError, match=reason):
        pages = [rows async for rows in scratch._source_headers(publisher, ["a"])]
        assert not pages
    assert fetch.await_count == 1


def test_control_catalog_preserves_native_hooks_fks_owner_and_acl():
    assert scratch._SOURCE_CATALOG_SQL is scratch.OUTPUT_CATALOG_SQL


@pytest.mark.parametrize("change", [None, "projection", "source_fence", "resource_types"])
def test_signed_scope_contract_refuses_changed_build_geometry(change):
    types = frozenset({"Location"})
    admission = _admission(artifact_targets={"location_contacts"}, types=types)
    admission._started = True
    execution, fence, projection = _inputs()
    if change == "projection":
        projection.projection_hash = "changed"
    elif change == "source_fence":
        fence.datasets[0].dataset_id = "changed"
    elif change == "resource_types":
        types = frozenset({"Practitioner"})
    execution_token = directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    admission_token = preparation._ACTIVE.set(admission)
    try:
        if change is None:
            identity = scratch._admitted_identity(directory, admission, fence, types, projection)
            assert identity["lease_digest"] == admission.lease.lease_digest
            assert identity["reservation_id"] == admission.lease.reservation_id
        else:
            with pytest.raises(RuntimeError, match="scratch_plan_changed"):
                scratch._admitted_identity(directory, admission, fence, types, projection)
    finally:
        preparation._ACTIVE.reset(admission_token)
        directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)


def _scope():
    return scratch.ScratchInputs(directory, object(), asyncio.current_task(), "{}", "{}", "{}", (), ())


@pytest.mark.asyncio
async def test_forged_descriptor_cannot_enter_scope_or_original_transaction():
    scope = _scope()
    with pytest.raises(RuntimeError, match="scratch_scope_required"):
        with scratch.scratch_input_scope(scope):
            pytest.fail("unissued descriptor")
    with pytest.raises(RuntimeError, match="scratch_scope_required"):
        async with scratch.scratch_input_transaction(directory, scope):
            pytest.fail("unissued descriptor")


@pytest.mark.asyncio
async def test_issued_descriptor_cannot_be_rebound_to_child_or_changed_membership():
    scope = _scope()
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        with scratch.scratch_input_scope(scope):
            assert scratch.active_scratch_inputs(directory) is scope

            async def child():
                with pytest.raises(RuntimeError, match="scratch_scope_required"):
                    scratch.active_scratch_inputs(directory)

            await asyncio.create_task(child())
        object.__setattr__(scope, "source_copy_json", '{"substituted":true}')
        with pytest.raises(RuntimeError, match="scratch_scope_required"):
            with scratch.scratch_input_scope(scope):
                pytest.fail("changed membership")
    finally:
        del scratch._ISSUED[scope]
    assert scratch.active_scratch_inputs(directory) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["contact", "coordinate", "npi", "address"])
async def test_raw_map_or_seen_does_not_authorize_enrichment(kind):
    token = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
        {"provider_directory_location": "ordinary_scope"}
    )
    try:
        with pytest.raises(RuntimeError, match="scratch_native_scope_required"):
            await scratch.scratch_consumer_projection(directory, kind, "provider_directory_location")
        with pytest.raises(RuntimeError, match="scratch_seen_custody_required"):
            await scratch.scratch_consumer_projection(
                directory, kind, "provider_directory_location", seen_table="ordinary_seen"
            )
    finally:
        directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,target",
    [
        ("contact", "provider_directory_location"),
        ("coordinate", "provider_directory_location"),
        ("npi", "provider_directory_practitioner"),
    ],
)
async def test_consumer_sql_dispatch_preserves_filters_without_standalone_cut(monkeypatch, kind, target):
    root = scratch.output_models()[target].__tablename__
    old = replace(relation(root, 101), schema=directory._schema(), target=root)
    stage = scratch.stages.OutputStage(directory._schema(), root, "synthetic_private_input", 201, "a" * 64, old)
    scope = replace(_scope(), stages=(stage,))
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    capability = SimpleNamespace(session=None, policy=SimpleNamespace(schema=directory._schema()))
    monkeypatch.setattr(scratch.custody, "_require_capability", AsyncMock())
    monkeypatch.setattr(scratch.stages, "_stage_row", AsyncMock())
    token = scratch._VERIFIED.set(capability)
    try:
        with scratch.scratch_input_scope(scope):
            selected = await scratch.scratch_consumer_projection(
                directory, kind, target, run_id="synthetic-run", source_ids=["synthetic-source"]
            )
        assert '"synthetic_private_input"' in selected["statement"]
        assert '"' + root + '"' not in selected["statement"]
        assert "UPDATE " not in selected["statement"] and "INSERT " not in selected["statement"]
        assert selected["secondary"] is None
        if kind != "contact":
            assert selected["params"]["run_id"] == "synthetic-run"
            assert selected["params"]["source_ids"] == ["synthetic-source"]
        scratch.custody._require_capability.assert_awaited_once_with(capability)
        scratch.stages._stage_row.assert_awaited_once_with(capability, stage, phases={"sealed"})
    finally:
        scratch._VERIFIED.reset(token)
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
async def test_address_consumer_cannot_omit_missing_geo_proof(monkeypatch):
    from process import provider_directory_output_address_key_backfill as address

    monkeypatch.setattr(address, "_native_module", lambda: object())
    missing = AsyncMock(side_effect=RuntimeError("address_geo_snapshot_required"))
    monkeypatch.setattr(address, "_geo_snapshot", missing)
    capability = SimpleNamespace(session=object(), policy=SimpleNamespace(schema="synthetic"))
    with pytest.raises(RuntimeError, match="address_geo_snapshot_required"):
        await scratch._consumer_sql(
            directory, capability, "address", "provider_directory_location", {"run_id": None, "source_ids": None}
        )
    missing.assert_awaited_once_with(capability.session, "synthetic")


def test_existing_journal_accepts_source_preparation_without_new_registry():
    intent_by_field = operation("source_projection")
    dataset, binding = _source()
    header_by_field = dict(
        dataset_hash=dataset.dataset_hash,
        endpoint_id=dataset.endpoint_id,
        acquisition_root_run_id=dataset.evidence_run_id,
        publication_metadata_sha256="c" * 64,
    )
    intent_by_field["source_pins"] = [scratch._source_pin(dataset, header_by_field, binding)]
    capability = SimpleNamespace(
        operation=intent_by_field,
        targets=scratch.TYPED_ROOTS,
        reference=None,
        operation_digest=scratch.custody._operation_digest(intent_by_field, scratch.TYPED_ROOTS),
    )
    plan = _plan()
    before = deepcopy(plan)
    entry = scratch.journal._forward_entry(capability, plan)
    assert entry["phase"] == "preparing" and entry["targets"] == list(scratch.TYPED_ROOTS)
    assert {key: value for key, value in plan.items() if key != scratch.journal._KEY} == before
    with pytest.raises(RuntimeError, match="targets_invalid"):
        scratch.journal._targets([scratch.SOURCE, *scratch.TYPED_ROOTS])


def test_all_eleven_existing_targets_and_profile_reset_remain_intact():
    assert len(directory.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS) == 11
    assert set(directory.PROVIDER_DIRECTORY_ARTIFACT_TARGET_RESOURCE_TYPES) == set(
        directory.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS
    )
    assert set(scratch.TYPED_ROOTS) == {model.__tablename__ for model in directory.RESOURCE_MODELS}


def _enrichment(targets):
    return dict(
        plan=dict(publish_targets=targets, decision_at="2026-01-02T03:04:05"),
        metrics=dict(
            resource_id_npis_backfilled={},
            location_address_keys_stamped=0,
            location_contacts_backfilled={"location_contact_rows_updated": 0},
            location_coordinates_backfilled=0,
        ),
        native=None,
    )


@pytest.mark.parametrize(
    "target,targets,kind",
    [
        ("provider_directory_practitioner", ["resource_id_npis"], "npi"),
        ("provider_directory_organization", ["address_overlay"], "npi"),
        ("provider_directory_location", ["location_address_keys"], "address"),
        ("provider_directory_location", ["location_archive"], None),
    ],
)
def test_loading_projection_keeps_attested_child_and_exact_value_filters(target, targets, kind):
    dataset, binding = _source()
    statement, params = scratch.typed_input_projection(directory, output_models()[target], dataset, binding)
    inputs, expected, actual_params, actual_kind, _counts = scratch._loading_projection(
        directory, target, statement, params, _enrichment(targets)
    )
    assert actual_kind == kind
    assert all(actual_params[name] == value for name, value in params.items())
    assert inputs.count(statement) == expected.count(statement) == 1
    assert '"' + binding["tables"][scratch.source_storage.RESOURCE]["name"] + '"' in inputs
    assert '"' + target + '" r' not in inputs
    assert "UPDATE " not in inputs and "INSERT " not in inputs
    if kind is not None:
        assert "COALESCE((" in inputs and "),FALSE) AS repair_eligible" in inputs
        assert actual_params["update_timestamp"] == datetime(2026, 1, 2, 3, 4, 5)


@pytest.mark.asyncio
async def test_loading_npi_transforms_complete_rows_and_counts_only_selected_rows():
    metadata = {"versionId": "one"}
    rows = [
        dict(resource_id="0000000123", npi=None, updated_at=None, fhir_meta=metadata, repair_eligible=True),
        dict(resource_id="0000000456", npi=7, updated_at=None, fhir_meta=metadata, repair_eligible=False),
    ]
    actual, count = await scratch._repair_loading_rows(directory, rows, "npi", _enrichment(["resource_id_npis"]))
    assert count == 1
    assert actual[0] == dict(
        resource_id="0000000123", npi=123, updated_at=datetime(2026, 1, 2, 3, 4, 5), fhir_meta=metadata
    )
    assert actual[1] == {name: value for name, value in rows[1].items() if name != "repair_eligible"}
    assert rows[0]["npi"] is None and rows[0]["repair_eligible"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ValueError("native failure"), asyncio.CancelledError()])
async def test_loading_address_native_failure_cannot_return_repaired_rows(monkeypatch, error):
    from process import provider_directory_output_address_key_backfill as address

    failed = AsyncMock(side_effect=error)
    transform = AsyncMock()
    monkeypatch.setattr(address, "_canonical_page", failed)
    monkeypatch.setattr(address, "repair_address_rows", transform)
    with pytest.raises(type(error)):
        await scratch._repair_loading_rows(
            directory, [dict(resource_id="location")], "address", _enrichment(["location_address_keys"])
        )
    failed.assert_awaited_once_with(directory, None, [dict(resource_id="location")], 512)
    transform.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "copy", "parity", "digest", "cancel"])
async def test_loading_stage_checks_all_evidence_before_sealing(monkeypatch, failure):
    events = []
    target_root = "provider_directory_location"
    stage = SimpleNamespace(target=target_root, schema="synthetic", relation="owned_stage", incumbent=object())
    capability = SimpleNamespace(session=object(), connection=SimpleNamespace(fetchval=AsyncMock(return_value=True)))
    admission = SimpleNamespace(measure=AsyncMock())
    enrichment = _enrichment(["location_address_keys"])
    enrichment["projection"] = SimpleNamespace(
        tables=[SimpleNamespace(table_name=root, projected_rows=2) for root in (scratch.SOURCE, *scratch.TYPED_ROOTS)]
    )

    async def fill(*_args):
        events.append("copy")
        if failure == "copy":
            raise ValueError("copy refused")
        if failure == "cancel":
            raise asyncio.CancelledError()
        return 2, "digest"

    async def indexes(*_args):
        events.append("indexes")

    async def parity(*_args):
        events.append("parity")
        if failure == "parity":
            raise ValueError("parity refused")

    async def digest(*_args):
        events.append("digest")
        if failure == "digest":
            raise ValueError("digest refused")

    seal, capture = AsyncMock(), AsyncMock(return_value="receipt")
    monkeypatch.setattr(directory, "_artifact_scope_resource_datasets", lambda *_args: [])
    monkeypatch.setattr(scratch.stages, "prepare_output_stage", AsyncMock(return_value=stage))
    monkeypatch.setattr(scratch.journal, "record_output_progress", AsyncMock())
    monkeypatch.setattr(scratch, "_fill_typed_input", fill)
    monkeypatch.setattr(scratch.stages, "finish_output_indexes", indexes)
    monkeypatch.setattr(scratch, "_verify_typed_parity", parity)
    monkeypatch.setattr(scratch, "_verify_typed_digest", digest)
    monkeypatch.setattr(scratch.stages, "seal_output_stage", seal)
    monkeypatch.setattr(scratch.custody, "capture_prepared_output", capture)
    args = directory, capability, admission, object(), frozenset(), {}, target_root, enrichment
    if failure is None:
        assert await scratch._prepare_typed_input(*args) == (stage, "receipt")
        assert events == ["copy", "indexes", "parity", "digest"]
        seal.assert_awaited_once_with(capability, stage, expected_rows=2)
        capture.assert_awaited_once_with(capability, target_root, stage.relation, expected_incumbent=stage.incumbent)
    else:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError):
            await scratch._prepare_typed_input(*args)
        seal.assert_not_awaited()
        capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_loading_fill_transforms_before_copy_and_records_actual_count(monkeypatch):
    events = []
    target_root = "provider_directory_practitioner"
    stage = SimpleNamespace(target=target_root, schema=directory._schema())
    enrichment = _enrichment(["resource_id_npis"])
    dataset, binding = _source()
    source_rows = [dict.fromkeys(str(column.name) for column in output_models()[target_root].__table__.columns)]
    source_rows[0].update(source_id=dataset.source_id, resource_id="0000000123", repair_eligible=True)

    async def pages(_reader, statement, params, _model):
        assert '"' + binding["tables"][scratch.source_storage.RESOURCE]["name"] + '"' in statement
        assert params["source_ids"] == [dataset.source_id]
        yield source_rows

    async def copy(_directory, _capability, supplied, repaired):
        assert supplied is stage and repaired[0]["npi"] == 123
        assert repaired[0]["updated_at"] == datetime(2026, 1, 2, 3, 4, 5)
        assert "repair_eligible" not in repaired[0]
        events.append("copy")
        return len(repaired)

    monkeypatch.setattr(scratch, "projected_pages", pages)
    monkeypatch.setattr(scratch.stages, "copy_output_rows", copy)
    monkeypatch.setattr(scratch.journal, "record_output_progress", AsyncMock())
    admission = SimpleNamespace(measure=AsyncMock())
    copied, digest = await scratch._fill_typed_input(
        directory, object(), admission, stage, [dataset], {dataset.source_id: binding}, enrichment
    )
    assert copied == 1 and len(digest) == 64 and events == ["copy"]
    assert enrichment["metrics"]["resource_id_npis_backfilled"] == {"Practitioner": 1}
    assert source_rows[0]["npi"] is None
    admission.measure.assert_awaited_once_with(directory, stage.schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_closed_candidate_digest_checks_every_cell_and_count(monkeypatch, changed):
    import hashlib

    from process.provider_directory_output_address_key_backfill import _digest_rows

    original_by_field = dict(source_id="source", resource_id="resource", npi=123, fhir_meta={"versionId": "one"})
    digest = hashlib.sha256()
    _digest_rows(digest, [original_by_field])

    async def pages(_reader, statement, params, _model):
        assert statement == 'SELECT * FROM "synthetic"."owned_stage" WHERE source_id=CAST(:source_id AS varchar)'
        assert params == {"source_id": "source"}
        yield [{**original_by_field, "npi": 456} if changed else original_by_field]

    monkeypatch.setattr(scratch, "projected_pages", pages)
    stage = SimpleNamespace(schema="synthetic", relation="owned_stage", target="provider_directory_practitioner")
    if changed:
        with pytest.raises(RuntimeError, match="scratch_enrichment_parity_failed"):
            await scratch._verify_typed_digest(
                directory, stage, [SimpleNamespace(source_id="source")], digest.hexdigest(), 1
            )
    else:
        await scratch._verify_typed_digest(
            directory, stage, [SimpleNamespace(source_id="source")], digest.hexdigest(), 1
        )


@pytest.mark.asyncio
async def test_loading_address_plan_refuses_missing_native_geo_evidence(monkeypatch):
    from process import provider_directory_output_address_key_backfill as address

    publisher = SimpleNamespace(connection=SimpleNamespace(fetchval=AsyncMock(return_value=datetime(2026, 1, 2))))
    monkeypatch.setattr(scratch, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(address, "_native_module", lambda: object())
    missing = AsyncMock(side_effect=RuntimeError("address_geo_snapshot_required"))
    monkeypatch.setattr(address, "_geo_snapshot", missing)
    admission = SimpleNamespace(plan=SimpleNamespace(publish_targets=("location_address_keys",)))
    fence = SimpleNamespace(source_ids=("source",))
    session = object()
    with pytest.raises(RuntimeError, match="address_geo_snapshot_required"):
        await scratch._loading_enrichment(directory, admission, fence, session, object())
    missing.assert_awaited_once_with(session, directory._schema())


@pytest.mark.asyncio
async def test_prepared_metrics_require_original_membership_and_exact_selectors(monkeypatch):
    from contextlib import asynccontextmanager

    scope = _scope()
    membership_by_field = dict(
        enrichment_plan=dict(
            selector=dict(run_id=None, source_ids=["source"], seen_table=None),
            publish_targets=["resource_id_npis"],
            address_batch_size=512,
        ),
        enrichment_metrics=dict(resource_id_npis_backfilled={"Organization": 0, "Practitioner": 2}),
    )
    object.__setattr__(scope, "source_copy_json", scratch.encoded(membership_by_field))
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    events = []

    @asynccontextmanager
    async def transaction(owner, supplied):
        assert owner is directory and supplied is scope
        events.append("verified")
        yield object()

    monkeypatch.setattr(scratch, "scratch_input_transaction", transaction)
    try:
        with scratch.scratch_input_scope(scope):
            actual = await scratch.prepared_scratch_enrichment(directory, "npi", source_ids=["source"])
            assert actual == {"Organization": 0, "Practitioner": 2} and events == ["verified"]
            actual["Practitioner"] = 99
            assert membership_by_field["enrichment_metrics"]["resource_id_npis_backfilled"]["Practitioner"] == 2
            for options in (
                dict(run_id="other"),
                dict(source_ids=["other"]),
                dict(seen_table="ordinary_seen"),
                dict(db_schema="other"),
            ):
                with pytest.raises(RuntimeError):
                    await scratch.prepared_scratch_enrichment(directory, "npi", **options)
            assert events == ["verified"]
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
async def test_final_geo_fence_refuses_without_original_cut_before_native_io(monkeypatch):
    scope = _scope()
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    monkeypatch.setattr(directory.db, "_transaction_binding", lambda: None)
    try:
        with scratch.scratch_input_scope(scope):
            with pytest.raises(RuntimeError, match="scratch_original_cut_required"):
                await scratch.fence_scratch_enrichment(directory)
    finally:
        del scratch._ISSUED[scope]


def _metadata_scope():
    scope = _scope()
    owned_stages = tuple(
        scratch.stages.OutputStage(
            "synthetic", name, "owned_" + str(index), index, "a" * 64, relation(name, 100 + index)
        )
        for index, name in enumerate(scratch.TYPED_ROOTS, 2)
    )
    return replace(
        scope, stages=owned_stages, source_copy_json=scratch.encoded(dict(name="owned_control", copy=dict(oid=1)))
    )


async def test_discard_enters_the_required_output_isolation_before_cleanup(monkeypatch):
    from contextlib import asynccontextmanager

    from process.provider_directory_generic_source_policy import _require_isolation

    scope, publisher = _metadata_scope(), SimpleNamespace(transaction_isolation=None)
    capability = SimpleNamespace(
        connection=SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("cleanup_sql_reached"))),
        policy=SimpleNamespace(schema="synthetic"),
    )

    @asynccontextmanager
    async def source_transaction(actual, *, isolation="read committed"):
        assert actual is directory.db
        publisher.transaction_isolation = isolation
        yield

    @asynccontextmanager
    async def output_transaction(actual, **options):
        assert actual is directory and options["targets"] == scratch.TYPED_ROOTS
        _require_isolation(publisher, "repeatable read")
        yield capability

    monkeypatch.setattr(scratch, "source_transaction", source_transaction)
    monkeypatch.setattr(scratch.custody, "output_snapshot_transaction", output_transaction)
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        with pytest.raises(RuntimeError, match="cleanup_sql_reached"):
            await scratch.discard_scratch_inputs(directory, scope)
        capability.connection.execute.assert_awaited_once()
        assert scope in scratch._ISSUED
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("failure", [False, True])
async def test_metadata_reverifies_same_task_without_nesting_lifetime_scope(monkeypatch, active, failure):
    from contextlib import asynccontextmanager, nullcontext

    scope, capability = _metadata_scope(), object()
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))

    @asynccontextmanager
    async def transaction(*_args, **_options):
        yield capability

    verify = AsyncMock(side_effect=RuntimeError("native membership refused") if failure else None)
    monkeypatch.setattr(scratch, "source_transaction", transaction)
    monkeypatch.setattr(scratch.custody, "output_snapshot_transaction", transaction)
    monkeypatch.setattr(scratch, "_verify_membership", verify)
    try:
        with scratch.scratch_input_scope(scope) if active else nullcontext():
            if failure:
                with pytest.raises(RuntimeError, match="native membership refused"):
                    await scratch.scratch_input_metadata(directory, scope)
            else:
                metadata = await scratch.scratch_input_metadata(directory, scope)
                assert set(metadata) == {"relation_by_table", "relation_oids"}
                assert metadata["relation_by_table"][scratch.SOURCE] == "owned_control"
                assert metadata["relation_oids"][scratch.SOURCE] == 1
                assert all(metadata["relation_oids"][stage.target] == stage.oid for stage in scope.stages)
            verify.assert_awaited_once_with(directory, scope, capability)
            assert scratch._ACTIVE.get() is (scope if active else None)
            assert scratch._VERIFIED.get() is None
        assert scratch._ACTIVE.get() is None
    finally:
        del scratch._ISSUED[scope]


def _two_source_digest_inputs(monkeypatch, changed):
    target_root = "provider_directory_location"
    stage = SimpleNamespace(target=target_root, schema="synthetic", relation="owned_stage", incumbent=object())
    capability = SimpleNamespace(session=object(), connection=SimpleNamespace(fetchval=AsyncMock(return_value=True)))
    rows_by_source = {
        source_id: dict(source_id=source_id, resource_id="resource", address_key=source_id)
        for source_id in ("source-a", "source_a")
    }
    observed_sources = []

    async def fill(_directory, _capability, _admission, _stage, selected_datasets, _bindings, _enrichment):
        assert [dataset.source_id for dataset in selected_datasets] == ["source-a", "source_a"]
        digest = hashlib.sha256()
        for dataset in selected_datasets:
            _digest_rows(digest, [rows_by_source[dataset.source_id]])
        return 2, digest.hexdigest()

    async def pages(_reader, statement, params, _model):
        if params:
            source_id = params["source_id"]
            observed_sources.append(source_id)
            records = [rows_by_source[source_id]]
        else:
            records = [rows_by_source["source_a"], rows_by_source["source-a"]]
        yield [
            {**row, "address_key": "changed"} if changed and row["source_id"] == "source_a" else row for row in records
        ]

    monkeypatch.setattr(
        directory,
        "_artifact_scope_resource_datasets",
        lambda *_args: [SimpleNamespace(source_id=value) for value in ("source_a", "source-a")],
    )
    monkeypatch.setattr(scratch.stages, "prepare_output_stage", AsyncMock(return_value=stage))
    monkeypatch.setattr(scratch.journal, "record_output_progress", AsyncMock())
    monkeypatch.setattr(scratch.stages, "finish_output_indexes", AsyncMock())
    monkeypatch.setattr(scratch, "_fill_typed_input", fill)
    monkeypatch.setattr(scratch, "_verify_typed_parity", AsyncMock())
    monkeypatch.setattr(scratch, "projected_pages", pages)
    seal = AsyncMock()
    monkeypatch.setattr(scratch.stages, "seal_output_stage", seal)
    monkeypatch.setattr(scratch.custody, "capture_prepared_output", AsyncMock(return_value="receipt"))
    enrichment = _enrichment(["location_address_keys"])
    enrichment["projection"] = SimpleNamespace(
        tables=[SimpleNamespace(table_name=root, projected_rows=2) for root in (scratch.SOURCE, *scratch.TYPED_ROOTS)]
    )
    args = (
        directory,
        capability,
        SimpleNamespace(measure=AsyncMock()),
        object(),
        frozenset(),
        {},
        target_root,
        enrichment,
    )
    return args, observed_sources, seal


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_two_source_digest_uses_loading_order_despite_different_database_collation(monkeypatch, changed):
    args, observed_sources, seal = _two_source_digest_inputs(monkeypatch, changed)
    if changed:
        with pytest.raises(RuntimeError, match="scratch_enrichment_parity_failed"):
            await scratch._prepare_typed_input(*args)
        seal.assert_not_awaited()
    else:
        stage, receipt = await scratch._prepare_typed_input(*args)
        assert receipt == "receipt" and stage.relation == "owned_stage"
        seal.assert_awaited_once()
    assert observed_sources == ["source-a", "source_a"]


@pytest.mark.parametrize(
    "location_targets", [["location_contacts"], ["location_coordinates"], ["location_contacts", "location_coordinates"]]
)
@pytest.mark.parametrize("address_enabled", [False, True])
def test_location_loading_composes_native_steps_in_established_order(location_targets, address_enabled):
    from process.provider_directory_output_contact_backfill import contact_projection
    from process.provider_directory_output_coordinate_backfill import coordinate_projection

    dataset, binding = _source()
    model = output_models()["provider_directory_location"]
    raw, params = scratch.typed_input_projection(directory, model, dataset, binding)
    targets = [*location_targets, *(["location_address_keys"] if address_enabled else [])]
    inputs, expected, actual_params, kind, counts = scratch._loading_projection(
        directory, model.__tablename__, raw, params, _enrichment(targets)
    )
    parent = scratch.storage_relation(directory._schema(), model.__tablename__)
    expected_projection = raw
    expected_counts = []
    if "location_contacts" in location_targets:
        projection, count = contact_projection(directory)
        expected_counts.append(("contact", count.replace(parent, "(" + expected_projection + ")", 1)))
        expected_projection = projection.replace(parent, "(" + expected_projection + ")", 1)
    if "location_coordinates" in location_targets:
        projection, count, _params = coordinate_projection(directory, dict(run_id=None, source_ids=None))
        expected_counts.append(("coordinate", count.replace(parent, "(" + expected_projection + ")", 1)))
        expected_projection = projection.replace(parent, "(" + expected_projection + ")", 1)
    assert expected == expected_projection and counts == expected_counts
    assert inputs.count(expected_projection) == 1
    assert kind == ("address" if address_enabled else None)
    assert actual_params["update_timestamp"] == datetime(2026, 1, 2, 3, 4, 5)
    assert all(actual_params[name] == value for name, value in params.items())
    assert all("UPDATE " not in sql and "INSERT " not in sql for sql in [inputs, *[sql for _kind, sql in counts]])


@pytest.mark.asyncio
async def test_location_changed_counts_accumulate_actual_native_results_only():
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[SimpleNamespace(scalar_one=lambda value=value: value) for value in (2, 1, 3, 0)])
    )
    capability = SimpleNamespace(session=session)
    enrichment = _enrichment(["location_contacts", "location_coordinates"])
    counts = [("contact", "SELECT contact_count"), ("coordinate", "SELECT coordinate_count")]
    params_by_name = dict(update_timestamp=datetime(2026, 1, 2, 3, 4, 5), source_ids=["source"])
    await scratch._record_loading_location_counts(capability, counts, params_by_name, enrichment)
    await scratch._record_loading_location_counts(capability, counts, params_by_name, enrichment)
    assert enrichment["metrics"]["location_contacts_backfilled"] == {"location_contact_rows_updated": 5}
    assert enrichment["metrics"]["location_coordinates_backfilled"] == 1
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        "SELECT contact_count",
        "SELECT coordinate_count",
    ] * 2
    assert all(call.args[1] is params_by_name for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, -1, None, "2"])
async def test_location_invalid_native_count_refuses_before_prepared_metrics(invalid):
    capability = SimpleNamespace(
        session=SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: invalid)))
    )
    enrichment = _enrichment(["location_contacts"])
    with pytest.raises(RuntimeError, match="scratch_metrics_changed"):
        await scratch._record_loading_location_counts(capability, [("contact", "SELECT count")], {}, enrichment)
    assert enrichment["metrics"]["location_contacts_backfilled"] == {"location_contact_rows_updated": 0}


@pytest.mark.asyncio
async def test_address_preservation_parity_uses_prior_native_contact_coordinate_results(monkeypatch):
    dataset, binding = _source()
    stage = SimpleNamespace(
        target="provider_directory_location", schema=directory._schema(), relation="private_location"
    )
    enrichment = _enrichment(["location_contacts", "location_coordinates", "location_address_keys"])
    verify = AsyncMock()
    monkeypatch.setattr(scratch, "_verify_set_parity", verify)
    await scratch._verify_typed_parity(
        directory, SimpleNamespace(session=object()), stage, [dataset], {dataset.source_id: binding}, enrichment
    )
    expected = verify.await_args.args[1]
    assert "repair_phone_number" in expected and "repair_latitude" in expected
    assert '"phone_number"' in expected and '"latitude"' in expected
    assert verify.await_args.kwargs["actual_filter"] == " WHERE a.source_id=ANY(CAST(:source_ids AS varchar[]))"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,publish_target,metric",
    [
        ("contact", "location_contacts", "location_contacts_backfilled"),
        ("coordinate", "location_coordinates", "location_coordinates_backfilled"),
    ],
)
@pytest.mark.parametrize("count", [0, 3])
async def test_prepared_location_dispatch_requires_issued_target_and_exact_selector(
    monkeypatch, kind, publish_target, metric, count
):
    from contextlib import asynccontextmanager

    expected = {"location_contact_rows_updated": count} if kind == "contact" else count
    membership_by_field = dict(
        enrichment_plan=dict(
            selector=dict(run_id=None, source_ids=["source"], seen_table=None),
            publish_targets=[publish_target],
            address_batch_size=512,
        ),
        enrichment_metrics={metric: expected},
    )
    scope = replace(_scope(), source_copy_json=scratch.encoded(membership_by_field))
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    events = []

    @asynccontextmanager
    async def transaction(owner, supplied):
        assert owner is directory and supplied is scope
        events.append("verified")
        yield object()

    monkeypatch.setattr(scratch, "scratch_input_transaction", transaction)
    try:
        with scratch.scratch_input_scope(scope):
            assert await scratch.prepared_scratch_enrichment(directory, kind, source_ids=["source"]) == expected
            assert events == ["verified"]
            for options in (dict(run_id="other"), dict(source_ids=["other"]), dict(seen_table="ordinary_seen")):
                with pytest.raises(RuntimeError, match="scratch_selector_changed"):
                    await scratch.prepared_scratch_enrichment(directory, kind, **options)
            other = "coordinate" if kind == "contact" else "contact"
            with pytest.raises(RuntimeError, match="scratch_targets_changed"):
                await scratch.prepared_scratch_enrichment(directory, other)
            assert events == ["verified"]
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.parametrize(
    "kind,bad",
    [
        ("contact", {"location_contact_rows_updated": True}),
        ("coordinate", True),
        ("contact", {"other": 1}),
        ("coordinate", -1),
    ],
)
def test_prepared_location_metric_shapes_are_exact(kind, bad):
    metric = "location_contacts_backfilled" if kind == "contact" else "location_coordinates_backfilled"
    with pytest.raises(RuntimeError, match="scratch_metrics_changed"):
        scratch._prepared_enrichment_metric(dict(enrichment_metrics={metric: bad}), kind)


@pytest.mark.asyncio
async def test_loading_location_counts_native_steps_before_copying_complete_rows(monkeypatch):
    events = []
    dataset, binding = _source()
    model = output_models()["provider_directory_location"]
    native_row = dict.fromkeys(str(column.name) for column in model.__table__.columns)
    native_row.update(
        source_id=dataset.source_id,
        resource_id="location",
        phone_number="123",
        latitude="1.25",
        fhir_meta={"versionId": "one"},
    )

    async def execute(statement, params):
        assert params["source_ids"] == [dataset.source_id]
        assert params["update_timestamp"] == datetime(2026, 1, 2, 3, 4, 5)
        events.append("coordinate" if "repair_latitude" in str(statement) else "contact")
        return SimpleNamespace(scalar_one=lambda: 1)

    async def pages(_reader, statement, _params, _model):
        assert "repair_phone_number" in statement and "repair_latitude" in statement
        assert events == ["contact", "coordinate"]
        yield [native_row]

    async def copy(_directory, _capability, _stage, rows):
        assert rows == [native_row] and rows[0]["fhir_meta"] == {"versionId": "one"}
        events.append("copy")
        return len(rows)

    monkeypatch.setattr(scratch, "projected_pages", pages)
    monkeypatch.setattr(scratch.stages, "copy_output_rows", copy)
    monkeypatch.setattr(scratch.journal, "record_output_progress", AsyncMock())
    stage = SimpleNamespace(target=model.__tablename__, schema=directory._schema())
    capability = SimpleNamespace(session=SimpleNamespace(execute=execute))
    enrichment = _enrichment(["location_contacts", "location_coordinates"])
    copied, _digest = await scratch._fill_typed_input(
        directory,
        capability,
        SimpleNamespace(measure=AsyncMock()),
        stage,
        [dataset],
        {dataset.source_id: binding},
        enrichment,
    )
    assert copied == 1 and events == ["contact", "coordinate", "copy"]
    assert enrichment["metrics"]["location_contacts_backfilled"] == {"location_contact_rows_updated": 1}
    assert enrichment["metrics"]["location_coordinates_backfilled"] == 1
    assert enrichment["metrics"]["location_address_keys_stamped"] == 0


@pytest.mark.asyncio
async def test_native_membership_wrapper_refuses_inherited_task_before_native_reads(monkeypatch):
    scope = _scope()
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    native_verify = AsyncMock()
    monkeypatch.setattr(scratch, "_verify_native_membership", native_verify)
    capability = object()
    try:
        await scratch._verify_membership(directory, scope, capability)

        async def child():
            with pytest.raises(RuntimeError, match="scratch_scope_required"):
                await scratch._verify_membership(directory, scope, capability)

        with scratch.scratch_input_scope(scope):
            await asyncio.create_task(child())
        native_verify.assert_awaited_once_with(directory, scope, capability, "{}")
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
async def test_native_membership_rechecks_lease_before_catalog_reads(monkeypatch):
    def refuse_lease():
        raise RuntimeError("expired lease")

    scope = replace(_scope(), admission=SimpleNamespace(_assert_lease=refuse_lease))
    catalog = AsyncMock()
    monkeypatch.setattr(scratch, "_source_catalog", catalog)
    with pytest.raises(RuntimeError, match="expired lease"):
        await scratch._verify_native_membership(directory, scope, object(), "{}")
    catalog.assert_not_awaited()


def _layout_scope():
    root = scratch.TYPED_ROOTS[0]
    expected = replace(relation("pdout_" + "a" * 32, 101), target=root)
    stage = scratch.stages.OutputStage(expected.schema, root, expected.relation, expected.oid, "b" * 64, expected)
    prepared = scratch.custody.PreparedOutputReceipt(1, "b" * 64, expected, expected)
    scope = replace(_scope(), stages=(stage,), prepared=(prepared,))
    catalog_row_by_field = dict(
        schema_oid=expected.schema_oid,
        oid=expected.oid,
        relname=expected.relation,
        relowner=expected.owner_oid,
        columns=[scratch.decoded(item) for item in expected.columns],
        constraints=[scratch.decoded(item) for item in expected.constraints],
        indexes=[scratch.decoded(item) for item in expected.indexes],
        topology=scratch.decoded(expected.table),
        acl=scratch.decoded(expected.acl),
        descriptor=None,
        acl_closed=True,
        readers_closed=True,
    )
    return scope, expected, catalog_row_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "oid", "schema", "persistence", "owner", "acl", "index", "child"])
async def test_scratch_native_layout_requires_original_scope_and_exact_catalog(monkeypatch, change):
    """Pure boundary checks delegate model validation and compare the complete issued receipt."""
    scope, expected, catalog_row_by_field = _layout_scope()
    from process.provider_directory_cms_preparation import OwnedRelation

    owned = OwnedRelation(expected.schema, expected.relation, expected.oid, 100, "p")
    if change in {"oid", "schema", "persistence"}:
        owned = replace(owned, **{change: {"oid": 999, "schema": "foreign", "persistence": "u"}[change]})
    if change == "owner":
        catalog_row_by_field["relowner"] += 1
    if change == "acl":
        catalog_row_by_field["acl_closed"] = False
    if change == "index":
        catalog_row_by_field["indexes"][0]["oid"] += 1
    fetch = AsyncMock(return_value=SimpleNamespace(_mapping=catalog_row_by_field))
    shape = Mock()
    monkeypatch.setattr(scratch, "require_output_shape", shape)
    executor = SimpleNamespace(first=fetch)
    assert await scratch.scratch_relation_proof(directory, owned, database=executor) is None
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        with scratch.scratch_input_scope(scope):
            if change:
                invocation = scratch.scratch_relation_proof(directory, owned, database=executor)
                if change == "child":
                    invocation = asyncio.create_task(invocation)
                with pytest.raises(RuntimeError, match="scratch_(scope_required|stage_changed)"):
                    await invocation
            else:
                assert await scratch.scratch_relation_proof(directory, owned, database=executor) == expected
                assert fetch.await_args.kwargs == dict(
                    schema=expected.schema, names=[expected.relation], owner=expected.owner_oid, readers=[]
                )
                shape.assert_called_once_with(catalog_row_by_field, expected.target)
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "unregistered", "foreign_task", "foreign_admission", "revoked"])
@pytest.mark.parametrize("batch", [False, True])
async def test_layout_recapture_accepts_only_same_issued_source_worker(source_case, monkeypatch, failure, batch):
    from process import entity_address_preparation_admission as address_admission
    from process.provider_directory_cms_preparation import OwnedRelation

    case = source_case
    original_worker = address_admission._admitted_preparation(
        case.admission, case.admission.plan.native_address_input_hash
    )
    scope, expected, catalog_by_field = _layout_scope()
    scope = replace(scope, admission=case.admission)
    owned = OwnedRelation(expected.schema, expected.relation, expected.oid, 100, "p")
    fetch = AsyncMock(return_value=SimpleNamespace(_mapping=catalog_by_field))

    async def all_rows(*args, **kwargs):
        return [await fetch(*args, **kwargs)]

    executor = SimpleNamespace(first=fetch, all=all_rows)
    monkeypatch.setattr(scratch, "require_output_shape", Mock())
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))

    async def recapture():
        worker = original_worker
        if failure in {"foreign_task", "foreign_admission"}:
            admission = case.admission if failure == "foreign_task" else _admission()
            admission._started = True
            token = preparation._ACTIVE.set(admission)
            worker = address_admission._admitted_preparation(admission, admission.plan.native_address_input_hash)
        else:
            token = preparation._ACTIVE.set(case.admission)
        worker_token = address_admission._ADMISSION.set(worker)
        try:
            if failure == "unregistered":
                with pytest.raises(RuntimeError, match="scratch_scope_required"):
                    await _worker_relation_proof(owned, executor, batch)
            else:
                async with address_admission._worker_slot():
                    await _recapture_worker_layout(owned, executor, expected, worker, fetch, failure, batch)
        finally:
            address_admission._ADMISSION.reset(worker_token)
            preparation._ACTIVE.reset(token)

    try:
        with scratch.scratch_input_scope(scope):
            await asyncio.create_task(recapture())
        assert fetch.await_count == (1 + int(batch and failure is None) if failure in {None, "revoked"} else 0)
    finally:
        del scratch._ISSUED[scope]


async def _worker_relation_proof(owned, executor, batch):
    if batch:
        async with scratch.scratch_relation_batch(directory, (owned,), database_for=lambda _item: executor) as proofs:
            return proofs[(owned.schema, owned.relation)][0]
    return await scratch.scratch_relation_proof(directory, owned, database=executor)


async def _recapture_worker_layout(owned, executor, expected, worker, fetch, failure, batch):
    with pytest.raises(RuntimeError, match="scratch_scope_required"):
        scratch.active_scratch_inputs(directory)
    if failure == "revoked":

        async def revoke(*args, **kwargs):
            worker.worker_tasks.remove(asyncio.current_task())
            return fetch.return_value

        fetch.side_effect = revoke
    try:
        if failure is not None:
            with pytest.raises(RuntimeError, match="scratch_scope_required"):
                await _worker_relation_proof(owned, executor, batch)
        else:
            assert await _worker_relation_proof(owned, executor, batch) == expected
    finally:
        if failure == "revoked":
            worker.worker_tasks.add(asyncio.current_task())


def _issued_receipt_scope():
    root = scratch.TYPED_ROOTS[0]
    incumbent = replace(relation(root, 101, descriptor={"phase": "sealed"}), target=root)
    successor = replace(incumbent, relation="pdout_" + "a" * 32, oid=201)
    stage = scratch.stages.OutputStage(
        incumbent.schema, root, successor.relation, successor.oid, "b" * 64, replace(incumbent)
    )
    receipt = scratch.custody.PreparedOutputReceipt(1, "b" * 64, replace(incumbent), replace(successor))
    return replace(_scope(), stages=(stage,), prepared=(receipt,))


def _changed_receipt_field(value):
    if type(value) is str:
        return value + " "
    if type(value) is int:
        return value + 1
    if type(value) is tuple:
        if value and type(value[0]) is str:
            return (" " + value[0], *value[1:])
        return (replace(value[0], oid=value[0].oid + 1),)
    if type(value) is scratch.custody.NativeOutputRelation:
        return replace(value, oid=value.oid + 1)
    return object()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_kind,field_name",
    [("scope", field.name) for field in fields(scratch.ScratchInputs)]
    + [("stage", field.name) for field in fields(scratch.stages.OutputStage)]
    + [("prepared", field.name) for field in fields(scratch.custody.PreparedOutputReceipt)]
    + [
        (target, field.name)
        for target in ("stage_incumbent", "prepared_incumbent", "prepared_successor")
        for field in fields(scratch.custody.NativeOutputRelation)
    ],
)
@pytest.mark.parametrize("boundary", ["direct", "batch"])
async def test_issued_scope_refuses_every_raw_receipt_field_mutation(receipt_kind, field_name, boundary):
    scope = _issued_receipt_scope()
    target_by_name = {
        "scope": scope,
        "stage": scope.stages[0],
        "prepared": scope.prepared[0],
        "stage_incumbent": scope.stages[0].incumbent,
        "prepared_incumbent": scope.prepared[0].incumbent,
        "prepared_successor": scope.prepared[0].successor,
    }
    original_directory = scope.directory
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        assert scratch._require_issued(scope, original_directory) == "{}"
        selected = target_by_name[receipt_kind]
        original_field_value = getattr(selected, field_name)
        if receipt_kind == "scope" and field_name == "prepared":
            changed_field_value = (replace(original_field_value[0], protocol=original_field_value[0].protocol + 1),)
        else:
            changed_field_value = _changed_receipt_field(original_field_value)
        if boundary == "direct":
            object.__setattr__(selected, field_name, changed_field_value)
            with pytest.raises(RuntimeError, match="scratch_scope_required"):
                scratch._require_issued(scope, original_directory)
        else:
            with scratch.scratch_input_scope(scope), pytest.raises(RuntimeError, match="scratch_scope_required"):
                async with scratch.scratch_relation_batch(original_directory, ()) as proofs:
                    object.__setattr__(selected, field_name, changed_field_value)
            assert proofs == {}
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["array", "boolean", "float", "json_text"])
async def test_issued_state_preserves_json_array_identity_and_exact_atoms(change):
    scope = _issued_receipt_scope()
    scratch._ISSUED[scope] = (scope.directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        assert scratch._require_issued(scope, directory) == "{}"
        if change == "array":
            successor = scope.prepared[0].successor
            object.__setattr__(successor, "columns", list(successor.columns))
            assert scratch._require_issued(scope, directory) == "{}"
        else:
            if change == "json_text":
                object.__setattr__(scope, "operation_json", scope.operation_json + " ")
            else:
                object.__setattr__(scope.prepared[0], "protocol", True if change == "boolean" else 1.0)
            with pytest.raises(RuntimeError, match="scratch_scope_required"):
                scratch._require_issued(scope, directory)
    finally:
        del scratch._ISSUED[scope]


def _batch_members(split=None):
    """Keep the complete real receipt shape while varying one grouping coordinate."""
    from process.provider_directory_cms_preparation import OwnedRelation

    scope, first, catalog_row_by_field = _layout_scope()
    second = replace(
        first,
        relation="pdout_" + "b" * 32,
        oid=first.oid + 1,
        schema="other" if split == "schema" else first.schema,
        owner_oid=first.owner_oid + int(split == "owner"),
    )
    members = (first, second)
    scope = replace(
        scope,
        stages=tuple(
            replace(
                scope.stages[0], schema=receipt.schema, relation=receipt.relation, oid=receipt.oid, incumbent=receipt
            )
            for receipt in members
        ),
        prepared=tuple(replace(scope.prepared[0], incumbent=receipt, successor=receipt) for receipt in members),
    )
    row_by_name = {}
    for receipt in members:
        row_by_name[receipt.relation] = {
            **deepcopy(catalog_row_by_field),
            "oid": receipt.oid,
            "relname": receipt.relation,
            "relowner": receipt.owner_oid,
        }
    relations = tuple(OwnedRelation(receipt.schema, receipt.relation, receipt.oid, 100, "p") for receipt in members)
    return scope, members, row_by_name, relations


@pytest.mark.asyncio
@pytest.mark.parametrize("split", [None, "actor", "schema", "owner"])
async def test_scratch_batch_groups_exact_actor_schema_owner_and_clears_proofs(monkeypatch, split):
    scope, members, row_by_name, relations = _batch_members(split)
    fetch = AsyncMock(side_effect=lambda _sql, **params: [row_by_name[name] for name in params["names"]])
    executors = (SimpleNamespace(all=fetch), SimpleNamespace(all=fetch))
    monkeypatch.setattr(scratch, "require_output_shape", Mock())
    scratch._ISSUED[scope] = (directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        with scratch.scratch_input_scope(scope):
            async with scratch.scratch_relation_batch(
                directory,
                relations,
                database_for=lambda item: executors[int(split == "actor" and item is relations[1])],
            ) as proofs:
                assert {key: value[0] for key, value in proofs.items()} == {
                    (item.schema, item.relation): item for item in members
                }
                assert fetch.await_count == (1 if split is None else 2)
            assert proofs == {}
        assert fetch.await_count == (2 if split is None else 4)
        assert all(len(call.kwargs["names"]) == (2 if split is None else 1) for call in fetch.await_args_list)
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "duplicate", "foreign", "oid", "owner", "acl", "index"])
async def test_scratch_batch_refuses_changed_member_at_exit(monkeypatch, change):
    scope, members, row_by_name, relations = _batch_members()
    has_changed_members = False

    async def fetch(_sql, **params):
        captured_rows = [deepcopy(row_by_name[name]) for name in params["names"]]
        if has_changed_members:
            if change == "missing":
                return captured_rows[:-1]
            if change == "duplicate":
                return [*captured_rows, captured_rows[0]]
            if change == "foreign":
                captured_rows[0]["relname"] = "unissued"
            elif change in {"oid", "owner"}:
                captured_rows[0]["oid" if change == "oid" else "relowner"] += 1
            elif change == "acl":
                captured_rows[0]["acl_closed"] = False
            else:
                captured_rows[0]["indexes"][0]["oid"] += 1
        return captured_rows

    database = SimpleNamespace(all=AsyncMock(side_effect=fetch))
    monkeypatch.setattr(scratch, "require_output_shape", Mock())
    scratch._ISSUED[scope] = (directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        with scratch.scratch_input_scope(scope), pytest.raises(RuntimeError, match="scratch_stage_changed"):
            async with scratch.scratch_relation_batch(
                directory, relations, database_for=lambda _item: database
            ) as proofs:
                has_changed_members = True
        assert proofs == {}
    finally:
        del scratch._ISSUED[scope]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_scratch_batch_discards_proofs_on_failure_or_cancellation(monkeypatch, failure):
    scope, _members, row_by_name, relations = _batch_members()
    fetch = AsyncMock(side_effect=lambda _sql, **params: [row_by_name[name] for name in params["names"]])
    database = SimpleNamespace(all=fetch)
    monkeypatch.setattr(scratch, "require_output_shape", Mock())
    scratch._ISSUED[scope] = (directory, scope.admission, scope.task, "{}", scratch._scope_state(scope))
    try:
        with scratch.scratch_input_scope(scope), pytest.raises(failure):
            async with scratch.scratch_relation_batch(
                directory, relations, database_for=lambda _item: database
            ) as proofs:
                assert len(proofs) == 2
                raise failure("synthetic interruption")
        assert proofs == {}
        fetch.assert_awaited_once()
    finally:
        del scratch._ISSUED[scope]
