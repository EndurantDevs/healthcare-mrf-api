# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary artifact preparation reads one declared published resource snapshot."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_artifacts as artifacts
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_named_fhir_source_snapshots import named_handoff as named_handoff

directory = importlib.import_module("process.provider_directory_fhir")
RESOURCE = directory.ProviderDirectoryDatasetResource.__tablename__


@pytest.fixture
async def ordinary_input(monkeypatch, named_handoff):
    factory_token = artifacts.source._FACTORY.set(None)
    publisher_token = artifacts.source._PUBLISHER.set(None)
    monkeypatch.setattr(directory, "_schema", lambda: "catalog_schema")
    first = AsyncMock(return_value={"candidate_storage_json": json.dumps(named_handoff)})
    monkeypatch.setattr(directory.db, "first", first)
    dataset = directory.ProviderDirectoryArtifactDataset(
        "directory_source",
        "endpoint_one",
        named_handoff["identity"]["dataset_id"],
        "run_one",
        selected_resources=("Practitioner",),
        dataset_hash=named_handoff["dataset_hash"],
    )
    fence = directory.ProviderDirectoryArtifactDatasetFence((dataset,))
    metrics_by_name = {
        artifacts._FOLLOWUP_PREFIX + key: marker
        for key, marker in {
            "version": 1,
            "stage": "address_overlay",
            "source_id": dataset.source_id,
            "endpoint_id": dataset.endpoint_id,
            "dataset_id": dataset.dataset_id,
            "dataset_hash": dataset.dataset_hash,
            "parent_run_id": dataset.evidence_run_id,
        }.items()
    }
    request = directory.ProviderDirectoryArtifactPublishRequest(
        None, metrics_by_name, [dataset.source_id], False, {"address_overlay"}
    )
    try:
        admission = artifacts._publication_admission(directory, fence, request, {"Practitioner"})
        assert admission.factory is None and not fence.promotion_datasets
        relation = named_handoff["tables"][RESOURCE]
        yield SimpleNamespace(
            admission=admission,
            dataset=dataset,
            first=first,
            document=named_handoff,
            relation=f'"{relation["schema"]}"."{relation["name"]}"',
        )
    finally:
        artifacts.source._PUBLISHER.reset(publisher_token)
        artifacts.source._FACTORY.reset(factory_token)


@pytest.mark.asyncio
async def test_named_ordinary_capacity_and_selection_share_input(ordinary_input, monkeypatch):
    case = ordinary_input
    count_rows = [
        SimpleNamespace(
            _mapping={"dataset_id": case.dataset.dataset_id, "resource_type": "Practitioner", "resource_rows": 1}
        )
    ]
    count = AsyncMock(return_value=count_rows)
    monkeypatch.setattr(directory.db, "all", count)
    async with artifacts._resource_input_scope(directory, case.admission):
        projection = await directory._provider_directory_artifact_scope_projection(
            case.admission.fence, frozenset({"Practitioner"})
        )
        assert projection["projected_rows"] > 0
        full = directory._provider_directory_artifact_resource_select_sql(
            directory.ProviderDirectoryPractitioner, directory._schema()
        )
        page = directory._artifact_resource_batch_select_sql(
            directory.ProviderDirectoryPractitioner, directory._schema()
        )
        for statement in (count.await_args.args[0], full, page):
            assert (
                case.relation in statement and '"catalog_schema"."provider_directory_dataset_resource"' not in statement
            )
        assert '"catalog_schema"."provider_directory_endpoint_dataset"' in full
        for root in ("provider_directory_source", "provider_directory_api_endpoint", "import_run"):
            assert directory._qt(directory._schema(), root) == f'"catalog_schema"."{root}"'
    case.first.assert_awaited_once_with(
        'SELECT candidate_storage_json FROM "catalog_schema"."provider_directory_endpoint_dataset" WHERE dataset_id=:dataset_id',
        dataset_id=case.dataset.dataset_id,
    )
    assert directory._qt(directory._schema(), RESOURCE) == f'"catalog_schema"."{RESOURCE}"'


@pytest.mark.asyncio
async def test_named_ordinary_copy_and_parity_share_input(ordinary_input, monkeypatch):
    case, model = ordinary_input, directory.ProviderDirectoryPractitioner
    key = SimpleNamespace(_mapping={"resource_id": "1000000001"})
    reads = AsyncMock(side_effect=[[key], []])
    monkeypatch.setattr(directory.db, "all", reads)
    resource_by_field = {str(column.name): None for column in model.__table__.columns}
    resource_by_field.update(source_id=case.dataset.source_id, resource_id="1000000001", npi=1000000001)
    copied_records = []

    async def pages(_reader, statement, params_by_name, actual_model):
        assert actual_model is model and params_by_name["dataset_id"] == case.dataset.dataset_id
        assert case.relation in statement
        yield [resource_by_field]

    async def copy(_name, **options):
        copied_records.extend(options["records"])
        return "COPY 1"

    @asynccontextmanager
    async def nested():
        yield

    session = SimpleNamespace(
        copy_records_to_table=copy,
        execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: True)),
        begin_nested=nested,
    )
    monkeypatch.setattr(artifacts, "projected_pages", pages)
    case.admission.prepared["plan"] = {"publish_targets": ["address_overlay"], "decision_at": "2026-01-01T00:00:00"}
    async with artifacts._resource_input_scope(directory, case.admission):
        statement = directory._provider_directory_artifact_resource_select_sql(model, directory._schema())
        params_by_name = dict(
            source_ids=[case.dataset.source_id],
            dataset_ids=[case.dataset.dataset_id],
            evidence_run_ids=[case.dataset.evidence_run_id],
            resource_type="Practitioner",
        )
        _loading, expected, params_by_name, _kind, _counts = artifacts._loading_projection(
            directory, model.__tablename__, statement, params_by_name, case.admission.prepared
        )
        assert (
            await artifacts._copy(
                directory, session, model, "prepared_practitioners", statement, params_by_name, admission=case.admission
            )
            == 1
        )
        await artifacts._verify_set_parity(
            session, expected, '"catalog_schema"."prepared_practitioners"', params_by_name
        )
    assert len(copied_records) == 1 and len(copied_records[0]) == len(model.__table__.columns)
    assert all(case.relation in call.args[0] for call in reads.await_args_list)
    parity_sql = str(session.execute.await_args_list[2].args[0])
    assert case.relation in parity_sql and "HAVING sum(delta) <> 0" in parity_sql
    assert reads.await_args_list[-1].kwargs["after_resource_id"] == "1000000001"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError("prepare failed"), asyncio.CancelledError()])
async def test_named_input_scope_restores_existing_overrides(ordinary_input, failure):
    case = ordinary_input
    existing_by_root = {RESOURCE: "previous_resources", "provider_directory_location": "prepared_locations"}
    with directory._provider_directory_artifact_relation_scope(existing_by_root):
        try:
            async with artifacts._resource_input_scope(directory, case.admission):
                assert directory._qt(directory._schema(), RESOURCE) == case.relation
                assert (
                    directory._qt(directory._schema(), "provider_directory_location")
                    == '"catalog_schema"."prepared_locations"'
                )
                if failure is not None:
                    raise failure
        except (ValueError, asyncio.CancelledError) as raised:
            assert raised is failure
        assert directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == existing_by_root


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "dataset", "schema"])
async def test_named_input_refuses_invalid_dataset_metadata(ordinary_input, change):
    case = ordinary_input
    if change == "missing":
        case.first.return_value = None
    else:
        if change == "dataset":
            case.document["identity"]["dataset_id"] = "other_dataset"
        else:
            case.document["tables"][RESOURCE]["schema"] = "other_schema"
        case.first.return_value = {"candidate_storage_json": json.dumps(case.document)}
    with pytest.raises(
        GenericCandidateError,
        match={"missing": "dataset_missing", "dataset": "dataset_changed", "schema": "schema_changed"}[change],
    ):
        async with artifacts._resource_input_scope(directory, case.admission):
            pytest.fail("Invalid metadata reached preparation")
    case.first.assert_awaited_once()
    assert directory._qt(directory._schema(), RESOURCE) == f'"catalog_schema"."{RESOURCE}"'


@pytest.mark.asyncio
async def test_source_factory_keeps_existing_input_route(ordinary_input):
    case = ordinary_input
    async with artifacts._resource_input_scope(directory, replace(case.admission, factory=object())):
        assert directory._qt(directory._schema(), RESOURCE) == f'"catalog_schema"."{RESOURCE}"'
    case.first.assert_not_awaited()


@pytest.mark.parametrize(
    "root,relation",
    [
        (RESOURCE, ("snapshot",)),
        (RESOURCE, ("snapshot", "raw", "extra")),
        (RESOURCE, ("bad.schema", "raw")),
        (RESOURCE, ("snapshot", "bad.table")),
        (RESOURCE, ["snapshot", "raw"]),
        ("provider_directory_endpoint_dataset", ("snapshot", "header")),
        ("provider_directory_source", ("snapshot", "source")),
    ],
)
@pytest.mark.asyncio
async def test_qualified_input_route_requires_raw_identifiers(ordinary_input, root, relation):
    with directory._provider_directory_artifact_relation_scope({root: relation}):
        with pytest.raises((ValueError, directory.ProviderDirectoryArtifactBuildStale)):
            directory._qt(directory._schema(), root)


@pytest.mark.asyncio
async def test_qualified_input_cannot_replace_source_factory_routing(ordinary_input):
    token = artifacts.source._FACTORY.set(object())
    try:
        with directory._provider_directory_artifact_relation_scope({RESOURCE: ("snapshot", "raw")}):
            with pytest.raises(directory.ProviderDirectoryArtifactBuildStale, match="input_relation_invalid"):
                directory._qt(directory._schema(), RESOURCE)
    finally:
        artifacts.source._FACTORY.reset(token)
